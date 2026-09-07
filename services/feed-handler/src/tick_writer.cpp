#include "tick_writer.hpp"

#include <nlohmann/json.hpp>

#include <cstdio>
#include <ctime>
#include <fstream>
#include <stdexcept>
#include <utility>

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

std::tm utc_tm(std::chrono::system_clock::time_point tp)
{
    const std::time_t t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);      // MSVCRT argument order is (tm*, time_t*)
#else
    gmtime_r(&t, &tm);      // POSIX order is the other way round
#endif
    return tm;
}

// to_time_t truncates to whole seconds, so the sub-second part has to be
// recovered from the original time_point.
int micros_of_second(std::chrono::system_clock::time_point tp)
{
    using namespace std::chrono;
    return static_cast<int>(duration_cast<microseconds>(tp - floor<seconds>(tp)).count());
}

// A previous run killed between the record and its newline leaves the file
// ending mid-line. Appending straight onto that glues two records into one
// corrupt entry, so the writer heals it on open.
bool ends_with_newline(const fs::path& p)
{
    std::ifstream in(p, std::ios::in | std::ios::binary);
    if (!in)
        return true;            // unreadable: do not add a stray newline
    in.seekg(-1, std::ios::end);
    char c = '\0';
    in.get(c);
    return c == '\n';
}

// Hive-style partition, from OUR clock rather than the exchange's. Using
// recv_time means partitions only ever move forward: a tick whose exchange
// timestamp straddles the hour boundary cannot send us back to reopen a file
// we already closed.
std::string partition_key(std::chrono::system_clock::time_point tp)
{
    const std::tm tm = utc_tm(tp);
    char buf[40];
    std::strftime(buf, sizeof buf, "dt=%Y-%m-%d/hour=%H", &tm);
    return buf;
}

} // namespace


std::string iso8601_utc(std::chrono::system_clock::time_point tp)
{
    const std::tm tm = utc_tm(tp);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%S", &tm);

    char out[48];
    std::snprintf(out, sizeof out, "%s.%06dZ", stamp, micros_of_second(tp));
    return out;
}


TickWriter::TickWriter(fs::path root, unsigned flush_every, std::chrono::seconds flush_interval)
    : root_(std::move(root)),
      flush_every_(flush_every ? flush_every : 1),
      flush_interval_(flush_interval),
      last_flush_(std::chrono::steady_clock::now())
{
}

TickWriter::~TickWriter()
{
    close();
}

void TickWriter::open_for(const std::string& symbol, const std::string& partition)
{
    close();

    const fs::path dir = root_ / ("symbol=" + symbol) / partition;
    fs::create_directories(dir);
    current_file_ = dir / "ticks.jsonl";

    // Append, never truncate. The contract is append-only, and a restart inside
    // the same hour must not wipe what the previous run already wrote.
    const bool heal = fs::exists(current_file_)
                   && fs::file_size(current_file_) > 0
                   && !ends_with_newline(current_file_);

    out_.open(current_file_, std::ios::out | std::ios::app);
    if (!out_)
        throw std::runtime_error("cannot open " + current_file_.string());

    if (heal)
        out_ << '\n';

    open_symbol_ = symbol;
    open_partition_ = partition;
    since_flush_ = 0;
    last_flush_ = std::chrono::steady_clock::now();
    ++files_;
}

void TickWriter::write(const Tick& t)
{
    const std::string partition = partition_key(t.recv_time);

    if (!out_.is_open() || partition != open_partition_ || t.product_id != open_symbol_)
        open_for(t.product_id, partition);

    // nlohmann rather than hand-rolled string building: it escapes the string
    // fields and round-trips the doubles losslessly. It is also the obvious
    // thing to replace once a benchmark says serialisation is actually hot.
    const json j = {
        {"sequence",      t.sequence},
        {"product_id",    t.product_id},
        {"price",         t.price},
        {"last_size",     t.last_size},
        {"side",          t.side},
        {"best_bid",      t.best_bid},
        {"best_bid_size", t.best_bid_size},
        {"best_ask",      t.best_ask},
        {"best_ask_size", t.best_ask_size},
        {"time",          t.time},
        {"trade_id",      t.trade_id},
        {"recv_time",     iso8601_utc(t.recv_time)},
    };

    // One insert rather than `<< dump() << '\n'`. Two inserts leave a window
    // for a kill to land between the record and its newline, which is exactly
    // the damage ends_with_newline() has to repair on the next open.
    out_ << (j.dump() + "\n");
    ++records_;
    ++since_flush_;

    const auto now = std::chrono::steady_clock::now();
    if (since_flush_ >= flush_every_ || now - last_flush_ >= flush_interval_)
        flush();
}

void TickWriter::flush()
{
    if (!out_.is_open())
        return;

    out_.flush();
    since_flush_ = 0;
    last_flush_ = std::chrono::steady_clock::now();
}

void TickWriter::close()
{
    if (!out_.is_open())
        return;

    out_.flush();
    out_.close();
}
