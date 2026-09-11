// Coinbase Exchange feed handler.
//
//   source thread --raw--> queue --> parser pool --> queue --> writer thread
//                     (ring or mutex)   (1..N)      (mutex)        |
//                                                            ticks.jsonl
//
// The source is either the live websocket or a recorded frame file, so the
// whole pipeline can be benchmarked without waiting on live market conditions.
//
//   feed_handler [options]
//     --out DIR          raw zone root            (default data/raw/ticks)
//     --queue mutex|spsc which queue to benchmark (default mutex)
//     --parsers N        parser threads           (default 1; spsc forces 1)
//     --capacity N       queue slots              (default 4096)
//     --overflow block|drop                       (default block)
//     --record FILE      also append raw frames, for later replay
//     --replay FILE      read frames from FILE instead of the network
//     --speed N          replay pacing: 0 = as fast as possible (default 0)
//     --bench            replay, discard output, print throughput and latency
//
//   Beast sync-ssl example: https://github.com/boostorg/beast/tree/develop/example/websocket/client/sync-ssl
//   Coinbase websocket:    https://docs.cdp.coinbase.com/exchange/websocket-feed/overview

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>

#include <iostream>
#include <string>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <type_traits>
#include <vector>

//Json
#include <nlohmann/json.hpp>

#include "bounded_queue.hpp"
#include "spsc_ring.hpp"
#include "tick.hpp"
#include "tick_writer.hpp"

namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
namespace fs        = std::filesystem;
using json = nlohmann::json;

using tcp = boost::asio::ip::tcp;

using steady = std::chrono::steady_clock;


// Set by the signal handler, read by every thread. A handler may only touch
// lock-free atomics -- no I/O, no allocation, no flushing the writer from in
// here. Everything that has to happen on shutdown happens back in main().
static std::atomic<bool> g_stop{false};

static void on_signal(int) { g_stop.store(true, std::memory_order_release); }


// ---------------------------------------------------------------------------
// what moves through the pipeline
// ---------------------------------------------------------------------------

// A frame as it came off the wire, plus the ticket that will put it back in
// order after the parser pool has scrambled it.
struct Raw
{
    uint64_t ticket = 0;
    std::string payload;
    std::chrono::system_clock::time_point recv{};
    steady::time_point enqueued{};
};

// Every ticket produces exactly one of these, even when the frame was a
// heartbeat or garbage. An empty `tick` means "nothing to write, but do not
// wait for me" -- without that the writer would block forever on a ticket that
// never turns into a record.
struct Parsed
{
    uint64_t ticket = 0;
    std::optional<Tick> tick;
    steady::time_point enqueued{};
};


struct Stats
{
    std::atomic<uint64_t> frames{0};       // frames pulled from the source
    std::atomic<uint64_t> ticks{0};        // frames that parsed into a Tick
    std::atomic<uint64_t> written{0};      // ticks handed to the writer
    std::atomic<uint64_t> unparseable{0};  // ticker frames we could not read
    std::atomic<uint64_t> dropped{0};      // shed by the overflow policy
    std::atomic<uint64_t> out_of_order{0}; // duplicate or replayed sequence
    std::atomic<uint64_t> sessions{0};     // connections opened
};


enum class QueueKind { Mutex, Spsc };

struct Config
{
    fs::path out = "data/raw/ticks";
    QueueKind queue = QueueKind::Mutex;
    unsigned parsers = 1;
    std::size_t capacity = 4096;
    Overflow overflow = Overflow::Block;
    fs::path record;
    fs::path replay;
    double speed = 0.0;      // 0 = unpaced
    bool bench = false;
};


// ---------------------------------------------------------------------------
// parsing  (unchanged semantics, now running off the network thread)
// ---------------------------------------------------------------------------

// Coinbase quotes every price and size as a JSON string, so .get<double>()
// throws on them.
//
// .at() rather than [] everywhere below: operator[] on a non-const json
// *inserts* a null member when the key is missing, silently mutating the
// frame and then handing back a null to convert. .at() throws
// json::out_of_range, which is the signal we actually want.
static double to_double(const json& j, const char* key)
{
    return std::stod(j.at(key).get<std::string>());
}

// nullopt = a frame we cannot trust. The caller counts these; one malformed
// frame must not kill a 72-hour unattended run.
static std::optional<Tick> parse_ticker(
    const json& j, std::chrono::system_clock::time_point recv)
{
    try
    {
        Tick t;
        t.sequence      = j.at("sequence").get<int64_t>();
        t.price         = to_double(j, "price");
        t.product_id    = j.at("product_id").get<std::string>();
        t.last_size     = to_double(j, "last_size");
        t.side          = j.at("side").get<std::string>();
        t.best_bid      = to_double(j, "best_bid");
        t.best_bid_size = to_double(j, "best_bid_size");
        t.best_ask      = to_double(j, "best_ask");
        t.best_ask_size = to_double(j, "best_ask_size");
        t.time          = j.at("time").get<std::string>();
        t.trade_id      = j.at("trade_id").get<int64_t>();
        t.recv_time     = recv;
        return t;
    }
    catch (const std::exception&)   // json::out_of_range, json::type_error,
    {                               // std::invalid_argument, std::out_of_range
        return std::nullopt;
    }
}


// ---------------------------------------------------------------------------
// stage 2 -- parser pool
// ---------------------------------------------------------------------------

// Templated on the queue so the same loop drives the mutex queue and the ring;
// both expose pop_wait(T&, const atomic<bool>&) for exactly this reason.
template <class RawQ>
static void parser_loop(RawQ& in, BoundedQueue<Parsed>& out, Stats& stats,
                        const std::atomic<bool>& source_done)
{
    Raw raw;
    while (in.pop_wait(raw, source_done))
    {
        Parsed p;
        p.ticket = raw.ticket;
        p.enqueued = raw.enqueued;

        // allow_exceptions=false -- a malformed frame must not throw out of the
        // loop, it gets counted and skipped.
        json j = json::parse(raw.payload, nullptr, /*allow_exceptions=*/false);

        if (!j.is_discarded() && j.value("type", "") == "ticker")
        {
            // skip the subscriptions ack, errors, and anything we don't handle
            // from https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
            if (auto t = parse_ticker(j, raw.recv))
            {
                stats.ticks.fetch_add(1, std::memory_order_relaxed);
                p.tick = std::move(t);
            }
            else
            {
                stats.unparseable.fetch_add(1, std::memory_order_relaxed);
            }
        }

        out.push(std::move(p));
    }
}


// ---------------------------------------------------------------------------
// stage 3 -- writer, and the reordering that makes a parser pool safe
// ---------------------------------------------------------------------------

// N parsers finish in whatever order they finish, so tickets arrive shuffled.
// The file has to stay in feed order, so the writer holds anything early in a
// map and only emits the contiguous run starting at the next expected ticket.
// The map is bounded in practice by how far the slowest parser lags.
static void writer_loop(BoundedQueue<Parsed>& in, TickWriter& writer, Stats& stats,
                        const std::atomic<bool>& parsers_done,
                        bool discard, std::vector<double>* latency_ms)
{
    std::map<uint64_t, Parsed> pending;
    uint64_t expect = 0;
    int64_t last_sequence = 0;

    auto emit = [&](Parsed& p) {
        if (!p.tick)
            return;

        // NOT a gap check. `sequence` counts every message the product's feed
        // generates, and `ticker` is a filtered view of it -- skipped numbers
        // are the normal case (a 25s sample jumped on 878 of 878 consecutive
        // pairs). What IS an error is the sequence standing still or going
        // backwards: a duplicate or replay, which would double-count a trade.
        if (last_sequence != 0 && p.tick->sequence <= last_sequence)
            stats.out_of_order.fetch_add(1, std::memory_order_relaxed);
        last_sequence = p.tick->sequence;

        if (!discard)
            writer.write(*p.tick);

        if (latency_ms)
            latency_ms->push_back(
                std::chrono::duration<double, std::milli>(steady::now() - p.enqueued).count());

        stats.written.fetch_add(1, std::memory_order_relaxed);
    };

    auto drain_contiguous = [&] {
        for (auto it = pending.find(expect); it != pending.end(); it = pending.find(expect))
        {
            emit(it->second);
            pending.erase(it);
            ++expect;
        }
    };

    Parsed p;
    while (in.pop_wait(p, parsers_done))
    {
        pending.emplace(p.ticket, std::move(p));
        drain_contiguous();
    }

    // Closed and drained. Anything still held was waiting on a ticket that will
    // never arrive (the source stopped mid-flight), so emit it in order rather
    // than discarding buffered records.
    for (auto& [ticket, held] : pending)
        emit(held);
}


// ---------------------------------------------------------------------------
// stage 1 -- sources
// ---------------------------------------------------------------------------

// Hand a frame to the parsers under the configured overflow policy.
// Returns false only when the frame was shed.
template <class RawQ>
static bool offer(RawQ& q, Raw&& raw, const Config& cfg, Stats& stats)
{
    if constexpr (std::is_same_v<RawQ, SpscRing<Raw>>)
    {
        // The ring has no blocking push, so the policy lives here instead.
        // full() is only trustworthy because this is the single producer.
        while (q.full())
        {
            if (cfg.overflow == Overflow::DropNewest)
            {
                stats.dropped.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            if (g_stop.load(std::memory_order_acquire))
                return false;
            std::this_thread::yield();
        }
        return q.push(std::move(raw));
    }
    else
    {
        if (!q.push(std::move(raw)))
        {
            stats.dropped.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        return true;
    }
}


// A blocking ws.read() with no timeout is the classic unattended-service hang:
// if the connection goes half-open (NAT drops the mapping, the laptop sleeps)
// there are no bytes and no error, so the read waits forever and the feed
// silently stops. SO_RCVTIMEO turns that into an error we can reconnect on.
//
// beast::tcp_stream::expires_after does not help here -- its timers only apply
// to *async* operations, and this is the synchronous client.
static void set_recv_timeout(tcp::socket& sock, int seconds)
{
#ifdef _WIN32
    DWORD ms = static_cast<DWORD>(seconds) * 1000;
    ::setsockopt(sock.native_handle(), SOL_SOCKET, SO_RCVTIMEO,
                 reinterpret_cast<const char*>(&ms), sizeof(ms));
#else
    timeval tv{};
    tv.tv_sec = seconds;
    ::setsockopt(sock.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
}


// One connection, start to finish. Returns normally when g_stop is set; throws
// on any network or protocol failure so the caller can reconnect.
template <class RawQ>
static void run_session(const Config& cfg, RawQ& raw_q, Stats& stats,
                        uint64_t& ticket, std::ofstream* recorder)
{
    const std::string host = "ws-feed.exchange.coinbase.com";
    const std::string port = "443";
    const std::string subscribe =
        R"({"type":"subscribe","product_ids":["BTC-USD"],"channels":["ticker"]})";

    // ioc = the I/O engine every Asio object needs.
    // ctx = TLS settings (protocol version, which certs to trust).
    net::io_context ioc;
    ssl::context    ctx{ssl::context::tlsv12_client};

    // Trust the system CA bundle (MSYS2: pacman -S mingw-w64-x86_64-ca-certificates,
    // which populates /mingw64/etc/ssl/certs). OpenSSL also honours the
    // SSL_CERT_FILE / SSL_CERT_DIR env vars if you need to override this.
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);

    tcp::resolver resolver{ioc};
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws{ioc, ctx};

    // 1. hostname -> list of IP endpoints
    auto const results = resolver.resolve(host, port);

    // 2. TCP connect. get_lowest_layer() reaches past the websocket and TLS
    //    layers down to the raw socket underneath.
    auto ep = beast::get_lowest_layer(ws).connect(results);

    set_recv_timeout(beast::get_lowest_layer(ws).socket(), 30);

    // 3. SNI -- tells the server which hostname we want a certificate for.
    //    Without this the TLS handshake fails with an unhelpful error.
    if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host.c_str()))
        throw beast::system_error{
            static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};

    // Check the certificate actually belongs to this hostname -- verify_peer
    // alone only proves the cert chains to a trusted CA, not that it is *theirs*.
    ws.next_layer().set_verify_callback(ssl::host_name_verification(host));

    // 4. TLS handshake (on the ssl layer, reached via next_layer())
    ws.next_layer().handshake(ssl::stream_base::client);

    // The Host header must carry the port we actually connected on.
    const std::string host_hdr = host + ':' + std::to_string(ep.port());

    ws.set_option(websocket::stream_base::decorator(
        [](websocket::request_type& req) {
            req.set(http::field::user_agent, "crypto-ml-pipeline-feed-handler");
        }));

    // 5. Websocket handshake (the HTTP Upgrade)
    ws.handshake(host_hdr, "/");

    // Coinbase sends and expects text frames; Beast defaults to binary.
    ws.text(true);

    // Must arrive within 5s of connecting or the server drops us.
    ws.write(net::buffer(subscribe));

    stats.sessions.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "connected to " << host << " (session "
              << stats.sessions.load() << ")\n";

    // 6. Read loop. This thread now does nothing but read and hand off.
    beast::flat_buffer buffer;

    while (!g_stop.load(std::memory_order_acquire))
    {
        // read FIRST -- the buffer is empty until this fills it
        buffer.clear();
        ws.read(buffer);

        // Stamp the clock before ANY work on the frame. Parsing first would
        // fold our own parser cost into the ingestion-latency number.
        //
        // system_clock, not steady_clock: steady_clock's epoch is arbitrary
        // (typically time since boot), so it cannot be compared against
        // Coinbase's wall-clock "time" field at all. steady_clock is for
        // durations measured inside this process; system_clock is for
        // "how stale is this tick".
        Raw raw;
        raw.recv = std::chrono::system_clock::now();
        raw.enqueued = steady::now();
        raw.ticket = ticket++;
        raw.payload = beast::buffers_to_string(buffer.data());

        stats.frames.fetch_add(1, std::memory_order_relaxed);

        if (recorder)
            *recorder << raw.payload << "\n";

        offer(raw_q, std::move(raw), cfg, stats);
    }

    // Only reached on a clean stop; a failure throws out of here instead.
    ws.close(websocket::close_code::normal);
}


// Replay a recorded frame file. Same downstream path as the live feed, which is
// the point: benchmarks stop depending on what the market happened to be doing.
template <class RawQ>
static void replay_file(const Config& cfg, RawQ& raw_q, Stats& stats, uint64_t& ticket)
{
    std::ifstream in(cfg.replay);
    if (!in)
        throw std::runtime_error("cannot open replay file " + cfg.replay.string());

    const auto started = steady::now();
    uint64_t n = 0;
    std::string line;

    while (std::getline(in, line) && !g_stop.load(std::memory_order_acquire))
    {
        if (line.empty())
            continue;

        // Pacing. speed 0 means "as fast as the pipeline will take it", which
        // is the only setting that actually measures the pipeline rather than
        // measuring the exchange.
        if (cfg.speed > 0.0)
        {
            const auto target = started + std::chrono::duration_cast<steady::duration>(
                std::chrono::duration<double>(static_cast<double>(n) / cfg.speed));
            std::this_thread::sleep_until(target);
        }

        Raw raw;
        raw.recv = std::chrono::system_clock::now();
        raw.enqueued = steady::now();
        raw.ticket = ticket++;
        raw.payload = std::move(line);

        stats.frames.fetch_add(1, std::memory_order_relaxed);
        offer(raw_q, std::move(raw), cfg, stats);
        ++n;
    }
}


// ---------------------------------------------------------------------------
// wiring
// ---------------------------------------------------------------------------

static void print_latency(std::vector<double>& v)
{
    if (v.empty())
    {
        std::cerr << "  (no samples)\n";
        return;
    }
    std::sort(v.begin(), v.end());
    auto at = [&](double q) { return v[std::min(v.size() - 1, static_cast<std::size_t>(v.size() * q))]; };
    std::cerr << "  pipeline latency  p50 " << at(0.50) << " ms"
              << "   p99 " << at(0.99) << " ms"
              << "   max " << v.back() << " ms\n";
}


template <class RawQ>
static int run_pipeline(const Config& cfg)
{
    Stats stats;

    std::unique_ptr<RawQ> raw_q;
    if constexpr (std::is_same_v<RawQ, SpscRing<Raw>>)
        raw_q = std::make_unique<RawQ>(cfg.capacity);
    else
        raw_q = std::make_unique<RawQ>(cfg.capacity, cfg.overflow);

    BoundedQueue<Parsed> parsed_q{cfg.capacity, Overflow::Block};

    // Two separate flags, because the stages shut down in order: the source
    // stops, the parsers drain and stop, only then may the writer stop. One
    // shared flag would let the writer quit on records still in a parser.
    std::atomic<bool> source_done{false};
    std::atomic<bool> parsers_done{false};

    TickWriter writer{cfg.out};
    std::vector<double> latency;

    std::thread writer_thread(writer_loop, std::ref(parsed_q), std::ref(writer),
                              std::ref(stats), std::cref(parsers_done),
                              cfg.bench, cfg.bench ? &latency : nullptr);

    std::vector<std::thread> parser_threads;
    for (unsigned i = 0; i < cfg.parsers; ++i)
        parser_threads.emplace_back(parser_loop<RawQ>, std::ref(*raw_q), std::ref(parsed_q),
                                    std::ref(stats), std::cref(source_done));

    std::unique_ptr<std::ofstream> recorder;
    if (!cfg.record.empty())
    {
        recorder = std::make_unique<std::ofstream>(cfg.record, std::ios::out | std::ios::app);
        if (!*recorder)
            throw std::runtime_error("cannot open record file " + cfg.record.string());
        std::cerr << "recording raw frames to " << cfg.record.string() << "\n";
    }

    uint64_t ticket = 0;
    const auto t0 = steady::now();

    if (!cfg.replay.empty())
    {
        replay_file(cfg, *raw_q, stats, ticket);
    }
    else
    {
        int backoff = 1;   // seconds, doubling, capped
        while (!g_stop.load(std::memory_order_acquire))
        {
            try
            {
                run_session(cfg, *raw_q, stats, ticket, recorder.get());
                backoff = 1;   // a clean return means the connection was healthy
            }
            catch (const std::exception& e)
            {
                if (g_stop.load(std::memory_order_acquire))
                    break;

                std::cerr << "session ended: " << e.what()
                          << " -- reconnecting in " << backoff << "s\n";

                // Sleep in slices so Ctrl+C during the backoff is still prompt.
                for (int i = 0; i < backoff * 5 && !g_stop.load(std::memory_order_acquire); ++i)
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));

                backoff = std::min(backoff * 2, 30);
            }
        }
    }

    const double elapsed = std::chrono::duration<double>(steady::now() - t0).count();

    // Shut the stages down in pipeline order so nothing in flight is lost.
    source_done.store(true, std::memory_order_release);
    if constexpr (!std::is_same_v<RawQ, SpscRing<Raw>>)
        raw_q->close();
    for (auto& t : parser_threads)
        t.join();

    parsers_done.store(true, std::memory_order_release);
    parsed_q.close();
    writer_thread.join();

    writer.close();
    if (recorder)
        recorder->flush();

    std::cerr << "\nstopped after " << stats.sessions.load() << " session(s), "
              << elapsed << "s\n"
              << "  frames read   : " << stats.frames.load() << "\n"
              << "  ticks parsed  : " << stats.ticks.load() << "\n"
              << "  ticks written : " << stats.written.load() << "\n"
              << "  unparseable   : " << stats.unparseable.load() << "\n"
              << "  dropped       : " << stats.dropped.load() << "\n"
              << "  out-of-order  : " << stats.out_of_order.load() << "\n"
              << "  files touched : " << writer.files() << "\n";

    if (cfg.bench)
    {
        std::cerr << "\nbenchmark  queue=" << (cfg.queue == QueueKind::Spsc ? "spsc" : "mutex")
                  << "  parsers=" << cfg.parsers
                  << "  capacity=" << cfg.capacity << "\n"
                  << "  throughput        " << (elapsed > 0 ? stats.frames.load() / elapsed : 0.0)
                  << " frames/sec\n";
        print_latency(latency);
    }

    return EXIT_SUCCESS;
}


static bool parse_args(int argc, char** argv, Config& cfg)
{
    auto need = [&](int& i) -> std::string {
        if (i + 1 >= argc)
            throw std::runtime_error(std::string("missing value for ") + argv[i]);
        return argv[++i];
    };

    for (int i = 1; i < argc; ++i)
    {
        const std::string a = argv[i];
        if      (a == "--out")      cfg.out = need(i);
        else if (a == "--record")   cfg.record = need(i);
        else if (a == "--replay")   cfg.replay = need(i);
        else if (a == "--parsers")  cfg.parsers = static_cast<unsigned>(std::stoul(need(i)));
        else if (a == "--capacity") cfg.capacity = static_cast<std::size_t>(std::stoul(need(i)));
        else if (a == "--speed")    cfg.speed = std::stod(need(i));
        else if (a == "--bench")    cfg.bench = true;
        else if (a == "--queue")
        {
            const std::string v = need(i);
            cfg.queue = (v == "spsc") ? QueueKind::Spsc : QueueKind::Mutex;
        }
        else if (a == "--overflow")
        {
            const std::string v = need(i);
            cfg.overflow = (v == "drop") ? Overflow::DropNewest : Overflow::Block;
        }
        else if (a == "--help" || a == "-h")
        {
            return false;
        }
        else
        {
            throw std::runtime_error("unknown option " + a);
        }
    }

    if (cfg.parsers < 1)
        cfg.parsers = 1;

    // The ring's whole correctness argument is one producer and one consumer.
    // A parser pool makes it single-producer/multi-consumer, which it is not,
    // so refuse rather than race.
    if (cfg.queue == QueueKind::Spsc && cfg.parsers != 1)
    {
        std::cerr << "note: --queue spsc is single-consumer by construction; "
                     "forcing --parsers 1\n";
        cfg.parsers = 1;
    }

    if (cfg.bench && cfg.replay.empty())
        throw std::runtime_error("--bench needs --replay FILE");

    return true;
}


int main(int argc, char** argv)
{
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
#ifdef SIGBREAK
    // Windows only. A harness can't deliver Ctrl+C to one child process, only
    // Ctrl+Break to its process group -- and unhandled, that kills the process
    // without the drain. This is what makes graceful shutdown testable.
    std::signal(SIGBREAK, on_signal);
#endif

    try
    {
        Config cfg;
        if (!parse_args(argc, argv, cfg))
        {
            std::cerr << "see the header of src/main.cpp for the full option list\n";
            return EXIT_SUCCESS;
        }

        if (cfg.bench)
            std::cerr << "bench mode: parsing and ordering, not writing to disk\n";
        else
            std::cerr << "writing to " << fs::absolute(cfg.out).string() << "\n";

        return cfg.queue == QueueKind::Spsc
                   ? run_pipeline<SpscRing<Raw>>(cfg)
                   : run_pipeline<BoundedQueue<Raw>>(cfg);
    }
    catch (std::exception const& e)
    {
        std::cerr << "Fatal: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
