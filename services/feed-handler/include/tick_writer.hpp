#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "tick.hpp"

// Append-only JSONL sink, partitioned by symbol / UTC date / UTC hour:
//
//   <root>/symbol=BTC-USD/dt=2026-09-07/hour=07/ticks.jsonl
//
// GoalState's output contract asks for Parquet in that layout. Producing Parquet
// from C++ means pulling in Apache Arrow -- a heavy MSYS2 dependency for no
// learning -- and the contract already names the way out: DuckDB promotes these
// files in the batch layer, the same way the legacy JSONL backfills.
//
//   COPY (SELECT * FROM read_json_auto('data/raw/ticks/**/*.jsonl'))
//     TO 'data/raw/ticks_pq' (FORMAT PARQUET, PARTITION_BY (product_id));
//
// Deliberately NOT thread-safe. Phase 1 step 2 puts a bounded queue in front of
// it with a single writer thread owning the instance -- that is the seam, and a
// mutex in here would make the queue pointless.
class TickWriter
{
public:
    // flush_every / flush_interval bound how much a hard kill can lose. Records
    // sit in the stream buffer until one of them trips, so the guarantee is
    // "at most `flush_every` records, and at most `flush_interval` of time".
    // The interval matters on a quiet product, where 100 records could be an
    // hour of traffic; the count matters on a busy one, where 2s is thousands.
    explicit TickWriter(std::filesystem::path root,
                        unsigned flush_every = 100,
                        std::chrono::seconds flush_interval = std::chrono::seconds{2});
    ~TickWriter();

    TickWriter(const TickWriter&) = delete;
    TickWriter& operator=(const TickWriter&) = delete;

    void write(const Tick& t);
    void flush();
    void close();

    uint64_t records() const { return records_; }
    uint64_t files() const { return files_; }
    const std::filesystem::path& current_file() const { return current_file_; }

private:
    void open_for(const std::string& symbol, const std::string& partition);

    std::filesystem::path root_;
    unsigned flush_every_;
    std::chrono::seconds flush_interval_;

    std::ofstream out_;
    std::filesystem::path current_file_;
    std::string open_symbol_;
    std::string open_partition_;

    unsigned since_flush_ = 0;
    std::chrono::steady_clock::time_point last_flush_{};

    uint64_t records_ = 0;
    uint64_t files_ = 0;
};

// "2026-09-07T07:26:53.123456Z" -- ISO 8601, microseconds, always UTC.
std::string iso8601_utc(std::chrono::system_clock::time_point tp);
