# feed-handler

A C++20 service that holds a websocket to the Coinbase Exchange `ticker` channel and
appends every trade to an hour-partitioned raw zone. Replaces REST polling, which could
only ever capture one price per run and lost everything in between.

Architecture diagram for the whole pipeline:
https://claude.ai/code/artifact/eaccaa07-d15f-4675-a6c1-71f1c4ada9ad

```
source thread --raw--> queue --> parser pool --> queue --> writer thread
                 (ring or mutex)    (1..N)      (mutex)         |
                                                           ticks.jsonl
```

The source is either the live websocket or a recorded frame file, which is what makes the
benchmarks below reproducible.

## Build

MSYS2 / mingw64, with Boost (headers only — Beast is header-only), OpenSSL and
nlohmann_json installed:

```sh
cmake --preset mingw
cmake --build --preset mingw
```

*Do not remove `CMAKE_NO_SYSTEM_FROM_IMPORTED ON` from CMakeLists.* All three dependencies
live in `/mingw64/include`, already a default system include dir. Without that flag CMake
re-adds it as `-isystem`, which reorders the search path and breaks libstdc++'s
`#include_next <stdlib.h>`. It fails as `stdlib.h: No such file or directory`, which points
nowhere near the cause.

## Run

```sh
./build/feed_handler.exe                              # live, writes data/raw/ticks
./build/feed_handler.exe --record frames.jsonl        # live, also keep the raw frames
./build/feed_handler.exe --replay frames.jsonl --out scratch/replay              # feed a recording back through
./build/feed_handler.exe --replay frames.jsonl --out scratch/replay --pace 100   # ...at 100x market speed
./build/feed_handler.exe --bench --replay frames.jsonl --queue spsc
```

**Always give a replay its own `--out`.** Replay stamps `recv_time` with the current clock,
so replaying into the default raw zone writes a fake hour of duplicate trades, and
`pipeline/sql/tick_bars.sql` will count every one of them.

| Option | Default | Meaning |
|---|---|---|
| `--out DIR` | `data/raw/ticks` | raw zone root |
| `--queue mutex\|spsc` | `mutex` | which queue implementation to run |
| `--parsers N` | `1` | parser threads (`spsc` forces 1 — see below) |
| `--capacity N` | `4096` | queue slots |
| `--overflow block\|drop` | `block` | what happens when the queue is full |
| `--record FILE` | — | append raw frames for later replay |
| `--replay FILE` | — | read frames from FILE instead of the network |
| `--speed N` | `0` | replay pacing in frames/sec; 0 = unpaced |
| `--pace N` | — | replay at N× the recorded market speed, scheduled from each frame's exchange time |
| `--bench` | off | replay, skip the disk write, report throughput and latency |

Ctrl+C drains the pipeline in order — source, then parsers, then writer — and flushes
before exiting. So does Ctrl+Break, which matters on Windows: a test harness can't send
Ctrl+C to a single child process, only Ctrl+Break to its process group.

`--pace` prints the pace it actually achieved and its worst-late frame. On the 86-second
sample: 9.99× when asked for 10×, 98.7× for 100×. Frames are scheduled against absolute
targets, so the Windows timer's ~15 ms granularity makes single frames late (15–30 ms
worst case) without the lateness accumulating; the shortfall at 100× is that granularity
on a replay that only lasts 0.87 s.

## Output

```
data/raw/ticks/symbol=BTC-USD/dt=2026-09-07/hour=07/ticks.jsonl
```

Append-only, one JSON object per line, twelve fields. Eleven come from Coinbase; `recv_time`
is our own clock read at the moment `ws.read()` returned, so `recv_time - time` is the
ingestion latency.

Parquet is deliberately **not** written here. Producing it natively means linking Apache
Arrow into the MinGW build — a heavy dependency for no learning — when DuckDB promotes
these files in one statement in the batch layer.

## Benchmarks

129,601 recorded BTC-USD frames replayed unpaced, `--bench` (parse, order, and serialise;
no disk write). Windows 10, MinGW GCC 14.2, RelWithDebInfo.

| Configuration | Throughput | p50 | p99 |
|---|---:|---:|---:|
| Python baseline (`bench_python.py`) | 135,526 f/s | — | — |
| C++ mutex queue, 1 parser | 132,827 f/s | 31.2 ms | 37.0 ms |
| C++ SPSC ring, 1 parser | 145,001 f/s | 28.7 ms | 38.3 ms |
| C++ mutex queue, 2 parsers | **149,854 f/s** | 28.2 ms | **29.4 ms** |
| C++ mutex queue, 4 parsers | 83,428 f/s | 51.2 ms | 53.3 ms |

**Read these honestly, because they do not say what a C++ rewrite is supposed to say.**

- **Python is faster than single-parser C++.** 135.5k vs 132.8k frames/sec. The best C++
  configuration beats Python by 10.6%, which is not the order-of-magnitude win the language
  choice implies. CPython's `json` module is C underneath and well optimised;
  nlohmann::json builds a full DOM per frame and is not a fast parser. **The bottleneck is
  the JSON library, not the queue** — which means every microsecond spent on lock-free
  indices was spent on the wrong end of the pipeline. The real optimisation is a faster
  parser (simdjson, RapidJSON) or extracting the eleven fields without building a DOM.

- **The lock-free ring is worth ~9%** over the mutex queue at one parser (145.0k vs 132.8k).
  Real, measurable, and much smaller than the effort suggests.

- **Two parsers is the sweet spot; four is 44% slower** than two. Past the point where
  parsing is the constraint, more threads buy contention on the parsed queue and a deeper
  reorder map, not throughput.

- **The latency column mostly measures saturation, not per-item cost.** Unpaced replay means
  the producer always outruns the consumer, so the queue sits full and latency settles at
  roughly `capacity ÷ throughput` — 4096 ÷ 133k ≈ 31 ms, which is what the table shows.
  Throughput is the number that carries information here.

- **The live feed runs at about 10 frames/sec.** Every configuration is four orders of
  magnitude above that. None of this concurrency is solving a live bottleneck; it is burst
  headroom and, honestly, an exercise. Say that rather than implying the service needed it.

Reproduce:

```sh
./build/feed_handler.exe --record frames.jsonl          # capture a live sample, Ctrl+C
for i in $(seq 1 200); do cat frames.jsonl >> bench.jsonl; done
./build/feed_handler.exe --bench --replay bench.jsonl --queue mutex --parsers 1
python bench_python.py bench.jsonl
```

## Load tests

`tests/test_feed_handler.py` drives the built binary as a black box: synthetic Coinbase
ticker frames in, the written `ticks.jsonl` read back and counted. Every run gets its own
`--out` under a temp directory. `uv run pytest tests/test_feed_handler.py` — six tests,
about 11 s.

```
block, 16 slots, 4 parsers    written 200,000   dropped       0   of 200,000
drop,  16 slots, 1 parser     written  13,615   dropped 186,385   of 200,000   <- control
Ctrl+Break at 1.5 s           read 29,793 -> parsed 29,792 -> written 29,792, dropped 0, exit 0
--pace 100                    99.89x achieved: 300 s of market in 3.003 s
```

**The zero-loss result only means something next to the control.** A 16-slot queue against
an unpaced 200k-frame replay is almost pure backpressure: under `--overflow drop` the same
load sheds 93% of frames. Under `--overflow block` it sheds none, and every trade id comes
out once, in feed order.

**Shutdown loses nothing that was accepted.** The replay is paced to take ~10 s and
interrupted at 1.5 s. Every frame the source had read — minus the subscriptions ack, which
is counted as read and never becomes a tick — was parsed and written, the process exited 0,
and every file ends on a complete record.

The suite also checks that 1-parser mutex, 4-parser mutex and SPSC runs write identical
output, which was the manual check in the design note below.

## Design notes

**The queue is bounded, and the overflow policy is explicit.** Unbounded is not a policy: if
the consumer falls behind, an unbounded queue turns a throughput problem into an
out-of-memory crash hours later, far from the cause. `--overflow block` never loses a record
and stalls the reader; `--overflow drop` never stalls and counts what it sheds. Both are
defensible; the count is a metric either way.

**The SPSC ring is single-consumer by construction, so `--queue spsc` forces `--parsers 1`.**
One producer and one consumer is the entire reason it needs no mutex — each index is written
by exactly one thread. A parser pool would make it single-producer/multi-consumer, which it
is not, so the option refuses rather than racing.

**Ordering survives the parser pool.** Every frame gets a ticket on the way in; the writer
holds anything that arrives early and only emits the contiguous run from the next expected
ticket. Frames that produce no tick (the subscribe ack, a malformed body) still get a ticket
with an empty payload, or the writer would wait forever on a record that never comes.
Verified: 1-parser mutex, 4-parser mutex and SPSC all produce byte-identical output in feed
order — now an automated test, see Load tests.

**`sequence` gaps are not data loss.** On the `ticker` channel `sequence` counts every message
the product's feed generates, and ticker is a filtered view of it — 878 of 878 consecutive
pairs jumped in a sample. Only a non-increasing sequence is an error, and that is what is
counted.

**A blocking read has a 30s socket timeout.** A half-open connection produces no bytes and no
error, so an untimed `ws.read()` waits forever and the feed silently stops.
`beast::tcp_stream::expires_after` does not help — its timers only apply to async operations
and this is the synchronous client, so it is `SO_RCVTIMEO` on the native handle.

## Not done

- **ThreadSanitizer has never run.** MinGW does not ship it. `-DENABLE_TSAN=ON` is wired for
  a clang/gcc toolchain that has it (WSL, Linux CI). The parser pool is the first real
  concurrency in this repo and it has not been checked by a race detector.
- **72-hour soak.** Reconnect with backoff and the socket timeout are in; the longest run so
  far is minutes.
- **Ingest latency needs a clock check.** A live sample gave p50 39 ms with a minimum of
  −29 ms. A negative minimum means the local clock is behind Coinbase's, so absolute figures
  carry that skew until NTP is verified.
