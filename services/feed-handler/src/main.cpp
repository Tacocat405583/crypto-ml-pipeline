// Coinbase Exchange websocket feed -- connect, subscribe, parse, append to disk.
//
//   Beast sync-ssl example: https://github.com/boostorg/beast/tree/develop/example/websocket/client/sync-ssl
//   Beast docs:            https://www.boost.org/doc/libs/release/libs/beast/doc/html/
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
#include <optional>
#include <thread>

//Json
#include <nlohmann/json.hpp>

#include "tick.hpp"
#include "tick_writer.hpp"

namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
namespace fs        = std::filesystem;
using json = nlohmann::json;

using tcp           = boost::asio::ip::tcp;


// Set by the signal handler, read by the loops. A handler may only touch
// lock-free atomics -- no I/O, no allocation, no flushing the writer from in
// here. Everything that has to happen on shutdown happens back in main().
static std::atomic<bool> g_stop{false};

static void on_signal(int) { g_stop.store(true); }


struct Stats
{
    uint64_t frames = 0;      // websocket frames read
    uint64_t ticks = 0;       // frames that parsed into a Tick
    uint64_t unparseable = 0; // frames we could not read
    uint64_t out_of_order = 0; // duplicate or replayed sequence numbers
    uint64_t sessions = 0;    // connections opened
};


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


// One connection, start to finish. Returns normally when g_stop is set; throws
// on any network or protocol failure so the caller can reconnect.
static void run_session(const std::string& host,
                        const std::string& port,
                        const std::string& subscribe,
                        TickWriter& writer,
                        Stats& stats)
{
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
    if(!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host.c_str()))
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

    ++stats.sessions;
    std::cerr << "connected to " << host << " (session " << stats.sessions << ")\n";

    // Sequence is per-connection: a reconnect legitimately skips whatever was
    // published while we were away, so this resets rather than counting the
    // reconnect itself as a gap.
    int64_t last_sequence = 0;

    // 6. Read loop.
    beast::flat_buffer buffer;

    while (!g_stop.load())
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
        const auto recv = std::chrono::system_clock::now();
        ++stats.frames;

        std::string s = beast::buffers_to_string(buffer.data());

        // allow_exceptions=false -- a malformed frame must not throw out of
        // the read loop, it gets counted and skipped.
        json j = json::parse(s, nullptr, /*allow_exceptions=*/false);
        if (j.is_discarded())
        {
            ++stats.unparseable;
            continue;
        }

        // skip the subscriptions ack, errors, and anything we don't handle
        //from here https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
        // .value() rather than j["type"] -- see the note on to_double.
        if (j.value("type", "") != "ticker")
            continue;

        auto tick = parse_ticker(j, recv);
        if (!tick)
        {
            ++stats.unparseable;
            std::cerr << "unparseable ticker frame (" << stats.unparseable
                      << " so far): " << s << "\n";
            continue;
        }

        // NOT a gap check. `sequence` counts every message the product's feed
        // generates, and `ticker` is a filtered view of that stream -- only
        // matches and best-bid/ask changes reach us. Skipped numbers are the
        // normal case: a 25s sample had a jump on 878 of 878 consecutive pairs,
        // so treating a jump as loss would flag every single message.
        //
        // What IS an error is the sequence standing still or going backwards.
        // That means a duplicate or a replay, and it would double-count a trade
        // downstream, so it gets counted rather than silently written.
        if (last_sequence != 0 && tick->sequence <= last_sequence)
        {
            ++stats.out_of_order;
            std::cerr << "out-of-order sequence: " << last_sequence
                      << " -> " << tick->sequence << "\n";
        }
        last_sequence = tick->sequence;

        writer.write(*tick);
        ++stats.ticks;

        if (stats.ticks % 500 == 0)
            std::cerr << stats.ticks << " ticks -> " << writer.current_file().string()
                      << "  (out-of-order " << stats.out_of_order << ")\n";
    }

    // Only reached on a clean stop; a failure throws out of here instead.
    ws.close(websocket::close_code::normal);
}


int main(int argc, char** argv)
{
    const std::string host = "ws-feed.exchange.coinbase.com";
    const std::string port = "443";
    const std::string subscribe =
        R"({"type":"subscribe","product_ids":["BTC-USD"],"channels":["ticker"]})";

    const fs::path root = (argc > 1) ? fs::path{argv[1]} : fs::path{"data/raw/ticks"};

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    Stats stats;
    int backoff = 1;   // seconds, doubling, capped

    try
    {
        TickWriter writer{root};
        std::cerr << "writing to " << fs::absolute(root).string() << "\n";

        while (!g_stop.load())
        {
            try
            {
                run_session(host, port, subscribe, writer, stats);
                backoff = 1;   // a clean return means the connection was healthy
            }
            catch (const std::exception& e)
            {
                if (g_stop.load())
                    break;

                // Whatever the failure was, the records already parsed are good;
                // get them on disk before waiting, so a reconnect loop cannot
                // sit on a buffer full of ticks.
                writer.flush();

                std::cerr << "session ended: " << e.what()
                          << " -- reconnecting in " << backoff << "s\n";

                // Sleep in slices so Ctrl+C during the backoff is still prompt.
                for (int i = 0; i < backoff * 5 && !g_stop.load(); ++i)
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));

                backoff = std::min(backoff * 2, 30);
            }
        }

        writer.close();

        std::cerr << "\nstopped after " << stats.sessions << " session(s)\n"
                  << "  frames read   : " << stats.frames << "\n"
                  << "  ticks written : " << stats.ticks << "\n"
                  << "  unparseable   : " << stats.unparseable << "\n"
                  << "  out-of-order  : " << stats.out_of_order << "\n"
                  << "  files touched : " << writer.files() << "\n";
    }
    catch(std::exception const& e)
    {
        std::cerr << "Fatal: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
