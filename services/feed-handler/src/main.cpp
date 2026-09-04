// Coinbase Exchange websocket feed -- step 1: connect, subscribe, print frames.
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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>

//Json
#include <nlohmann/json.hpp>

namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
using json = nlohmann::json;

using tcp           = boost::asio::ip::tcp;


// One trade from the Coinbase ticker channel.
// Field reference: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
//
// Only "sequence" and "trade_id" arrive as real JSON numbers. Everything else
// is a quoted string -- .get<double>() throws, use std::stod on those.
//
//   1. sequence        int64_t   gap vs previous = a dropped message
//   2. price           double    (string in JSON)
//   3. product_id      string    "BTC-USD"
//   4. last_size       double    (string in JSON) size of this trade
//   5. side            string    "buy" / "sell" -- which side initiated
//   6. best_bid        double    (string in JSON) top of book
//   7. best_bid_size   double    (string in JSON)
//   8. best_ask        double    (string in JSON)
//   9. best_ask_size   double    (string in JSON)
//  10. time            string    exchange timestamp, ISO8601
//  11. trade_id        int64_t
//
//  12. recv_time  -- LAST, and not from Coinbase. Your own clock read at the
//                    moment ws.read() returns. (recv_time - time) is your
//                    ingestion latency, which is the README benchmark number.
struct Tick
{
    int64_t sequence = 0;
    double price = 0.0;
    std::string product_id;
    double last_size = 0.0;
    std::string side;
    double best_bid = 0.0;
    double best_bid_size = 0.0;
    double best_ask = 0.0;
    double best_ask_size = 0.0;
    std::string time;
    int64_t trade_id = 0;
    std::chrono::system_clock::time_point recv_time;
};


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


int main()
{
    const std::string host = "ws-feed.exchange.coinbase.com";
    const std::string port = "443";
    const std::string subscribe =
        R"({"type":"subscribe","product_ids":["BTC-USD"],"channels":["ticker"]})";

    try
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

        // 6. Read loop. Ctrl+C to stop.
        beast::flat_buffer buffer;
        uint64_t dropped = 0;   // unparseable frames; becomes a Phase 3 metric

        while (true)
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

            std::string s = beast::buffers_to_string(buffer.data());

            // allow_exceptions=false -- a malformed frame must not throw out of
            // the read loop, it gets counted and skipped.
            json j = json::parse(s, nullptr, /*allow_exceptions=*/false);
            if (j.is_discarded())
            {
                ++dropped;
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
                ++dropped;
                std::cerr << "unparseable ticker frame (" << dropped
                          << " so far): " << s << "\n";
                continue;
            }

            std::cout << tick->sequence
                      << "  " << tick->product_id
                      << "  " << tick->price
                      << "  " << tick->last_size
                      << "  " << tick->side << "\n";
        }

        ws.close(websocket::close_code::normal);
    }
    catch(std::exception const& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
