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

namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
using tcp           = boost::asio::ip::tcp;

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

        // 6. Read loop. First frame back is the "subscriptions" ack, then ticks.
        beast::flat_buffer buffer;
        for(int i = 0; i < 5; ++i)
        {
            buffer.clear();
            ws.read(buffer);
            std::cout << beast::make_printable(buffer.data()) << "\n\n";
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
