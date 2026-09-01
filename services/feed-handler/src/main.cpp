// Reference:
//   Beast sync-ssl websocket client (start here):
//     https://github.com/boostorg/beast/tree/develop/example/websocket/client/sync-ssl
//   Beast docs:
//     https://www.boost.org/doc/libs/release/libs/beast/doc/html/
//   Coinbase Exchange websocket:
//     https://docs.cdp.coinbase.com/exchange/docs/websocket-overview

// Core Beast & Asio Sockets
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>

// REQUIRED FOR WSS:// (SSL/TLS Encryption)
#include <boost/beast/ssl.hpp>
#include <boost/asio/ssl.hpp>

// Standard Utilities
#include <iostream>
#include <string>
#include <memory>


//straight from 
//https://www.boost.org/doc/libs/latest/libs/beast/doc/html/beast/quick_start/websocket_client.html
namespace beast = boost::beast;         // from <boost/beast.hpp>
namespace http = beast::http;           // from <boost/beast/http.hpp>
namespace websocket = beast::websocket; // from <boost/beast/websocket.hpp>
namespace net = boost::asio;            // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>




int main() {
    return 0;
}
