#pragma once

#include <chrono>
#include <cstdint>
#include <string>

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
