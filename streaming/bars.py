"""One-minute OHLCV bars built incrementally from a stream of ticks.

Pure logic, no Kafka: the consumer feeds ticks in as they arrive and writes
whatever bars come back. Kept apart from the broker so it can be checked
against the batch answer -- the same ticks aggregated by DuckDB -- directly.

A symbol's bar for minute m is emitted when its first tick for a later minute
arrives. Bucketing is on recv_time, the clock tick_bars.sql uses. Ticks must
arrive in feed order per symbol, which the feed handler guarantees and a topic
keyed by symbol preserves. A tick for a minute already emitted is counted as
late and dropped -- never merged into a bar that has already been written.
"""

import datetime as dt
from dataclasses import dataclass


@dataclass
class Bar:
    symbol: str
    minute: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int


class BarBuilder:
    def __init__(self) -> None:
        self.open_bars: dict[str, Bar] = {}
        self._emitted: dict[str, dt.datetime] = {}
        self.late = 0

    def add(self, symbol: str, recv_time: dt.datetime, price: float, size: float) -> list[Bar]:
        minute = recv_time.replace(second=0, microsecond=0)
        if symbol in self._emitted and minute <= self._emitted[symbol]:
            self.late += 1
            return []

        done = []
        bar = self.open_bars.get(symbol)
        if bar is not None and minute > bar.minute:
            done.append(bar)
            self._emitted[symbol] = bar.minute
            bar = None
        elif bar is not None and minute < bar.minute:
            self.late += 1                      # older than the bar still open
            return []

        if bar is None:
            self.open_bars[symbol] = Bar(symbol, minute, price, price, price, price, size, 1)
        else:
            bar.high = max(bar.high, price)
            bar.low = min(bar.low, price)
            bar.close = price
            bar.volume += size
            bar.trades += 1
        return done

    def flush(self) -> list[Bar]:
        """Emit every open bar: end of a finite replay, or a clean shutdown."""
        done = list(self.open_bars.values())
        for b in done:
            self._emitted[b.symbol] = b.minute
        self.open_bars.clear()
        return done
