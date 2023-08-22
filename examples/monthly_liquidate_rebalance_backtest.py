from __future__ import annotations

import calendar
import datetime

from munch import Munch

from qstrader import settings
from qstrader.compat import queue
from qstrader.event import SignalEvent, EventType, BarEvent, TickEvent
from qstrader.position_sizer.rebalance import LiquidateRebalancePositionSizer
from qstrader.strategy.base import AbstractStrategy
from qstrader.trading_session import TradingSession


class MonthlyLiquidateRebalanceStrategy(AbstractStrategy):
    """
    A generic strategy that allows monthly rebalancing of a
    set of tickers, via full liquidation and dollar-weighting
    of new positions.

    Must be used in conjunction with the
    LiquidateRebalancePositionSizer object to work correctly.
    """

    def __init__(self, tickers: list[str], events_queue: queue.Queue):
        self.tickers: list[str] = tickers
        self.events_queue: queue.Queue = events_queue
        self.tickers_invested: dict[str | bool] = self._create_invested_list()

    def _end_of_month(self, cur_time: datetime.datetime) -> bool:
        """
        Determine if the current day is at the end of the month.
        """
        cur_day: int = cur_time.day
        end_day: int = calendar.monthrange(cur_time.year, cur_time.month)[1]
        return cur_day == end_day

    def _create_invested_list(self) -> dict[str, bool]:
        """
        Create a dictionary with each ticker as a key, with
        a boolean value depending upon whether the ticker has
        been "invested" yet. This is necessary to avoid sending
        a liquidation signal on the first allocation.
        """
        tickers_invested: dict[str, bool] = {ticker: False for ticker in self.tickers}
        return tickers_invested

    def calculate_signals(self, event: BarEvent | TickEvent):
        """
        For a particular received BarEvent, determine whether
        it is the end of the month (for that bar) and generate
        a liquidation signal, as well as a purchase signal,
        for each ticker.
        """
        if event.type in [EventType.BAR, EventType.TICK] and self._end_of_month(
            event.time
        ):
            ticker: str = event.ticker
            if self.tickers_invested[ticker]:
                liquidate_signal: SignalEvent = SignalEvent(ticker, "EXIT")
                self.events_queue.put(liquidate_signal)

            long_signal: SignalEvent = SignalEvent(ticker, "BOT")
            self.events_queue.put(long_signal)

            self.tickers_invested[ticker] = True


def run(config: Munch, testing: bool, tickers: list[str], filename: str):
    # Backtest information
    title: list[str] = ["Monthly Liquidate/Rebalance on 60%/40% SPY/AGG Portfolio"]
    initial_equity = 500000.0
    start_date = datetime.datetime(2006, 11, 1)
    end_date = datetime.datetime(2016, 10, 12)

    # Use the Monthly Liquidate And Rebalance strategy
    events_queue = queue.Queue()
    strategy = MonthlyLiquidateRebalanceStrategy(tickers, events_queue)

    # Use the liquidate and rebalance position sizer
    # with prespecified ticker weights
    ticker_weights = {
        "SPY": 0.6,
        "AGG": 0.4,
    }
    position_sizer = LiquidateRebalancePositionSizer(ticker_weights)

    # Set up the backtest

    backtest: TradingSession = TradingSession(
        config=config,
        strategy=strategy,
        tickers=tickers,
        equity=initial_equity,
        start_date=start_date,
        end_date=end_date,
        events_queue=events_queue,
        title=title,
        benchmark=tickers[0],
        position_sizer=position_sizer,
    )

    results = backtest.start_trading(testing=testing)
    return results


if __name__ == "__main__":
    # Configuration data
    testing = False
    config: Munch = settings.from_file(settings.DEFAULT_CONFIG_FILENAME, testing)
    tickers: list[str] = ["SPY", "AGG"]
    filename: None | str = None
    run(config, testing, tickers, filename)
