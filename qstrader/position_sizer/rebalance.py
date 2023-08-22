from __future__ import annotations

from math import floor

from qstrader.event import OrderEvent
from qstrader.portfolio import Portfolio
from qstrader.price_parser import PriceParser
from .base import AbstractPositionSizer


class LiquidateRebalancePositionSizer(AbstractPositionSizer):
    """
    Carries out a periodic full liquidation and rebalance of
    the Portfolio.

    This is achieved by determining whether an order type
    is "EXIT" or "BOT/SLD".

    If the former, the current quantity of shares in the ticker
    is determined and then BOT or SLD to net the position to zero.

    If the latter, the current quantity of shares to obtain is
    determined by prespecified weights and adjusted to reflect
    current account equity.
    """

    def __init__(self, ticker_weights: dict[str, float]) -> None:
        self.ticker_weights: dict[str, float] = ticker_weights

    def size_order(self, portfolio: Portfolio, initial_order: OrderEvent) -> OrderEvent:
        """
        Size the order to reflect the dollar-weighting of the
        current equity account size based on pre-specified
        ticker weights.
        """
        ticker: str = initial_order.ticker
        action: str = initial_order.action
        if action == "EXIT":
            # Obtain current quantity and liquidate
            cur_quantity: float = portfolio.positions[ticker].quantity

            if cur_quantity > 0:
                initial_order.action = "SLD"
                initial_order.quantity = cur_quantity
            else:
                initial_order.action = "BOT"
                initial_order.quantity = cur_quantity
        else:
            weight: float = self.ticker_weights[ticker]
            # Determine total portfolio value, work out dollar weight
            # and finally determine integer quantity of shares to purchase
            price: float = portfolio.price_handler.tickers[ticker]["adj_close"]
            price = PriceParser.display(price)
            equity: float | int = PriceParser.display(portfolio.equity)
            dollar_weight: float = weight * equity
            weighted_quantity: int = int(floor(dollar_weight / price))
            initial_order.quantity = weighted_quantity
        return initial_order
