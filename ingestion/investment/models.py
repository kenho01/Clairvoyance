from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Position:
    source: str # "ibkr" | "tiger" | "gemini"
    symbol: str # e.g. "AAPL", "BTC"
    asset_class: str # "stock" | "crypto" | "etf" | "cash"
    quantity: float
    price: float # price per unit in local currency
    market_value: float  # quantity * price
    currency: str # e.g. "USD", "SGD"
    fx_rate_to_sgd: float = 1.0
    market_value_sgd: float = 0.0

    def to_dict(self) -> dict:
        return self.__dict__
