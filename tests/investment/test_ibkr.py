"""Tests for IBKR Flex Query ingestion."""

from unittest.mock import MagicMock, patch

from ingestion.investment.ibkr import _parse_positions

_SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement>
      <OpenPositions>
        <OpenPosition
          symbol="AAPL"
          assetCategory="STK"
          position="50"
          markPrice="178.50"
          positionValue="8925.00"
          currency="USD"
        />
        <OpenPosition
          symbol="QQQ"
          assetCategory="ETF"
          position="10"
          markPrice="420.00"
          positionValue="4200.00"
          currency="USD"
        />
      </OpenPositions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


def test_parse_positions_returns_correct_count():
    positions = _parse_positions(_SAMPLE_XML)
    assert len(positions) == 2


def test_parse_positions_fields():
    positions = _parse_positions(_SAMPLE_XML)
    aapl = next(p for p in positions if p.symbol == "AAPL")

    assert aapl.source == "ibkr"
    assert aapl.asset_class == "stock"
    assert aapl.quantity == 50.0
    assert aapl.price == 178.50
    assert aapl.market_value == 8925.00
    assert aapl.currency == "USD"


def test_parse_positions_etf_asset_class():
    positions = _parse_positions(_SAMPLE_XML)
    qqq = next(p for p in positions if p.symbol == "QQQ")
    assert qqq.asset_class == "etf"
