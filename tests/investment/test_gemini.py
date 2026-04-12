"""Tests for Gemini exchange ingestion."""

from unittest.mock import MagicMock, patch

from ingestion.investment.gemini import fetch_positions

_MOCK_BALANCES = [
    {"currency": "BTC", "amount": "0.5", "available": "0.5"},
    {"currency": "ETH", "amount": "3.2", "available": "3.2"},
    {"currency": "USD", "amount": "1500.00", "available": "1500.00"},
    {"currency": "LTC", "amount": "0.0", "available": "0.0"},  # zero — should be excluded
]


def test_fetch_positions_excludes_zero_balances():
    with patch("ingestion.investment.gemini.requests.post") as mock_post, \
         patch("ingestion.investment.gemini._get_spot_price", return_value=50000.0):

        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.json.return_value = _MOCK_BALANCES
        mock_post.return_value.raise_for_status = MagicMock()

        positions = fetch_positions(api_key="test-key", api_secret="test-secret")

    symbols = [p.symbol for p in positions]
    assert "LTC" not in symbols
    assert len(positions) == 3


def test_fetch_positions_usd_is_cash():
    with patch("ingestion.investment.gemini.requests.post") as mock_post, \
         patch("ingestion.investment.gemini._get_spot_price", return_value=50000.0):

        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.json.return_value = _MOCK_BALANCES
        mock_post.return_value.raise_for_status = MagicMock()

        positions = fetch_positions(api_key="test-key", api_secret="test-secret")

    usd = next(p for p in positions if p.symbol == "USD")
    assert usd.asset_class == "cash"
    assert usd.price == 1.0


def test_fetch_positions_crypto_asset_class():
    with patch("ingestion.investment.gemini.requests.post") as mock_post, \
         patch("ingestion.investment.gemini._get_spot_price", return_value=50000.0):

        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.json.return_value = _MOCK_BALANCES
        mock_post.return_value.raise_for_status = MagicMock()

        positions = fetch_positions(api_key="test-key", api_secret="test-secret")

    btc = next(p for p in positions if p.symbol == "BTC")
    assert btc.asset_class == "crypto"
    assert btc.source == "gemini"
    assert btc.quantity == 0.5
    assert btc.market_value == 0.5 * 50000.0
