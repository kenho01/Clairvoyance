"""Unit tests for the categoriser (no real API calls)."""

import json
from unittest.mock import MagicMock, patch

from ingestion.bank.categoriser import CATEGORIES, _parse_response, categorise
from ingestion.bank.pdf_parser import RawTransaction


def _make_tx(description: str) -> RawTransaction:
    return RawTransaction(date="01 Jan 2024", description=description, amount=-10.00)


def test_parse_response_valid():
    raw = json.dumps([{"index": 0, "category": "Dining"}, {"index": 1, "category": "Transport"}])
    result = _parse_response(raw, offset=0)
    assert result == {0: "Dining", 1: "Transport"}


def test_parse_response_with_offset():
    raw = json.dumps([{"index": 0, "category": "Groceries"}])
    result = _parse_response(raw, offset=5)
    assert result == {5: "Groceries"}


def test_all_categories_are_strings():
    assert all(isinstance(c, str) for c in CATEGORIES)


def test_categorise_attaches_category():
    transactions = [_make_tx("NTUC FAIRPRICE"), _make_tx("GRAB * TRIP")]
    fake_response_content = json.dumps([
        {"index": 0, "category": "Groceries"},
        {"index": 1, "category": "Transport"},
    ])

    mock_message = MagicMock()
    mock_message.content = [MagicMock(text=fake_response_content)]

    with patch("ingestion.bank.categoriser.anthropic.Anthropic") as mock_client_cls:
        mock_client_cls.return_value.messages.create.return_value = mock_message
        result = categorise(transactions, api_key="test-key")

    assert result[0].category == "Groceries"
    assert result[1].category == "Transport"
