"""
LLM categoriser using Claude Haiku.

Accepts a batch of raw transaction descriptions and returns each one
tagged with a spending category. Batches transactions per call to
minimise API round-trips.
"""

from __future__ import annotations

import json
import os

import anthropic

from ingestion.bank.pdf_parser import RawTransaction

# ── Allowed categories ─────────────────────────────────────────────────────────
CATEGORIES = [
    "Groceries",
    "Dining",
    "Transport",
    "Entertainment",
    "Shopping",
    "Utilities",
    "Healthcare",
    "Travel",
    "Education",
    "Insurance",
    "Investment",
    "Transfer",
    "Income",
    "Reimbursement",
    "Other",
]

_SYSTEM_PROMPT = f"""\
You are a personal finance assistant based in Singapore. Your job is to classify \
bank transactions into spending categories.

Rules:
- Choose exactly one category from this list: {json.dumps(CATEGORIES)}
- Return ONLY a JSON array of objects with keys "index" and "category"
- Do not include any explanation or extra text
- "index" is the 0-based position of the transaction in the input list
- Use Singapore context when making your classification (e.g. NETS, PayNow, PayLah, \
  EZ-Link, TransitLink, HDB, CPF, SGX, CDP are all common Singapore payment methods)

Specific merchant rules (override general classification):
- TIMBRE → Dining
- KOUFU, KOPITIAM, FOOD REPUBLIC, FOODFARE → Dining (Singapore food courts)
- FAIRPRICE, GIANT, COLD STORAGE, SHENG SIONG, Prime Supermarket → Groceries
- BUS/MRT, SMRT, SBS TRANSIT, EZ-LINK, TRANSIT → Transport
- GRAB (ride) → Transport; GRAB (food) → Dining; use description context to distinguish
- SINGTEL, STARHUB, M1, GOMO, CIRCLES.LIFE, REDONE → Utilities
- SP DIGITAL, SP GROUP, CITY ENERGY → Utilities
- SHOPEE, LAZADA, QXPRESS, NINJA VAN → Shopping
- KLOOK → Travel
- CLAUDE.AI, CHATGPT, OPENAI, NOTION, SPOTIFY, NETFLIX, YOUTUBE → Entertainment
- Transfers to crypto exchanges (GEMINI, BINANCE, COINBASE, CRYPTO.COM) → Investment
- Transfers to brokers (IBKR, INTERACTIVE BROKERS, TIGER, MOOMOO, SYFE, ENDOWUS) → Investment
- Incoming PayNow from a named person ("PAYNOW OTHR | PERSON NAME | ...") → Reimbursement
- Outgoing PayNow to a named person → categorise by context (Dining if meal split, Transfer if large)
- Merchant refunds via PayNow (company name in description) → same category as original purchase
- Credit card bill payments on a Credit Card statement (e.g. "PAYMT THRU E-BANK", \
  "PAYMT THRU INTERNET", "PAYMT THRU HOMEB", "PAYMENT - THANK YOU", "AUTOPAY") → Transfer
- Any positive (credit) amount on a Credit Card statement that represents a bill payment → Transfer
- Salary / payroll credits (e.g. "SALA", "SALARY", "PAYROLL", "SAL ") → Income
- Government credits (e.g. "GOV", "GST VOUCHER", "CDC VOUCHER", "SKILLSFUTURE", "WORKFARE") → Income
- Bank interest credits → Income
- Cashback / rebate credits (e.g. "CASH REBATE", "CASHBACK", "REBATE") → Income

Example output:
[
  {{"index": 0, "category": "Dining"}},
  {{"index": 1, "category": "Transport"}}
]
"""

_BATCH_SIZE = 40  # transactions per API call


def _build_user_message(transactions: list[RawTransaction]) -> str:
    lines = []
    for i, tx in enumerate(transactions):
        lines.append(f"{i}. Date: {tx.date} | Description: {tx.description} | Amount: {tx.amount}")
    return "Classify these transactions:\n\n" + "\n".join(lines)


def _parse_response(content: str, offset: int) -> dict[int, str]:
    """Parse the JSON array returned by Haiku into {absolute_index: category}."""
    try:
        # Strip markdown code fences if present
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        items = json.loads(content)
        return {offset + item["index"]: item["category"] for item in items}
    except (json.JSONDecodeError, KeyError) as e:
        raise ValueError(f"Failed to parse Haiku response: {e}\nRaw response:\n{content}") from e


def categorise(
    transactions: list[RawTransaction],
    api_key: str | None = None,
    batch_size: int = _BATCH_SIZE,
) -> list[RawTransaction]:
    """
    Categorise a list of transactions using Claude Haiku.

    Returns the same list with a `category` attribute added to each item.
    Uses prompt caching on the system prompt to reduce cost on repeated calls.
    """
    client = anthropic.Anthropic(api_key=api_key or os.environ["ANTHROPIC_API_KEY"])
    category_map: dict[int, str] = {}

    for batch_start in range(0, len(transactions), batch_size):
        batch = transactions[batch_start : batch_start + batch_size]
        user_message = _build_user_message(batch)

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},  # cache system prompt
                }
            ],
            messages=[{"role": "user", "content": user_message}],
        )

        raw_content = response.content[0].text
        category_map.update(_parse_response(raw_content, offset=batch_start))

    # Attach categories back to transaction objects
    for i, tx in enumerate(transactions):
        tx.category = category_map.get(i, "Other")

    return transactions
