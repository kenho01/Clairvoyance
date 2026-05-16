from __future__ import annotations

import json
import os
from pathlib import Path

import anthropic

from ingestion.bank.pdf_parser import RawTransaction

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
    "Endowus",
    "Transfer",
    "Other",
]

_PROMPT_BASE_PATH     = Path(__file__).parent / "prompt_base.txt"
_PROMPT_PERSONAL_PATH = Path(__file__).parent / "prompt_personal.txt"


def _load_system_prompt() -> str:
    base = _PROMPT_BASE_PATH.read_text().format(categories=json.dumps(CATEGORIES))

    # Env var takes precedence (GCP Secret Manager → Cloud Run); fall back to local file
    personal = os.environ.get("PERSONAL_PROMPT_RULES", "").strip()
    if not personal and _PROMPT_PERSONAL_PATH.exists():
        personal = _PROMPT_PERSONAL_PATH.read_text().strip()

    if personal:
        base = base.rstrip() + "\n\nPersonal rules (take priority over all rules above):\n" + personal

    return base

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
                    "text": _load_system_prompt(),
                    "cache_control": {"type": "ephemeral"},
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
