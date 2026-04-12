from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pdfplumber

_DATE_RE    = re.compile(r"as at\s+(\d{1,2}\s+\w+\s+\d{4})", re.IGNORECASE)
_BALANCE_RE = re.compile(r"\$([\d,]+\.\d{2})")

_ACCOUNT_PATTERNS = {
    "ordinary_account":  re.compile(r"Ordinary Account", re.IGNORECASE),
    "special_account":   re.compile(r"Special Account",  re.IGNORECASE),
    "medisave_account":  re.compile(r"MediSave Account", re.IGNORECASE),
}


@dataclass
class CpfSnapshot:
    statement_date: date
    ordinary_account: float
    special_account: float
    medisave_account: float

    @property
    def total(self) -> float:
        return round(self.ordinary_account + self.special_account + self.medisave_account, 2)


def parse_pdf(pdf_path: str | Path) -> CpfSnapshot:
    """Parse a CPF account balances PDF and return a CpfSnapshot."""
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[0].extract_text() or ""

    # Extract statement date
    date_match = _DATE_RE.search(text)
    if not date_match:
        raise ValueError(f"Could not find 'as at' date in PDF:\n{text}")
    from datetime import datetime
    statement_date = datetime.strptime(date_match.group(1), "%d %b %Y").date()

    # Extract balances per account type
    balances: dict[str, float] = {}
    for line in text.splitlines():
        for account_key, pattern in _ACCOUNT_PATTERNS.items():
            if pattern.search(line):
                amount_match = _BALANCE_RE.search(line)
                if amount_match:
                    balances[account_key] = float(amount_match.group(1).replace(",", ""))

    missing = [k for k in _ACCOUNT_PATTERNS if k not in balances]
    if missing:
        raise ValueError(f"Could not find balances for: {missing}\nPDF text:\n{text}")

    return CpfSnapshot(
        statement_date=statement_date,
        ordinary_account=balances["ordinary_account"],
        special_account=balances["special_account"],
        medisave_account=balances["medisave_account"],
    )
