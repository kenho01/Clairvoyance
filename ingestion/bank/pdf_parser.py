from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pdfplumber


_AMOUNT_RE = re.compile(r"^\d{1,3}(?:,\d{3})*\.\d{2}$")

_NOISE_FRAGMENTS = [
    "Page ", "United Overseas", "Please note", "please note", "Pleasenote",
    "请注意", "omissions", "claim against", "Co. Reg", "GST Reg",
    "----------", "------------------------",
]


@dataclass
class RawTransaction:
    date: str
    description: str
    transaction_type: str
    amount: float        # negative = debit/spend, positive = credit/refund
    currency: str = "SGD"
    source_file: str = ""
    category: str = ""


def _is_noise(text: str) -> bool:
    return any(f in text for f in _NOISE_FRAGMENTS)


# ── Savings Account Parser ──────────────────────────────────────────────────────

_SAVINGS_DATE_RE   = re.compile(r"^\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$")
_DEPOSIT_KEYWORDS  = ["Inward Credit", "Interest Credit", "Credit"]
_SAVINGS_SKIP_DESC = {"BALANCE B/F", "Total", "Interest Credit"}
_SAVINGS_NOISE     = _NOISE_FRAGMENTS + [
    "Date", "Withdrawals", "Deposits", "Balance SGD",
    "End of", "continued", "Account Transaction Details", "One Account 761",
    "Total ",
]


def _savings_extract_year(pdf) -> str:
    text = pdf.pages[0].extract_text() or ""
    match = re.search(r"Period:.*?(\d{4})", text)
    return match.group(1) if match else "2025"


def _savings_extract_closing_balance(pdf) -> float | None:
    """Extract the closing balance from page 1 account overview."""
    text = pdf.pages[0].extract_text() or ""
    # The One Account line ends with the closing balance amount
    match = re.search(r"One Account\s+SGD[\d\s.,-]+?([\d,]+\.\d{2})\s*$", text, re.MULTILINE)
    if match:
        return float(match.group(1).replace(",", ""))
    # Fallback: look for Grand Total line
    match = re.search(r"Grand Total.*?([\d,]+\.\d{2})", text)
    return float(match.group(1).replace(",", "")) if match else None


def _savings_find_col_centers(page) -> dict[str, float] | None:
    words = page.extract_words(x_tolerance=3, y_tolerance=3)
    for i, w in enumerate(words):
        if w["text"] == "Withdrawals":
            w_center = (w["x0"] + w["x1"]) / 2
            rest = words[i + 1:]
            dep = next((r for r in rest if r["text"] == "Deposits"), None)
            bal = next((r for r in rest if r["text"] == "Balance"), None)
            if dep and bal:
                return {
                    "withdrawal": w_center,
                    "deposit":    (dep["x0"] + dep["x1"]) / 2,
                    "balance":    (bal["x0"] + bal["x1"]) / 2,
                }
    return None


def _savings_assign_col(x_center: float, col_centers: dict[str, float]) -> str:
    return min(col_centers, key=lambda c: abs(x_center - col_centers[c]))


def _parse_savings_page(page, year: str, col_centers: dict[str, float]) -> list[RawTransaction]:
    words = page.extract_words(keep_blank_chars=False, x_tolerance=3, y_tolerance=3)
    rows: dict[float, list[dict]] = {}
    for w in words:
        rows.setdefault(round(w["top"], 1), []).append(w)

    transactions: list[RawTransaction] = []
    current_date = current_type = None
    desc_lines: list[str] = []
    tx_amount: float | None = None
    is_deposit = False

    def flush():
        nonlocal current_date, current_type, desc_lines, tx_amount, is_deposit
        if not current_date or tx_amount is None:
            current_date = None; desc_lines = []; tx_amount = None
            return
        clean = [
            l for l in desc_lines
            if not any(f in l for f in _SAVINGS_NOISE)
            and not re.match(r"^(xxxxxx\d*|PIB\d+|\d{7,}|OTHR|SGD)$", l.strip())
            and len(l.strip()) > 1
        ]
        description = " | ".join(clean).strip()
        if not description or description in _SAVINGS_SKIP_DESC:
            current_date = None; desc_lines = []; tx_amount = None; return
        amount = tx_amount if is_deposit else -tx_amount
        transactions.append(RawTransaction(
            date=f"{current_date} {year}",
            description=description,
            transaction_type=current_type or "",
            amount=round(amount, 2),
        ))
        current_date = current_type = None
        desc_lines = []; tx_amount = None; is_deposit = False

    for _y, row_words in sorted(rows.items()):
        row_words = sorted(row_words, key=lambda w: w["x0"])
        line_text = " ".join(w["text"] for w in row_words).strip()
        if any(f in line_text for f in _SAVINGS_NOISE):
            continue

        w0 = row_words[0]["text"] if row_words else ""
        w1 = row_words[1]["text"] if len(row_words) > 1 else ""
        potential_date = f"{w0} {w1}"

        if _SAVINGS_DATE_RE.match(potential_date):
            flush()
            current_date = potential_date
            desc_parts = []
            for w in row_words[2:]:
                if _AMOUNT_RE.match(w["text"]):
                    val = float(w["text"].replace(",", ""))
                    col = _savings_assign_col((w["x0"] + w["x1"]) / 2, col_centers)
                    if col == "withdrawal" and tx_amount is None:
                        tx_amount = val; is_deposit = False
                    elif col == "deposit" and tx_amount is None:
                        tx_amount = val; is_deposit = True
                else:
                    desc_parts.append(w["text"])
            current_type = " ".join(desc_parts).strip()
            if current_type:
                desc_lines = [current_type]
                if tx_amount is not None:
                    is_deposit = any(k in current_type for k in _DEPOSIT_KEYWORDS)

        elif current_date:
            if any(f in line_text for f in _SAVINGS_NOISE):
                continue
            for w in row_words:
                if _AMOUNT_RE.match(w["text"]):
                    val = float(w["text"].replace(",", ""))
                    col = _savings_assign_col((w["x0"] + w["x1"]) / 2, col_centers)
                    if col == "withdrawal" and tx_amount is None:
                        tx_amount = val; is_deposit = False
                    elif col == "deposit" and tx_amount is None:
                        tx_amount = val; is_deposit = True
            if not re.match(r"^(xxxxxx\d*|PIB\d+|\d{7,}|OTHR|SGD)$", line_text.strip()):
                desc_lines.append(line_text)

    flush()
    return transactions


def _parse_savings(pdf) -> list[RawTransaction]:
    year = _savings_extract_year(pdf)
    transactions = []
    for page in pdf.pages:
        col_centers = _savings_find_col_centers(page)
        if col_centers is None:
            continue
        transactions.extend(_parse_savings_page(page, year, col_centers))
    return transactions


# ── Credit Card Parser ──────────────────────────────────────────────────────────

_CC_DATE_RE   = re.compile(r"^\d{1,2}\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$")
_CC_SKIP_DESC = {"PREVIOUS BALANCE", "SUB TOTAL", "TOTAL BALANCE FOR UOB ONE CARD"}
_CC_NOISE     = _NOISE_FRAGMENTS + [
    "Post", "Trans", "Date", "Description of Transaction", "Transaction Amount",
    "UOB ONE CARD", "continued", "End of Transaction",
    "SUB TOTAL", "TOTAL BALANCE",
]


def _cc_extract_year(pdf) -> str:
    text = pdf.pages[0].extract_text() or ""
    match = re.search(r"Statement Date\s+\d{1,2}\s+\w+\s+(\d{4})", text)
    return match.group(1) if match else "2026"


def _cc_has_transaction_table(page) -> bool:
    text = page.extract_text() or ""
    return "Description of Transaction" in text or "Transaction Amount" in text


def _parse_cc_page(page, year: str) -> list[RawTransaction]:
    words = page.extract_words(keep_blank_chars=False, x_tolerance=3, y_tolerance=3)
    rows: dict[float, list[dict]] = {}
    for w in words:
        rows.setdefault(round(w["top"], 1), []).append(w)

    transactions: list[RawTransaction] = []
    current_date: str | None = None
    desc_lines: list[str] = []
    tx_amount: float | None = None
    is_credit = False

    def flush():
        nonlocal current_date, desc_lines, tx_amount, is_credit
        if not current_date or tx_amount is None:
            current_date = None; desc_lines = []; tx_amount = None
            return
        clean = [
            l for l in desc_lines
            if not _is_noise(l)
            and not re.match(r"^Ref No\.", l.strip())
            and len(l.strip()) > 1
        ]
        description = " | ".join(clean).strip()
        if not description or description in _CC_SKIP_DESC:
            current_date = None; desc_lines = []; tx_amount = None; return
        amount = tx_amount if is_credit else -tx_amount
        transactions.append(RawTransaction(
            date=f"{current_date} {year}",
            description=description,
            transaction_type="Credit Card",
            amount=round(amount, 2),
        ))
        current_date = None
        desc_lines = []; tx_amount = None; is_credit = False

    for _y, row_words in sorted(rows.items()):
        row_words = sorted(row_words, key=lambda w: w["x0"])
        line_text = " ".join(w["text"] for w in row_words).strip()

        if _is_noise(line_text) or any(f in line_text for f in _CC_NOISE):
            continue

        # Credit card rows: "07 FEB  07 FEB  Description  Amount"
        # First two tokens are Post Date, next two are Trans Date (same format)
        w0 = row_words[0]["text"] if row_words else ""
        w1 = row_words[1]["text"] if len(row_words) > 1 else ""
        potential_date = f"{w0} {w1}"

        if _CC_DATE_RE.match(potential_date):
            flush()
            # Skip the Trans Date (next two words also match date pattern)
            rest = row_words[2:]
            if len(rest) >= 2:
                td0, td1 = rest[0]["text"], rest[1]["text"]
                if _CC_DATE_RE.match(f"{td0} {td1}"):
                    rest = rest[2:]  # skip trans date

            current_date = potential_date
            desc_parts = []
            for w in rest:
                text_w = w["text"]
                if text_w.endswith("CR") and _AMOUNT_RE.match(text_w[:-2].strip()):
                    tx_amount = float(text_w[:-2].strip().replace(",", ""))
                    is_credit = True
                elif _AMOUNT_RE.match(text_w):
                    tx_amount = float(text_w.replace(",", ""))
                    is_credit = False
                else:
                    desc_parts.append(text_w)

            desc = " ".join(desc_parts).strip()
            if desc and desc not in _CC_SKIP_DESC:
                desc_lines = [desc]

        elif current_date:
            if _is_noise(line_text) or any(f in line_text for f in _CC_NOISE):
                continue
            # Check for amount on continuation line (e.g. "CR" amounts split across lines)
            for w in row_words:
                text_w = w["text"]
                if text_w == "CR" and tx_amount is not None:
                    is_credit = True
                elif text_w.endswith("CR") and _AMOUNT_RE.match(text_w[:-2].strip()):
                    tx_amount = float(text_w[:-2].strip().replace(",", ""))
                    is_credit = True
                elif _AMOUNT_RE.match(text_w) and tx_amount is None:
                    tx_amount = float(text_w.replace(",", ""))
                    is_credit = False
            # Add non-reference, non-amount-only lines to description
            if (not re.match(r"^Ref No\.", line_text.strip())
                    and not _AMOUNT_RE.match(line_text.strip())):
                desc_lines.append(line_text)

    flush()
    return transactions


def _parse_credit_card(pdf) -> list[RawTransaction]:
    year = _cc_extract_year(pdf)
    transactions = []
    for page in pdf.pages:
        if not _cc_has_transaction_table(page):
            continue
        transactions.extend(_parse_cc_page(page, year))
    return transactions


# ── Entry Point ─────────────────────────────────────────────────────────────────

def _detect_statement_type(pdf) -> str:
    text = pdf.pages[0].extract_text() or ""
    # "Description of Transaction" and "Transaction Amount" are CC-specific column headers
    # "Statement of Account" is savings-specific; savings PDFs also contain "Credit Card
    # Eligible Spend" which would falsely match a naive "Credit Card" check
    if "Description of Transaction" in text or "Transaction Amount" in text:
        return "credit_card"
    return "savings"


def parse_pdf(pdf_path: str | Path) -> tuple[list[RawTransaction], str, float | None]:
    """Parse a UOB bank statement PDF. Auto-detects savings vs credit card.

    Returns (transactions, statement_type, closing_balance) where:
      - statement_type is 'credit_card' or 'savings'
      - closing_balance is the account balance for savings statements (None for credit card)
    """
    pdf_path = Path(pdf_path)
    with pdfplumber.open(pdf_path) as pdf:
        stmt_type = _detect_statement_type(pdf)
        if stmt_type == "credit_card":
            transactions = _parse_credit_card(pdf)
            closing_balance = None
        else:
            transactions = _parse_savings(pdf)
            closing_balance = _savings_extract_closing_balance(pdf)

    for tx in transactions:
        tx.source_file = pdf_path.name

    return transactions, stmt_type, closing_balance


def transactions_to_dataframe(transactions: list[RawTransaction]) -> pd.DataFrame:
    return pd.DataFrame([t.__dict__ for t in transactions])


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "assets/eStatement.pdf"
    txs, stmt_type, closing_balance = parse_pdf(path)
    if closing_balance is not None:
        print(f"Closing balance: SGD {closing_balance:,.2f}")
    df = transactions_to_dataframe(txs)
    pd.set_option("display.max_colwidth", 55)
    pd.set_option("display.width", 200)
    print(df[["date", "description", "transaction_type", "amount"]].to_string(index=False))
    print(f"\nTotal transactions: {len(txs)}")
    print(f"Total debits:   SGD {abs(df[df['amount'] < 0]['amount'].sum()):.2f}")
    print(f"Total credits:  SGD {df[df['amount'] > 0]['amount'].sum():.2f}")
