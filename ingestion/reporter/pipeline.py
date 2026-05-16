from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv
from google.cloud import bigquery


_SGT = timezone(timedelta(hours=8))

_TABLE_LABELS = {
    "ads_net_worth_dashboard_df": "Net worth",
    "ads_monthly_spend_dashboard_df": "Monthly spend",
    "ads_net_worth_daily_df": "Daily net worth",
}


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _query_source_statuses(client: bigquery.Client, project_id: str, run_date: str) -> list[dict]:
    query = f"""
        SELECT source, status, row_count, message
        FROM `{project_id}.ods.pipeline_run_status`
        WHERE run_date = '{run_date}'
          AND pipeline = 'investment'
        ORDER BY source
    """
    try:
        return [
            {
                "source":    row.source,
                "status":    row.status,
                "row_count": row.row_count,
                "message":   row.message,
            }
            for row in client.query(query).result()
        ]
    except Exception as e:
        print(f"WARNING: Could not query pipeline status — {e}")
        return []


def _query_ads_freshness(client: bigquery.Client, project_id: str) -> list[dict]:
    query = f"""
        SELECT table_id, TIMESTAMP_MILLIS(last_modified_time) AS last_modified
        FROM `{project_id}.ads.__TABLES__`
        WHERE table_id IN UNNEST({list(_TABLE_LABELS.keys())})
    """
    try:
        return [
            {"table": row.table_id, "last_modified": row.last_modified}
            for row in client.query(query).result()
        ]
    except Exception as e:
        print(f"WARNING: Could not query ads freshness — {e}")
        return []


def _icon(status: str) -> str:
    return {"live": "✅", "fallback": "⚠️", "failed": "❌"}.get(status, "❓")


def _build_message(run_date: str, source_statuses: list[dict], ads_tables: list[dict]) -> str:
    date_str = datetime.strptime(run_date, "%Y-%m-%d").strftime("%a %d %b %Y")
    today_start_utc = datetime.now(_SGT).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).astimezone(timezone.utc)

    lines = [f"📊 <b>Clairvoyance Daily Report</b> — {date_str}", ""]

    # Investment sources
    lines.append("<b>Investment Positions</b>")
    brokers = [s for s in source_statuses if s["source"] != "fx"]
    fx_rows = [s for s in source_statuses if s["source"] == "fx"]

    if brokers:
        for s in brokers:
            count = f" — {s['row_count']} positions" if s.get("row_count") is not None else ""
            lines.append(f"{_icon(s['status'])} {s['source'].upper()}{count} ({s['status']})")
    else:
        lines.append("❓ No status recorded — pipeline may not have run")

    if fx_rows:
        fx = fx_rows[0]
        lines.append(f"{_icon(fx['status'])} FX Rates ({fx['status']})")

    lines.append("")

    # dbt + dashboard
    lines.append("<b>dbt Build &amp; Dashboard</b>")
    if ads_tables:
        for t in sorted(ads_tables, key=lambda x: x["table"]):
            lm_utc = _to_utc(t["last_modified"])
            label  = _TABLE_LABELS.get(t["table"], t["table"])
            if lm_utc >= today_start_utc:
                lm_sgt = lm_utc.astimezone(_SGT)
                lines.append(f"✅ {label} — updated {lm_sgt.strftime('%H:%M SGT')}")
            else:
                age_h = int((datetime.now(timezone.utc) - lm_utc).total_seconds() / 3600)
                lines.append(f"❌ {label} — stale ({age_h}h old)")
    else:
        lines.append("❓ Could not check table freshness")

    lines.append("")
    lines.append("──────────────────────")

    # Overall verdict
    has_failed   = any(s["status"] == "failed"   for s in source_statuses)
    has_fallback = any(s["status"] == "fallback"  for s in source_statuses)
    ads_stale    = not ads_tables or any(
        _to_utc(t["last_modified"]) < today_start_utc for t in ads_tables
    )

    if has_failed or ads_stale:
        lines.append("❌ <b>Action needed</b> — check Cloud Run logs")
    elif has_fallback:
        lines.append("⚠️ Some sources used fallback data — live APIs may be down")
    elif not source_statuses:
        lines.append("❓ No pipeline status found — investment job may not have run")
    else:
        lines.append("✅ All clear — dashboard is up to date")

    return "\n".join(lines)


def _send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    resp = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )
    resp.raise_for_status()
    print("Telegram notification sent")


def run(project_id: str, bot_token: str, chat_id: str) -> None:
    client   = bigquery.Client(project=project_id)
    run_date = datetime.now(_SGT).strftime("%Y-%m-%d")

    print(f"Reporter: checking pipeline status for {run_date}...")
    source_statuses = _query_source_statuses(client, project_id, run_date)
    ads_tables = _query_ads_freshness(client, project_id)

    message = _build_message(run_date, source_statuses, ads_tables)
    print(message)
    _send_telegram(bot_token, chat_id, message)


def main() -> None:
    load_dotenv()
    project_id = os.environ.get("GCP_PROJECT_ID", "")
    bot_token  = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    if not project_id:
        raise ValueError("GCP_PROJECT_ID not set")

    run(project_id=project_id, bot_token=bot_token, chat_id=chat_id)


if __name__ == "__main__":
    main()
