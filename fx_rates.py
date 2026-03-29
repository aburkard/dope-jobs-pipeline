from __future__ import annotations

import argparse
from urllib.request import urlopen

from dotenv import load_dotenv

from db import get_connection, init_schema, upsert_fx_rates, get_latest_fx_rates
from salary_normalization import ECB_DAILY_XML_URL, compute_usd_per_unit_rates, parse_ecb_daily_xml


def fetch_ecb_rates_xml(url: str = ECB_DAILY_XML_URL) -> str:
    with urlopen(url, timeout=30) as response:
        return response.read().decode("utf-8")


def refresh_fx_rates(conn, url: str = ECB_DAILY_XML_URL, source: str = "ECB euro reference rates") -> tuple[str, int]:
    xml_text = fetch_ecb_rates_xml(url)
    as_of_date, eur_quotes = parse_ecb_daily_xml(xml_text)
    usd_rates = compute_usd_per_unit_rates(eur_quotes)
    rows = [
        (currency_code, usd_per_unit, as_of_date, source)
        for currency_code, usd_per_unit in sorted(usd_rates.items())
    ]
    upsert_fx_rates(conn, rows)
    return as_of_date.isoformat(), len(rows)


def show_latest_rates(conn, limit: int = 10) -> tuple[str | None, list[tuple[str, float]]]:
    rates, as_of_date = get_latest_fx_rates(conn)
    top_rows = sorted(rates.items())[:limit]
    return as_of_date.isoformat() if as_of_date else None, top_rows


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch and store FX rates")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh_parser = subparsers.add_parser("refresh", help="Fetch latest ECB rates and store them")
    refresh_parser.add_argument("--url", default=ECB_DAILY_XML_URL, help="ECB XML feed URL")

    show_parser = subparsers.add_parser("show", help="Show the latest stored FX rates")
    show_parser.add_argument("--limit", type=int, default=10, help="Rows to print")

    args = parser.parse_args()

    conn = get_connection()
    init_schema(conn)
    try:
        if args.command == "refresh":
            as_of_date, count = refresh_fx_rates(conn, url=args.url)
            print(f"fx_rates_refreshed as_of={as_of_date} count={count}")
        else:
            as_of_date, rows = show_latest_rates(conn, limit=args.limit)
            print(f"fx_rates_latest as_of={as_of_date}")
            for currency_code, usd_per_unit in rows:
                print(f"{currency_code} {usd_per_unit:.6f}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
