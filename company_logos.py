import argparse
import json

from dotenv import load_dotenv

from db import get_connection, init_schema


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and override company logos")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("show", "clear"):
        sub = subparsers.add_parser(name)
        sub.add_argument("--ats", required=True)
        sub.add_argument("--board-token", required=True)

    set_parser = subparsers.add_parser("set")
    set_parser.add_argument("--ats", required=True)
    set_parser.add_argument("--board-token", required=True)
    set_parser.add_argument("--logo-url", required=True)

    return parser


def fetch_company_logo_info(conn, ats: str, board_token: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ats,
                board_token,
                company_name,
                company_slug,
                domain,
                logo_url,
                scraped_logo_url,
                COALESCE(logo_url, scraped_logo_url) AS effective_logo_url
            FROM pipeline_companies
            WHERE ats = %s AND board_token = %s
            """,
            (ats, board_token),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "ats": row[0],
        "board_token": row[1],
        "company_name": row[2],
        "company_slug": row[3],
        "domain": row[4],
        "logo_url": row[5],
        "scraped_logo_url": row[6],
        "effective_logo_url": row[7],
    }


def set_logo_override(conn, ats: str, board_token: str, logo_url: str | None) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_companies
            SET logo_url = %s
            WHERE ats = %s AND board_token = %s
            """,
            (logo_url, ats, board_token),
        )
        updated = cur.rowcount > 0
    conn.commit()
    return updated


def main() -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()

    conn = get_connection()
    try:
        init_schema(conn)

        if args.command == "show":
            payload = fetch_company_logo_info(conn, args.ats, args.board_token)
            if not payload:
                print("Company not found")
                return 1
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0

        if args.command == "set":
            updated = set_logo_override(conn, args.ats, args.board_token, args.logo_url)
            if not updated:
                print("Company not found")
                return 1
            payload = fetch_company_logo_info(conn, args.ats, args.board_token)
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0

        if args.command == "clear":
            updated = set_logo_override(conn, args.ats, args.board_token, None)
            if not updated:
                print("Company not found")
                return 1
            payload = fetch_company_logo_info(conn, args.ats, args.board_token)
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0

        parser.error(f"Unknown command: {args.command}")
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
