#!/usr/bin/env python3
import json
import os
import sqlite3
from pathlib import Path

import psycopg
from psycopg.types.json import Json


ROOT = Path(__file__).resolve().parents[1]
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", ROOT / "data" / "rent_management.db"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://rent_app:rent_app@localhost:5432/rent_app")
TABLES = [
    "properties",
    "unmatched_payments",
    "tenant_aliases",
    "whatsapp_pending_matches",
    "rent_increases",
    "payments",
]
JSON_COLUMNS = {"candidates_json"}
EMPTY_STRING_TO_NULL_COLUMNS = {
    "lease_start",
    "lease_end",
    "last_paid_date",
    "payment_date",
    "date_from",
    "date_till",
    "reviewed_at",
    "created_at",
    "updated_at",
}


def reset_sequence(conn: psycopg.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        SELECT setval(
            pg_get_serial_sequence('{table_name}', 'id'),
            COALESCE((SELECT MAX(id) FROM {table_name}), 1),
            (SELECT COUNT(*) > 0 FROM {table_name})
        )
        """
    )


def main() -> None:
    if not SQLITE_PATH.exists():
        raise SystemExit(f"SQLite database not found: {SQLITE_PATH}")

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg.connect(DATABASE_URL)

    try:
        for table_name in TABLES:
            rows = sqlite_conn.execute(f"SELECT * FROM {table_name}").fetchall()
            if not rows:
                continue

            columns = list(rows[0].keys())
            column_sql = ", ".join(columns)
            placeholder_sql = ", ".join(["%s"] * len(columns))
            pg_conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")

            for row in rows:
                values = []
                for column in columns:
                    value = row[column]
                    if column in EMPTY_STRING_TO_NULL_COLUMNS and value == "":
                        value = None
                    if column in JSON_COLUMNS and isinstance(value, str):
                        value = Json(json.loads(value))
                    values.append(value)
                pg_conn.execute(
                    f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholder_sql})",
                    values,
                )

            if "id" in columns and table_name != "whatsapp_pending_matches":
                reset_sequence(pg_conn, table_name)

        pg_conn.commit()
        print("SQLite data migrated to Postgres.")
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
