import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import psycopg


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://rent_app:rent_app@postgres:5432/rent_app",
)
SCHEMA_PATH = Path(__file__).with_name("schema.sql")
RETURNING_ID_TABLES = {
    "properties",
    "unmatched_payments",
    "tenant_aliases",
    "rent_increases",
    "payments",
}


class Row(dict):
    pass


def _normalize_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        try:
            text = value.isoformat(sep=" ")
        except TypeError:
            text = value.isoformat()
        return text.replace("T", " ")
    return value


def _split_statements(script: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    in_single = False
    in_double = False

    for char in script:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double

        if char == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue

        current.append(char)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def _convert_placeholders(query: str) -> str:
    return re.sub(r"\?", "%s", query)


def _insert_table_name(query: str) -> Optional[str]:
    match = re.search(r"insert\s+into\s+([a-zA-Z_][a-zA-Z0-9_]*)", query, re.IGNORECASE)
    return match.group(1).lower() if match else None


def _should_return_id(query: str) -> bool:
    lowered = query.lower()
    if "returning" in lowered or "on conflict" in lowered:
        return False
    table_name = _insert_table_name(query)
    return table_name in RETURNING_ID_TABLES


def _row_from_record(columns: Sequence[str], record: Sequence[Any]) -> Row:
    return Row({column: _normalize_value(value) for column, value in zip(columns, record)})


class Cursor:
    def __init__(
        self,
        cursor: Optional[psycopg.Cursor[Any]] = None,
        *,
        lastrowid: Optional[int] = None,
    ) -> None:
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchone(self) -> Optional[Row]:
        if self._cursor is None:
            return None
        record = self._cursor.fetchone()
        if record is None:
            return None
        columns = [item.name for item in self._cursor.description]
        return _row_from_record(columns, record)

    def fetchall(self) -> List[Row]:
        if self._cursor is None:
            return []
        columns = [item.name for item in self._cursor.description]
        return [_row_from_record(columns, record) for record in self._cursor.fetchall()]


class Connection:
    def __init__(self, raw_connection: psycopg.Connection[Any]) -> None:
        self._conn = raw_connection

    def execute(self, query: str, params: Iterable[Any] = ()) -> Cursor:
        normalized_query = _convert_placeholders(query)
        cursor = self._conn.cursor()
        if _should_return_id(normalized_query):
            cursor.execute(f"{normalized_query.rstrip()} RETURNING id", tuple(params))
            record = cursor.fetchone()
            return Cursor(lastrowid=int(record[0]) if record else None)

        cursor.execute(normalized_query, tuple(params))
        return Cursor(cursor)

    def fetch_one(self, query: str, params: Iterable[Any] = ()) -> Optional[Row]:
        return self.execute(query, params).fetchone()

    def fetch_all(self, query: str, params: Iterable[Any] = ()) -> List[Row]:
        return self.execute(query, params).fetchall()

    def execute_returning_id(self, query: str, params: Iterable[Any] = ()) -> int:
        cursor = self.execute(query, params)
        if cursor.lastrowid is None:
            raise RuntimeError("Query did not return an id.")
        return int(cursor.lastrowid)

    def executescript(self, script: str) -> None:
        for statement in _split_statements(script):
            self._conn.execute(statement)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.rollback()
        self.close()


def get_connection() -> Connection:
    return Connection(psycopg.connect(DATABASE_URL))


def init_db() -> None:
    with get_connection() as conn:
        bootstrap_schema(conn)


def row_to_dict(row: Row) -> Dict[str, Any]:
    return dict(row)


def ensure_column(conn: Connection, table_name: str, column_name: str, column_type: str) -> None:
    existing = conn.fetch_one(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    if not existing:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def table_exists(conn: Connection, table_name: str) -> bool:
    row = conn.fetch_one(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    return bool(row)


def bootstrap_schema(conn: Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text())
    ensure_column(conn, "users", "whatsapp_number", "TEXT")
    ensure_column(conn, "users", "last_login_at", "TIMESTAMP")
    ensure_column(conn, "users", "whatsapp_onboarding_sent_at", "TIMESTAMP")
    ensure_column(conn, "properties", "owner_username", "TEXT")
    ensure_column(conn, "unmatched_payments", "owner_username", "TEXT")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_whatsapp_number ON users(whatsapp_number) WHERE whatsapp_number IS NOT NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_properties_owner_username ON properties(owner_username)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_unmatched_owner_username ON unmatched_payments(owner_username)")
    conn.commit()
