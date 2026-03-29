import json
import os
import re
import sqlite3
from html import escape
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Form, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "rent_management.db"


app = FastAPI(title="Rent Management MVP")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PaymentRequest(BaseModel):
    message: str


class RentIncreaseRequest(BaseModel):
    date_from: str
    date_till: str
    rent_amount: float


class PropertyCreateRequest(BaseModel):
    tenant_name: str
    property_name: str
    rent_amount: float
    rent_due_day: Optional[int] = None
    lease_start: Optional[str] = None
    lease_end: Optional[str] = None
    rent_increases: List[RentIncreaseRequest] = Field(default_factory=list)


class ManualMatchRequest(BaseModel):
    property_id: int
    amount: float
    date: str
    unmatched_payment_id: Optional[int] = None
    sender_key: Optional[str] = None


class WhatsAppInboundPayload(BaseModel):
    body: str
    wa_id: Optional[str] = None
    profile_name: Optional[str] = None


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_connection()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS unmatched_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_message TEXT NOT NULL,
                extracted_tenant_name TEXT,
                sender_key TEXT,
                amount REAL NOT NULL,
                payment_date TEXT NOT NULL,
                candidates_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'UNMATCHED',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS tenant_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER NOT NULL,
                sender_key TEXT NOT NULL UNIQUE,
                sender_name TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(property_id) REFERENCES properties(id)
            );

            CREATE TABLE IF NOT EXISTS whatsapp_pending_matches (
                wa_id TEXT PRIMARY KEY,
                original_message TEXT NOT NULL,
                amount REAL NOT NULL,
                payment_date TEXT NOT NULL,
                sender_key TEXT,
                sender_name TEXT,
                unmatched_payment_id INTEGER,
                candidates_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS rent_increases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER NOT NULL,
                date_from TEXT NOT NULL,
                date_till TEXT NOT NULL,
                rent_amount REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(property_id) REFERENCES properties(id)
            );
            """
        )
        migrate_properties_table(conn)
        ensure_column(conn, "unmatched_payments", "sender_key", "TEXT")
        ensure_column(conn, "properties", "current_month_paid_amount", "REAL")
        ensure_column(conn, "properties", "property_name", "TEXT")
        conn.execute("UPDATE properties SET property_name = tenant_name WHERE property_name IS NULL OR property_name = ''")
        conn.commit()


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def migrate_properties_table(conn: sqlite3.Connection) -> None:
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'properties'"
    ).fetchone()
    if not table_exists:
        conn.execute(
            """
            CREATE TABLE properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_name TEXT NOT NULL,
                property_name TEXT,
                rent_amount REAL NOT NULL,
                rent_due_day INTEGER,
                lease_start TEXT,
                lease_end TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                last_paid_date TEXT,
                last_payment_amount REAL,
                current_month_paid_amount REAL
            )
            """
        )
        return

    columns = {
        row["name"]: dict(row) for row in conn.execute("PRAGMA table_info(properties)").fetchall()
    }
    needs_migration = not columns or any(
        columns.get(name, {}).get("notnull") == 1
        for name in ("rent_due_day", "lease_start", "lease_end")
    )
    if not needs_migration:
        return

    conn.executescript(
        """
        ALTER TABLE properties RENAME TO properties_legacy;

        CREATE TABLE properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_name TEXT NOT NULL,
            property_name TEXT,
            rent_amount REAL NOT NULL,
            rent_due_day INTEGER,
            lease_start TEXT,
            lease_end TEXT,
            status TEXT NOT NULL DEFAULT 'PENDING',
            last_paid_date TEXT,
            last_payment_amount REAL,
            current_month_paid_amount REAL
        );

        INSERT INTO properties (
            id,
            tenant_name,
            property_name,
            rent_amount,
            rent_due_day,
            lease_start,
            lease_end,
            status,
            last_paid_date,
            last_payment_amount,
            current_month_paid_amount
        )
        SELECT
            id,
            tenant_name,
            tenant_name,
            rent_amount,
            NULLIF(rent_due_day, 0),
            NULLIF(lease_start, ''),
            NULLIF(lease_end, ''),
            status,
            last_paid_date,
            last_payment_amount,
            NULL
        FROM properties_legacy;

        DROP TABLE properties_legacy;
        """
    )


def parse_amount(message: str) -> float:
    match = re.search(r"Rs\.?\s?([\d,]+\.\d+)", message)
    if not match:
        raise ValueError("Could not extract amount from payment message.")
    return float(match.group(1).replace(",", ""))


def parse_payment_date(message: str) -> str:
    match = re.search(r"(\d{1,2}-[A-Za-z]{3}-\d{2})", message)
    if not match:
        raise ValueError("Could not extract payment date from payment message.")
    parsed = datetime.strptime(match.group(1), "%d-%b-%y")
    return parsed.strftime("%Y-%m-%d")


def parse_sender_details(message: str) -> Dict[str, Optional[str]]:
    match = re.search(r"NEFT-([A-Z0-9]+)-([A-Z][A-Z\s]*)", message.upper())
    if not match:
        return {"sender_key": None, "sender_name": None}
    return {
        "sender_key": match.group(1).strip(),
        "sender_name": normalize_name(match.group(2)),
    }


def calculate_status(row: sqlite3.Row, today: Optional[date] = None) -> str:
    today = today or date.today()
    paid_amount = paid_amount_for_row(row, today)
    rent_amount = rent_amount_for_period(row, today)
    if paid_amount > rent_amount > 0:
        return "SURPLUS"
    if paid_amount == rent_amount and rent_amount > 0:
        return "PAID"
    if paid_amount > 0:
        return "PARTIALLY_PAID"
    rent_due_day = row["rent_due_day"]
    if rent_due_day is None:
        return "PENDING"
    if today.day > int(rent_due_day):
        return "LATE"
    return "PENDING"


def refresh_all_statuses() -> None:
    with closing(get_connection()) as conn:
        rows = conn.execute("SELECT * FROM properties").fetchall()
        for row in rows:
            status = calculate_status(row)
            conn.execute("UPDATE properties SET status = ? WHERE id = ?", (status, row["id"]))
        conn.commit()


def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return dict(row)


def paid_amount_for_row(row: sqlite3.Row, reference_date: Optional[date] = None) -> float:
    reference_date = reference_date or date.today()
    if not row["last_paid_date"]:
        return 0.0
    last_paid_dt = datetime.strptime(row["last_paid_date"], "%Y-%m-%d").date()
    if last_paid_dt.year != reference_date.year or last_paid_dt.month != reference_date.month:
        return 0.0
    if row["current_month_paid_amount"] is not None:
        return float(row["current_month_paid_amount"])
    return float(row["last_payment_amount"] or 0)


def get_rent_increases_for_property(
    property_id: int, conn: Optional[sqlite3.Connection] = None
) -> List[Dict[str, Any]]:
    should_close = conn is None
    local_conn = conn or get_connection()
    try:
        rows = local_conn.execute(
            "SELECT id, property_id, date_from, date_till, rent_amount FROM rent_increases WHERE property_id = ? ORDER BY date_from ASC, id ASC",
            (property_id,),
        ).fetchall()
        return [row_to_dict(row) for row in rows]
    finally:
        if should_close:
            local_conn.close()


def rent_amount_for_period(
    row: sqlite3.Row,
    reference_date: Optional[date] = None,
    rent_increases: Optional[List[Dict[str, Any]]] = None,
) -> float:
    reference_date = reference_date or date.today()
    increases = rent_increases if rent_increases is not None else get_rent_increases_for_property(int(row["id"]))
    for increase in increases:
        start_dt = datetime.strptime(increase["date_from"], "%Y-%m-%d").date()
        end_dt = datetime.strptime(increase["date_till"], "%Y-%m-%d").date()
        if start_dt <= reference_date <= end_dt:
            return round(float(increase["rent_amount"]), 2)
    return round(float(row["rent_amount"] or 0), 2)


def balance_amount_for_row(row: sqlite3.Row, reference_date: Optional[date] = None) -> float:
    reference_date = reference_date or date.today()
    expected_rent = rent_amount_for_period(row, reference_date)
    return round(max(expected_rent - paid_amount_for_row(row, reference_date), 0), 2)


def surplus_amount_for_row(row: sqlite3.Row, reference_date: Optional[date] = None) -> float:
    reference_date = reference_date or date.today()
    expected_rent = rent_amount_for_period(row, reference_date)
    return round(max(paid_amount_for_row(row, reference_date) - expected_rent, 0), 2)


def enrich_property(row: sqlite3.Row) -> Dict[str, Any]:
    rent_increases = get_rent_increases_for_property(int(row["id"]))
    paid_amount = round(paid_amount_for_row(row), 2)
    current_rent_amount = rent_amount_for_period(row, rent_increases=rent_increases)
    return {
        **row_to_dict(row),
        "status": calculate_status(row),
        "balance_amount": balance_amount_for_row(row),
        "surplus_amount": surplus_amount_for_row(row),
        "current_month_paid_amount": paid_amount,
        "current_rent_amount": current_rent_amount,
        "rent_increases": rent_increases,
    }


def candidate_properties(extracted_name: str) -> List[Dict[str, Any]]:
    normalized_target = normalize_name(extracted_name)
    with closing(get_connection()) as conn:
        properties = conn.execute("SELECT * FROM properties").fetchall()

    matches = []
    for prop in properties:
        tenant_name = normalize_name(prop["tenant_name"])
        if normalized_target in tenant_name or tenant_name in normalized_target:
            candidate = row_to_dict(prop)
            candidate["status"] = calculate_status(prop)
            candidate["balance_amount"] = balance_amount_for_row(prop)
            candidate["surplus_amount"] = surplus_amount_for_row(prop)
            candidate["current_month_paid_amount"] = round(paid_amount_for_row(prop), 2)
            candidate["current_rent_amount"] = rent_amount_for_period(prop)
            candidate["rent_increases"] = get_rent_increases_for_property(int(prop["id"]))
            matches.append(candidate)
    return matches


def all_properties() -> List[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        rows = conn.execute("SELECT * FROM properties ORDER BY id DESC").fetchall()
    return [enrich_property(row) for row in rows]


def find_property_by_sender_key(sender_key: str) -> Optional[Dict[str, Any]]:
    if not sender_key:
        return None
    with closing(get_connection()) as conn:
        row = conn.execute(
            """
            SELECT p.*
            FROM tenant_aliases ta
            JOIN properties p ON p.id = ta.property_id
            WHERE ta.sender_key = ?
            """,
            (sender_key,),
        ).fetchone()
    if not row:
        return None
    return enrich_property(row)


def calculate_payment_totals(row: sqlite3.Row, payment_amount: float, payment_date: str) -> Dict[str, float]:
    payment_dt = datetime.strptime(payment_date, "%Y-%m-%d").date()
    existing_total = paid_amount_for_row(row, payment_dt)

    total_paid = round(existing_total + payment_amount, 2)
    rent_amount = rent_amount_for_period(row, payment_dt)
    balance_amount = round(max(rent_amount - total_paid, 0), 2)
    surplus_amount = round(max(total_paid - rent_amount, 0), 2)
    return {
        "total_paid": total_paid,
        "rent_amount": rent_amount,
        "balance_amount": balance_amount,
        "surplus_amount": surplus_amount,
    }


def normalize_rent_increases(
    rent_increases: List[RentIncreaseRequest],
) -> List[Dict[str, Any]]:
    normalized = []
    parsed_ranges = []

    for item in rent_increases:
        try:
            date_from = datetime.strptime(item.date_from, "%Y-%m-%d").date()
            date_till = datetime.strptime(item.date_till, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="Rent increase dates must be YYYY-MM-DD."
            ) from exc

        if date_till < date_from:
            raise HTTPException(
                status_code=400, detail="Rent increase end date must be on or after start date."
            )

        parsed_ranges.append((date_from, date_till))
        normalized.append(
            {
                "date_from": date_from.strftime("%Y-%m-%d"),
                "date_till": date_till.strftime("%Y-%m-%d"),
                "rent_amount": round(float(item.rent_amount), 2),
            }
        )

    ordered_ranges = sorted(parsed_ranges)
    for index in range(1, len(ordered_ranges)):
        previous_end = ordered_ranges[index - 1][1]
        current_start = ordered_ranges[index][0]
        if current_start <= previous_end:
            raise HTTPException(
                status_code=400,
                detail="Rent increase date ranges cannot overlap.",
            )

    return sorted(normalized, key=lambda item: (item["date_from"], item["date_till"]))


def save_rent_increases(
    conn: sqlite3.Connection, property_id: int, rent_increases: List[Dict[str, Any]]
) -> None:
    conn.execute("DELETE FROM rent_increases WHERE property_id = ?", (property_id,))
    for item in rent_increases:
        conn.execute(
            """
            INSERT INTO rent_increases (property_id, date_from, date_till, rent_amount)
            VALUES (?, ?, ?, ?)
            """,
            (property_id, item["date_from"], item["date_till"], item["rent_amount"]),
        )


def apply_payment_to_property(property_id: int, payment_amount: float, payment_date: str) -> Dict[str, Any]:
    with closing(get_connection()) as conn:
        property_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
        if not property_row:
            raise HTTPException(status_code=404, detail="Property not found.")

        totals = calculate_payment_totals(property_row, payment_amount, payment_date)
        status = "SURPLUS" if totals["surplus_amount"] > 0 else "PAID" if totals["balance_amount"] == 0 else "PARTIALLY_PAID"

        conn.execute(
            """
            UPDATE properties
            SET status = ?, last_paid_date = ?, last_payment_amount = ?, current_month_paid_amount = ?
            WHERE id = ?
            """,
            (status, payment_date, payment_amount, totals["total_paid"], property_id),
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()

    enriched = enrich_property(updated_row)
    enriched["last_payment_amount"] = round(float(payment_amount), 2)
    return enriched


def mark_unmatched_payment_as_matched(unmatched_payment_id: Optional[int], amount: float, payment_date: str) -> None:
    with closing(get_connection()) as conn:
        if unmatched_payment_id is not None:
            conn.execute(
                "UPDATE unmatched_payments SET status = 'MATCHED' WHERE id = ?",
                (unmatched_payment_id,),
            )
        else:
            conn.execute(
                """
                UPDATE unmatched_payments
                SET status = 'MATCHED'
                WHERE amount = ? AND payment_date = ? AND status = 'UNMATCHED'
                """,
                (amount, payment_date),
            )
        conn.commit()


def record_sender_alias(property_id: int, sender_key: Optional[str], sender_name: Optional[str]) -> None:
    if not sender_key:
        return
    with closing(get_connection()) as conn:
        conn.execute(
            """
            INSERT INTO tenant_aliases (property_id, sender_key, sender_name)
            VALUES (?, ?, ?)
            ON CONFLICT(sender_key) DO UPDATE SET
                property_id = excluded.property_id,
                sender_name = excluded.sender_name
            """,
            (property_id, sender_key, sender_name),
        )
        conn.commit()


def save_unmatched_payment(
    message: str,
    extracted_name: str,
    sender_key: Optional[str],
    amount: float,
    payment_date: str,
    candidates: List[Dict[str, Any]],
) -> int:
    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO unmatched_payments (
                raw_message, extracted_tenant_name, sender_key, amount, payment_date, candidates_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (message, extracted_name, sender_key, amount, payment_date, json.dumps(candidates)),
        )
        conn.commit()
        return int(cursor.lastrowid)


def save_whatsapp_pending_match(
    wa_id: str,
    original_message: str,
    amount: float,
    payment_date: str,
    sender_key: Optional[str],
    sender_name: Optional[str],
    unmatched_payment_id: Optional[int],
    candidates: List[Dict[str, Any]],
) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            """
            INSERT INTO whatsapp_pending_matches (
                wa_id, original_message, amount, payment_date, sender_key, sender_name, unmatched_payment_id, candidates_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wa_id) DO UPDATE SET
                original_message = excluded.original_message,
                amount = excluded.amount,
                payment_date = excluded.payment_date,
                sender_key = excluded.sender_key,
                sender_name = excluded.sender_name,
                unmatched_payment_id = excluded.unmatched_payment_id,
                candidates_json = excluded.candidates_json,
                created_at = CURRENT_TIMESTAMP
            """,
            (
                wa_id,
                original_message,
                amount,
                payment_date,
                sender_key,
                sender_name,
                unmatched_payment_id,
                json.dumps(candidates),
            ),
        )
        conn.commit()


def get_whatsapp_pending_match(wa_id: str) -> Optional[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT * FROM whatsapp_pending_matches WHERE wa_id = ?",
            (wa_id,),
        ).fetchone()
    if not row:
        return None

    item = row_to_dict(row)
    item["candidates"] = json.loads(item.pop("candidates_json"))
    return item


def clear_whatsapp_pending_match(wa_id: str) -> None:
    with closing(get_connection()) as conn:
        conn.execute("DELETE FROM whatsapp_pending_matches WHERE wa_id = ?", (wa_id,))
        conn.commit()


def process_payment_message(message: str) -> Dict[str, Any]:
    try:
        amount = parse_amount(message)
        payment_date = parse_payment_date(message)
        sender_details = parse_sender_details(message)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    matched_by_alias = find_property_by_sender_key(sender_details["sender_key"])
    if matched_by_alias:
        matched_by_alias = apply_payment_to_property(matched_by_alias["id"], amount, payment_date)
        return {
            "status": matched_by_alias["status"],
            "match_source": "saved_sender",
            "matched_property": matched_by_alias,
            "sender_key": sender_details["sender_key"],
            "extracted_tenant_name": sender_details["sender_name"],
            "balance_amount": matched_by_alias["balance_amount"],
            "current_month_paid_amount": matched_by_alias["current_month_paid_amount"],
            "surplus_amount": matched_by_alias["surplus_amount"],
        }

    candidates = candidate_properties(sender_details["sender_name"] or "")
    fallback_candidates = candidates or all_properties()

    unmatched_id = save_unmatched_payment(
        message,
        sender_details["sender_name"] or "",
        sender_details["sender_key"],
        amount,
        payment_date,
        fallback_candidates,
    )
    return {
        "status": "UNMATCHED",
        "unmatched_payment_id": unmatched_id,
        "candidates": fallback_candidates,
        "extracted_tenant_name": sender_details["sender_name"],
        "sender_key": sender_details["sender_key"],
        "amount": amount,
        "date": payment_date,
        "matching_hint": "saved sender not found" if sender_details["sender_key"] else "sender could not be identified",
    }


def manual_match_payment(
    property_id: int,
    amount: float,
    normalized_date: str,
    unmatched_payment_id: Optional[int] = None,
    sender_key: Optional[str] = None,
) -> Dict[str, Any]:
    enriched = apply_payment_to_property(property_id, amount, normalized_date)
    mark_unmatched_payment_as_matched(unmatched_payment_id, amount, normalized_date)
    record_sender_alias(property_id, sender_key, None)

    return {
        "status": enriched["status"],
        "match_source": "manual",
        "matched_property": enriched,
        "sender_key": sender_key,
        "balance_amount": enriched["balance_amount"],
        "current_month_paid_amount": enriched["current_month_paid_amount"],
        "surplus_amount": enriched["surplus_amount"],
    }


def build_twiml_message(message: str) -> str:
    escaped = escape(message)
    return f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{escaped}</Message></Response>'


def summarize_match(result: Dict[str, Any]) -> str:
    matched_property = result["matched_property"]
    parts = [
        f"Matched to {matched_property['tenant_name']}.",
        f"Status: {result['status']}.",
        f"Paid this month: Rs. {result['current_month_paid_amount']:.2f}.",
    ]
    balance_amount = float(result.get("balance_amount") or 0)
    surplus_amount = float(result.get("surplus_amount") or 0)
    if surplus_amount > 0:
        parts.append(f"Surplus: Rs. {surplus_amount:.2f}.")
    else:
        parts.append(f"Balance: Rs. {balance_amount:.2f}.")
    return " ".join(parts)


def build_candidate_prompt(candidates: List[Dict[str, Any]], extracted_name: Optional[str] = None) -> str:
    lines = ["I could not auto-match this payment."]
    if extracted_name:
        lines.append(f"Detected name: {extracted_name}.")
    lines.append("Reply with the tenant number to match:")
    for index, candidate in enumerate(candidates[:9], start=1):
        lines.append(
            f"{index}. {candidate['tenant_name']} ({candidate.get('property_name') or 'Property not set'}) - Rent Rs. {float(candidate.get('current_rent_amount') or candidate['rent_amount']):.2f}"
        )
    lines.append("Reply CANCEL to leave it unmatched.")
    return "\n".join(lines)


def handle_whatsapp_selection(body: str, wa_id: str) -> Optional[str]:
    pending = get_whatsapp_pending_match(wa_id)
    if not pending:
        return None

    normalized = body.strip().upper()
    if normalized == "CANCEL":
        clear_whatsapp_pending_match(wa_id)
        return "Kept this payment unmatched. You can still resolve it from the dashboard later."

    if not re.fullmatch(r"\d+", body.strip()):
        return "Reply with a tenant number from the list, or reply CANCEL."

    selected_index = int(body.strip()) - 1
    candidates = pending["candidates"][:9]
    if selected_index < 0 or selected_index >= len(candidates):
        return "That option is not in the list. Reply with a valid tenant number, or reply CANCEL."

    candidate = candidates[selected_index]
    result = manual_match_payment(
        property_id=int(candidate["id"]),
        amount=float(pending["amount"]),
        normalized_date=pending["payment_date"],
        unmatched_payment_id=pending.get("unmatched_payment_id"),
        sender_key=pending.get("sender_key"),
    )
    clear_whatsapp_pending_match(wa_id)
    return summarize_match(result)


def process_whatsapp_message(payload: WhatsAppInboundPayload) -> str:
    body = payload.body.strip()
    if not body:
        return "Send the bank credit message text exactly as received, or reply with a tenant number if I asked you to choose."

    if payload.wa_id:
        selection_reply = handle_whatsapp_selection(body, payload.wa_id)
        if selection_reply:
            return selection_reply

    try:
        result = process_payment_message(body)
    except HTTPException as exc:
        return str(exc.detail)
    if result["status"] != "UNMATCHED":
        return summarize_match(result)

    candidates = result.get("candidates", [])[:9]
    if payload.wa_id and candidates:
        save_whatsapp_pending_match(
            wa_id=payload.wa_id,
            original_message=body,
            amount=float(result["amount"]),
            payment_date=result["date"],
            sender_key=result.get("sender_key"),
            sender_name=result.get("extracted_tenant_name"),
            unmatched_payment_id=result.get("unmatched_payment_id"),
            candidates=candidates,
        )
        return build_candidate_prompt(candidates, result.get("extracted_tenant_name"))

    return "I could not auto-match this payment, and there were no tenant options to suggest. Please review it in the dashboard."


@app.on_event("startup")
def startup() -> None:
    init_db()
    refresh_all_statuses()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def validate_property_payload(payload: PropertyCreateRequest) -> Dict[str, Any]:
    tenant_name = payload.tenant_name.strip()
    if not tenant_name:
        raise HTTPException(status_code=400, detail="Tenant name is required.")
    property_name = payload.property_name.strip()
    if not property_name:
        raise HTTPException(status_code=400, detail="Property name is required.")

    lease_start = None
    lease_end = None
    if payload.lease_start:
        try:
            lease_start = datetime.strptime(payload.lease_start, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Lease start must be YYYY-MM-DD.") from exc
    if payload.lease_end:
        try:
            lease_end = datetime.strptime(payload.lease_end, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Lease end must be YYYY-MM-DD.") from exc
    if payload.rent_due_day is not None and not 1 <= payload.rent_due_day <= 31:
        raise HTTPException(status_code=400, detail="Rent due day must be between 1 and 31.")
    rent_increases = normalize_rent_increases(payload.rent_increases)

    return {
        "tenant_name": tenant_name,
        "property_name": property_name,
        "rent_amount": round(float(payload.rent_amount), 2),
        "rent_due_day": payload.rent_due_day,
        "lease_start": lease_start,
        "lease_end": lease_end,
        "rent_increases": rent_increases,
    }


@app.post("/properties")
def create_property(payload: PropertyCreateRequest) -> Dict[str, Any]:
    data = validate_property_payload(payload)

    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO properties (
                tenant_name, property_name, rent_amount, rent_due_day, lease_start, lease_end, status, last_paid_date, last_payment_amount, current_month_paid_amount
            ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', NULL, NULL, NULL)
            """,
            (
                data["tenant_name"],
                data["property_name"],
                data["rent_amount"],
                data["rent_due_day"],
                data["lease_start"],
                data["lease_end"],
            ),
        )
        property_id = int(cursor.lastrowid)
        save_rent_increases(conn, property_id, data["rent_increases"])
        conn.commit()
        row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()

    return enrich_property(row)


@app.put("/properties/{property_id}")
def update_property(property_id: int, payload: PropertyCreateRequest) -> Dict[str, Any]:
    data = validate_property_payload(payload)
    with closing(get_connection()) as conn:
        existing_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
        if not existing_row:
            raise HTTPException(status_code=404, detail="Property not found.")

        conn.execute(
            """
            UPDATE properties
            SET tenant_name = ?, property_name = ?, rent_amount = ?, rent_due_day = ?, lease_start = ?, lease_end = ?
            WHERE id = ?
            """,
            (
                data["tenant_name"],
                data["property_name"],
                data["rent_amount"],
                data["rent_due_day"],
                data["lease_start"],
                data["lease_end"],
                property_id,
            ),
        )
        save_rent_increases(conn, property_id, data["rent_increases"])
        conn.commit()
        updated_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
    return enrich_property(updated_row)


@app.delete("/properties/{property_id}")
def delete_property(property_id: int) -> Dict[str, Any]:
    with closing(get_connection()) as conn:
        existing_row = conn.execute("SELECT id FROM properties WHERE id = ?", (property_id,)).fetchone()
        if not existing_row:
            raise HTTPException(status_code=404, detail="Property not found.")

        conn.execute("DELETE FROM rent_increases WHERE property_id = ?", (property_id,))
        conn.execute("DELETE FROM tenant_aliases WHERE property_id = ?", (property_id,))
        conn.execute("DELETE FROM properties WHERE id = ?", (property_id,))
        conn.commit()
    return {"status": "deleted", "property_id": property_id}


@app.get("/properties")
def get_properties() -> List[Dict[str, Any]]:
    refresh_all_statuses()
    return all_properties()


@app.get("/unmatched-payments")
def get_unmatched_payments() -> List[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        rows = conn.execute(
            "SELECT * FROM unmatched_payments WHERE status = 'UNMATCHED' ORDER BY id DESC"
        ).fetchall()
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["candidates"] = json.loads(item.pop("candidates_json"))
        result.append(item)
    return result


@app.post("/process-payment")
def process_payment(payload: PaymentRequest) -> Dict[str, Any]:
    return process_payment_message(payload.message)


@app.post("/manual-match")
def manual_match(payload: ManualMatchRequest) -> Dict[str, Any]:
    try:
        normalized_date = datetime.strptime(payload.date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD.") from exc

    return manual_match_payment(
        property_id=payload.property_id,
        amount=payload.amount,
        normalized_date=normalized_date,
        unmatched_payment_id=payload.unmatched_payment_id,
        sender_key=payload.sender_key,
    )


@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(
    Body: str = Form(default=""),
    WaId: Optional[str] = Form(default=None),
    ProfileName: Optional[str] = Form(default=None),
) -> Response:
    payload = WhatsAppInboundPayload(body=Body, wa_id=WaId, profile_name=ProfileName)
    reply = process_whatsapp_message(payload)
    return Response(content=build_twiml_message(reply), media_type="application/xml")
