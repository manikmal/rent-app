import json
import os
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pdfplumber
import requests
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "rent_management.db"
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")


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


class PropertyCreateRequest(BaseModel):
    tenant_name: str
    rent_amount: float
    rent_due_day: Optional[int] = None
    lease_start: Optional[str] = None
    lease_end: Optional[str] = None


class ManualMatchRequest(BaseModel):
    property_id: int
    amount: float
    date: str
    unmatched_payment_id: Optional[int] = None
    sender_key: Optional[str] = None


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
            """
        )
        migrate_properties_table(conn)
        ensure_column(conn, "unmatched_payments", "sender_key", "TEXT")
        ensure_column(conn, "properties", "current_month_paid_amount", "REAL")
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
        raise ValueError("Could not extract NEFT sender details from payment message.")
    return {
        "sender_key": match.group(1).strip(),
        "sender_name": normalize_name(match.group(2)),
    }


def build_lease_prompt(text: str) -> str:
    return (
        'Extract:\n\n'
        '* rent due day\n'
        '* lease start date\n'
        '* lease end date\n\n'
        'Return ONLY JSON:\n'
        '{\n'
        '"rent_due_day": int,\n'
        '"lease_start": "YYYY-MM-DD",\n'
        '"lease_end": "YYYY-MM-DD"\n'
        '}\n\n'
        f"Text: {text}"
    )


def extract_json_from_response(response_text: str) -> Dict[str, Any]:
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", response_text, re.DOTALL)
        if not match:
            raise ValueError("AI response did not contain valid JSON.")
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError as exc:
            raise ValueError("AI response contained malformed JSON.") from exc


def call_ollama_for_lease(text: str) -> Dict[str, Any]:
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": build_lease_prompt(text),
                "stream": False,
            },
            timeout=120,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise ValueError(f"Failed to reach Ollama: {exc}") from exc

    raw_response = payload.get("response", "")
    parsed = extract_json_from_response(raw_response)

    try:
        rent_due_day = int(parsed["rent_due_day"])
        lease_start = datetime.strptime(parsed["lease_start"], "%Y-%m-%d").strftime("%Y-%m-%d")
        lease_end = datetime.strptime(parsed["lease_end"], "%Y-%m-%d").strftime("%Y-%m-%d")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("AI response was missing required lease fields.") from exc

    return {
        "rent_due_day": rent_due_day,
        "lease_start": lease_start,
        "lease_end": lease_end,
    }


def extract_pdf_text(upload: UploadFile) -> str:
    try:
        with pdfplumber.open(upload.file) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
    except Exception as exc:
        raise ValueError(f"Failed to parse PDF: {exc}") from exc
    text = "\n".join(page for page in pages if page).strip()
    if not text:
        raise ValueError("No readable text found in PDF.")
    return text


def calculate_status(row: sqlite3.Row, today: Optional[date] = None) -> str:
    today = today or date.today()
    last_paid_date = row["last_paid_date"]
    paid_amount = paid_amount_for_row(row)
    rent_amount = float(row["rent_amount"] or 0)
    if last_paid_date:
        paid_dt = datetime.strptime(last_paid_date, "%Y-%m-%d").date()
        if paid_dt.year == today.year and paid_dt.month == today.month:
            if paid_amount >= rent_amount > 0:
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


def paid_amount_for_row(row: sqlite3.Row) -> float:
    if row["current_month_paid_amount"] is not None:
        return float(row["current_month_paid_amount"])
    return float(row["last_payment_amount"] or 0)


def balance_amount_for_row(row: sqlite3.Row) -> float:
    return round(max(float(row["rent_amount"]) - paid_amount_for_row(row), 0), 2)


def enrich_property(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        **row_to_dict(row),
        "status": calculate_status(row),
        "balance_amount": balance_amount_for_row(row),
        "current_month_paid_amount": round(paid_amount_for_row(row), 2),
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
            candidate["current_month_paid_amount"] = round(paid_amount_for_row(prop), 2)
            matches.append(candidate)
    return matches


def all_properties() -> List[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        rows = conn.execute("SELECT * FROM properties ORDER BY id DESC").fetchall()
    return [enrich_property(row) for row in rows]


def find_property_by_sender_key(sender_key: str) -> Optional[Dict[str, Any]]:
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
    payment_dt = datetime.strptime(payment_date, "%Y-%m-%d")
    existing_total = 0.0
    if row["last_paid_date"]:
        last_paid_dt = datetime.strptime(row["last_paid_date"], "%Y-%m-%d")
        if last_paid_dt.year == payment_dt.year and last_paid_dt.month == payment_dt.month:
            existing_total = paid_amount_for_row(row)

    total_paid = round(existing_total + payment_amount, 2)
    rent_amount = round(float(row["rent_amount"]), 2)
    balance_amount = round(max(rent_amount - total_paid, 0), 2)
    return {
        "total_paid": total_paid,
        "rent_amount": rent_amount,
        "balance_amount": balance_amount,
    }


def apply_payment_to_property(property_id: int, payment_amount: float, payment_date: str) -> Dict[str, Any]:
    with closing(get_connection()) as conn:
        property_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
        if not property_row:
            raise HTTPException(status_code=404, detail="Property not found.")

        totals = calculate_payment_totals(property_row, payment_amount, payment_date)
        status = "PAID" if totals["balance_amount"] == 0 else "PARTIALLY_PAID"

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


@app.on_event("startup")
def startup() -> None:
    init_db()
    refresh_all_statuses()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/upload-lease")
async def upload_lease(
    file: UploadFile = File(...),
    tenant_name: str = Form(...),
    rent_amount: float = Form(...),
) -> Dict[str, Any]:
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")

    try:
        extracted_text = extract_pdf_text(file)
        lease_data = call_ollama_for_lease(extracted_text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO properties (
                tenant_name, rent_amount, rent_due_day, lease_start, lease_end, status, last_paid_date, last_payment_amount, current_month_paid_amount
            ) VALUES (?, ?, ?, ?, ?, 'PENDING', NULL, NULL, NULL)
            """,
            (
                tenant_name.strip(),
                rent_amount,
                lease_data["rent_due_day"],
                lease_data["lease_start"],
                lease_data["lease_end"],
            ),
        )
        property_id = int(cursor.lastrowid)
        conn.commit()

    return {
        "property_id": property_id,
        "tenant_name": tenant_name.strip(),
        "rent_amount": rent_amount,
        **lease_data,
    }


@app.post("/properties")
def create_property(payload: PropertyCreateRequest) -> Dict[str, Any]:
    tenant_name = payload.tenant_name.strip()
    if not tenant_name:
        raise HTTPException(status_code=400, detail="Tenant name is required.")

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

    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO properties (
                tenant_name, rent_amount, rent_due_day, lease_start, lease_end, status, last_paid_date, last_payment_amount, current_month_paid_amount
            ) VALUES (?, ?, ?, ?, ?, 'PENDING', NULL, NULL, NULL)
            """,
            (tenant_name, payload.rent_amount, payload.rent_due_day, lease_start, lease_end),
        )
        property_id = int(cursor.lastrowid)
        conn.commit()
        row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()

    return enrich_property(row)


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
    try:
        amount = parse_amount(payload.message)
        payment_date = parse_payment_date(payload.message)
        sender_details = parse_sender_details(payload.message)
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
        }

    candidates = candidate_properties(sender_details["sender_name"] or "")
    fallback_candidates = candidates or all_properties()

    unmatched_id = save_unmatched_payment(
        payload.message,
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
    }


@app.post("/manual-match")
def manual_match(payload: ManualMatchRequest) -> Dict[str, Any]:
    try:
        normalized_date = datetime.strptime(payload.date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD.") from exc

    with closing(get_connection()) as conn:
        property_row = conn.execute(
            "SELECT * FROM properties WHERE id = ?", (payload.property_id,)
        ).fetchone()
        if not property_row:
            raise HTTPException(status_code=404, detail="Property not found.")

        payment_row = conn.execute("SELECT * FROM properties WHERE id = ?", (payload.property_id,)).fetchone()
        if not payment_row:
            raise HTTPException(status_code=404, detail="Property not found.")

        totals = calculate_payment_totals(payment_row, payload.amount, normalized_date)
        status = "PAID" if totals["balance_amount"] == 0 else "PARTIALLY_PAID"
        conn.execute(
            """
            UPDATE properties
            SET status = ?, last_paid_date = ?, last_payment_amount = ?, current_month_paid_amount = ?
            WHERE id = ?
            """,
            (status, normalized_date, payload.amount, totals["total_paid"], payload.property_id),
        )

        if payload.unmatched_payment_id is not None:
            conn.execute(
                "UPDATE unmatched_payments SET status = 'MATCHED' WHERE id = ?",
                (payload.unmatched_payment_id,),
            )
        else:
            conn.execute(
                """
                UPDATE unmatched_payments
                SET status = 'MATCHED'
                WHERE amount = ? AND payment_date = ? AND status = 'UNMATCHED'
                """,
                (payload.amount, normalized_date),
            )

        conn.commit()

        updated_row = conn.execute(
            "SELECT * FROM properties WHERE id = ?", (payload.property_id,)
        ).fetchone()

    record_sender_alias(payload.property_id, payload.sender_key, None)

    enriched = enrich_property(updated_row)

    return {
        "status": enriched["status"],
        "match_source": "manual",
        "matched_property": enriched,
        "sender_key": payload.sender_key,
        "balance_amount": enriched["balance_amount"],
        "current_month_paid_amount": enriched["current_month_paid_amount"],
    }
