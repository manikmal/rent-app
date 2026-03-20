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


class ManualMatchRequest(BaseModel):
    property_id: int
    amount: float
    date: str
    unmatched_payment_id: Optional[int] = None


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_connection()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_name TEXT NOT NULL,
                rent_amount REAL NOT NULL,
                rent_due_day INTEGER NOT NULL,
                lease_start TEXT NOT NULL,
                lease_end TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                last_paid_date TEXT,
                last_payment_amount REAL
            );

            CREATE TABLE IF NOT EXISTS unmatched_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_message TEXT NOT NULL,
                extracted_tenant_name TEXT,
                amount REAL NOT NULL,
                payment_date TEXT NOT NULL,
                candidates_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'UNMATCHED',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.commit()


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


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


def parse_tenant_name(message: str) -> str:
    match = re.search(r"NEFT-[A-Z0-9]+-([A-Z]+)", message)
    if not match:
        raise ValueError("Could not extract tenant name from payment message.")
    return normalize_name(match.group(1))


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
    if last_paid_date:
        paid_dt = datetime.strptime(last_paid_date, "%Y-%m-%d").date()
        if paid_dt.year == today.year and paid_dt.month == today.month:
            return "PAID"
    if today.day > int(row["rent_due_day"]):
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
            matches.append(candidate)
    return matches


def save_unmatched_payment(
    message: str, extracted_name: str, amount: float, payment_date: str, candidates: List[Dict[str, Any]]
) -> int:
    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO unmatched_payments (raw_message, extracted_tenant_name, amount, payment_date, candidates_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (message, extracted_name, amount, payment_date, json.dumps(candidates)),
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
                tenant_name, rent_amount, rent_due_day, lease_start, lease_end, status, last_paid_date, last_payment_amount
            ) VALUES (?, ?, ?, ?, ?, 'PENDING', NULL, NULL)
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


@app.get("/properties")
def get_properties() -> List[Dict[str, Any]]:
    refresh_all_statuses()
    with closing(get_connection()) as conn:
        rows = conn.execute("SELECT * FROM properties ORDER BY id DESC").fetchall()
    return [{**row_to_dict(row), "status": calculate_status(row)} for row in rows]


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
        extracted_tenant_name = parse_tenant_name(payload.message)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    candidates = candidate_properties(extracted_tenant_name)

    if len(candidates) == 1:
        matched = candidates[0]
        with closing(get_connection()) as conn:
            conn.execute(
                """
                UPDATE properties
                SET status = 'PAID', last_paid_date = ?, last_payment_amount = ?
                WHERE id = ?
                """,
                (payment_date, amount, matched["id"]),
            )
            conn.commit()
        matched["status"] = "PAID"
        matched["last_paid_date"] = payment_date
        matched["last_payment_amount"] = amount
        return {"status": "PAID", "matched_property": matched}

    unmatched_id = save_unmatched_payment(
        payload.message, extracted_tenant_name, amount, payment_date, candidates
    )
    return {
        "status": "UNMATCHED",
        "unmatched_payment_id": unmatched_id,
        "candidates": candidates,
        "extracted_tenant_name": extracted_tenant_name,
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

        conn.execute(
            """
            UPDATE properties
            SET status = 'PAID', last_paid_date = ?, last_payment_amount = ?
            WHERE id = ?
            """,
            (normalized_date, payload.amount, payload.property_id),
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

    return {"status": "PAID", "matched_property": row_to_dict(updated_row)}
