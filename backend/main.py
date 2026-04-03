import json
import hmac
import math
import os
import re
from contextlib import closing
from datetime import date, datetime, timedelta
from html import escape
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Form, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from db import Connection, Row, get_connection, init_db, row_to_dict

SESSION_COOKIE_NAME = "rentdesk_session"
SESSION_MAX_AGE_HOURS = int(os.getenv("SESSION_MAX_AGE_HOURS", "12"))
SESSION_SECRET = os.getenv("APP_SESSION_SECRET", "change-this-session-secret")
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"
APP_USERS_ENV = os.getenv("APP_USERS", "admin:changeme")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_API_KEY = os.getenv("TWILIO_API_KEY", "").strip()
TWILIO_API_SECRET = os.getenv("TWILIO_API_SECRET", "").strip()
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "").strip()
TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID = os.getenv("TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID", "").strip()
PUBLIC_PATH_PREFIXES = (
    "/health",
    "/auth/login",
    "/webhooks/whatsapp",
)
PUBLIC_EXACT_PATHS = {"/", "/favicon.ico"}


def parse_allowed_origins() -> List[str]:
    origins = os.getenv(
        "ALLOW_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000",
    )
    return [item.strip() for item in origins.split(",") if item.strip()]


ALLOWED_ORIGINS = parse_allowed_origins()
ALLOW_CREDENTIALS = "*" not in ALLOWED_ORIGINS

app = FastAPI(title="Rent Management MVP")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PaymentRequest(BaseModel):
    message: str


class LoginRequest(BaseModel):
    username: str
    password: str


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
    phone_number: Optional[str] = None
    unit_number: Optional[str] = None
    property_address: Optional[str] = None
    security_deposit: Optional[float] = None
    lease_terms: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_phone: Optional[str] = None
    rent_increases: List[RentIncreaseRequest] = Field(default_factory=list)


class ManualMatchRequest(BaseModel):
    property_id: int
    amount: float
    date: str
    unmatched_payment_id: Optional[int] = None
    sender_key: Optional[str] = None


class InboxMatchRequest(BaseModel):
    property_id: int
    sender_key: Optional[str] = None
    note: Optional[str] = None


class ReviewDecisionRequest(BaseModel):
    note: Optional[str] = None


class UndoPaymentRequest(BaseModel):
    reason: Optional[str] = None


class WhatsAppInboundPayload(BaseModel):
    body: str
    wa_id: Optional[str] = None
    profile_name: Optional[str] = None


class WhatsAppOutboundRequest(BaseModel):
    message: Optional[str] = None


def normalize_whatsapp_number(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    digits = "".join(char for char in value if char.isdigit())
    return digits or None


def format_whatsapp_address(value: str) -> str:
    digits = normalize_whatsapp_number(value)
    if not digits:
        raise HTTPException(status_code=400, detail="A valid WhatsApp number is required.")
    return f"whatsapp:+{digits}"


def configured_whatsapp_sender() -> str:
    if not TWILIO_WHATSAPP_FROM:
        raise HTTPException(status_code=500, detail="TWILIO_WHATSAPP_FROM is not configured.")
    sender = TWILIO_WHATSAPP_FROM
    if not sender.startswith("whatsapp:"):
        sender = format_whatsapp_address(sender)
    return sender


def send_whatsapp_message(
    to_number: str,
    body: str = "",
    *,
    content_sid: Optional[str] = None,
    content_variables: Optional[Dict[str, Any]] = None,
) -> str:
    if not TWILIO_ACCOUNT_SID or not TWILIO_API_KEY or not TWILIO_API_SECRET:
        raise HTTPException(status_code=500, detail="Twilio credentials are not configured.")

    message_body = body.strip()
    if not content_sid and not message_body:
        raise HTTPException(status_code=400, detail="Message body cannot be empty.")

    endpoint = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
    data = {
        "To": format_whatsapp_address(to_number),
        "From": configured_whatsapp_sender(),
    }
    if content_sid:
        data["ContentSid"] = content_sid
        if content_variables:
            data["ContentVariables"] = json.dumps(content_variables)
    else:
        data["Body"] = message_body
    try:
        response = httpx.post(
            endpoint,
            auth=(TWILIO_API_KEY, TWILIO_API_SECRET),
            data=data,
            timeout=20.0,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Could not reach Twilio.") from exc

    if response.status_code >= 400:
        try:
            error_payload = response.json()
        except ValueError:
            error_payload = {}
        detail = error_payload.get("message") or "Twilio rejected the WhatsApp message."
        raise HTTPException(status_code=502, detail=detail)

    payload = response.json()
    return str(payload.get("sid") or "")


def parse_app_user_configs() -> List[Dict[str, Optional[str]]]:
    users: List[Dict[str, Optional[str]]] = []
    for item in APP_USERS_ENV.split(","):
        if ":" not in item:
            continue
        parts = item.split(":")
        if len(parts) < 2:
            continue
        username = parts[0].strip()
        password = parts[1].strip()
        whatsapp_number = normalize_whatsapp_number(":".join(parts[2:]).strip()) if len(parts) > 2 else None
        username = username.strip()
        if username and password:
            users.append(
                {
                    "username": username,
                    "password": password,
                    "whatsapp_number": whatsapp_number,
                }
            )
    if not users:
        users.append(
            {
                "username": "admin",
                "password": "changeme",
                "whatsapp_number": None,
            }
        )
    return users


APP_USER_CONFIGS = parse_app_user_configs()
APP_USERS = {item["username"]: item["password"] for item in APP_USER_CONFIGS}
DEFAULT_APP_USERNAME = APP_USER_CONFIGS[0]["username"] or "admin"


def get_request_username(request: Request) -> str:
    return getattr(request.state, "username", None) or require_authenticated_user(request)


def get_user_by_username(username: str) -> Optional[Row]:
    with closing(get_connection()) as conn:
        return conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()


def update_user_login_tracking(username: str) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            """
            UPDATE users
            SET last_login_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = ?
            """,
            (username,),
        )
        conn.commit()


def mark_user_onboarding_sent(username: str) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            """
            UPDATE users
            SET whatsapp_onboarding_sent_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = ?
            """,
            (username,),
        )
        conn.commit()


def send_first_login_onboarding(username: str) -> str:
    user_row = get_user_by_username(username)
    if not user_row:
        return "skipped_unknown_user"

    if user_row.get("whatsapp_onboarding_sent_at"):
        return "already_sent"

    whatsapp_number = user_row.get("whatsapp_number")
    if not whatsapp_number:
        return "skipped_no_whatsapp_number"

    if not TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID:
        return "skipped_missing_template"

    send_whatsapp_message(
        whatsapp_number,
        content_sid=TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID,
    )
    mark_user_onboarding_sent(username)
    return "sent"


def get_user_by_whatsapp_number(wa_id: Optional[str]) -> Optional[Row]:
    normalized_wa_id = normalize_whatsapp_number(wa_id)
    if not normalized_wa_id:
        return None
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM users WHERE whatsapp_number = ?",
            (normalized_wa_id,),
        ).fetchone()


def require_owned_property(conn: Connection, property_id: int, owner_username: str) -> Row:
    row = conn.execute(
        "SELECT * FROM properties WHERE id = ? AND owner_username = ?",
        (property_id, owner_username),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Property not found.")
    return row


def require_owned_unmatched_payment(conn: Connection, unmatched_payment_id: int, owner_username: str) -> Row:
    row = conn.execute(
        "SELECT * FROM unmatched_payments WHERE id = ? AND owner_username = ?",
        (unmatched_payment_id, owner_username),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Unmatched payment not found.")
    return row


def build_session_token(username: str) -> str:
    expires_at = int((datetime.utcnow() + timedelta(hours=SESSION_MAX_AGE_HOURS)).timestamp())
    payload = f"{username}|{expires_at}"
    signature = hmac.new(
        SESSION_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        "sha256",
    ).hexdigest()
    return f"{payload}|{signature}"


def get_authenticated_username(session_token: Optional[str]) -> Optional[str]:
    if not session_token:
        return None

    try:
        username, expires_at, signature = session_token.split("|", 2)
    except ValueError:
        return None

    payload = f"{username}|{expires_at}"
    expected_signature = hmac.new(
        SESSION_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        "sha256",
    ).hexdigest()

    if not hmac.compare_digest(signature, expected_signature):
        return None

    try:
        expires_ts = int(expires_at)
    except ValueError:
        return None

    if datetime.utcnow().timestamp() > expires_ts:
        return None

    user_row = get_user_by_username(username)
    if not user_row:
        return None

    return username


def require_authenticated_user(request: Request) -> str:
    username = get_authenticated_username(request.cookies.get(SESSION_COOKIE_NAME))
    if not username:
        raise HTTPException(status_code=401, detail="Authentication required.")
    return username


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    if request.url.path in PUBLIC_EXACT_PATHS or request.url.path.startswith(PUBLIC_PATH_PREFIXES):
        return await call_next(request)

    username = get_authenticated_username(request.cookies.get(SESSION_COOKIE_NAME))
    if not username:
        return Response(
            content=json.dumps({"detail": "Authentication required."}),
            status_code=401,
            media_type="application/json",
        )

    request.state.username = username
    return await call_next(request)


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def clean_optional_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def validate_amount(value: float, label: str, *, allow_zero: bool = False) -> float:
    amount = float(value)
    if not math.isfinite(amount):
        raise HTTPException(status_code=400, detail=f"{label} must be a finite number.")
    if allow_zero:
        if amount < 0:
            raise HTTPException(status_code=400, detail=f"{label} cannot be negative.")
    elif amount <= 0:
        raise HTTPException(status_code=400, detail=f"{label} must be greater than 0.")
    return round(amount, 2)


def parse_iso_date(value: str, label: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{label} must be YYYY-MM-DD.") from exc


def parse_month_reference(month: Optional[str]) -> date:
    if not month:
        today = date.today()
        return today.replace(day=1)
    try:
        parsed = datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Month must be YYYY-MM.") from exc
    return parsed.replace(day=1)


def month_start(reference_date: date) -> date:
    return reference_date.replace(day=1)


def add_months(reference_date: date, months: int) -> date:
    zero_based_month = reference_date.month - 1 + months
    year = reference_date.year + zero_based_month // 12
    month = zero_based_month % 12 + 1
    return date(year, month, 1)


def last_day_of_month(reference_date: date) -> date:
    return add_months(reference_date.replace(day=1), 1) - timedelta(days=1)


def format_month(reference_date: date) -> str:
    return reference_date.strftime("%Y-%m")


def get_rent_increases_for_property(
    property_id: int,
    conn: Connection,
) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, property_id, date_from, date_till, rent_amount
        FROM rent_increases
        WHERE property_id = ?
        ORDER BY date_from ASC, id ASC
        """,
        (property_id,),
    ).fetchall()
    return [row_to_dict(row) for row in rows]


def normalize_rent_increases(rent_increases: List[RentIncreaseRequest]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    parsed_ranges: List[Tuple[date, date]] = []

    for item in rent_increases:
        try:
            date_from = datetime.strptime(item.date_from, "%Y-%m-%d").date()
            date_till = datetime.strptime(item.date_till, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="Rent increase dates must be YYYY-MM-DD.",
            ) from exc

        if date_till < date_from:
            raise HTTPException(
                status_code=400,
                detail="Rent increase end date must be on or after start date.",
            )

        normalized.append(
            {
                "date_from": date_from.strftime("%Y-%m-%d"),
                "date_till": date_till.strftime("%Y-%m-%d"),
                "rent_amount": validate_amount(item.rent_amount, "Rent increase amount"),
            }
        )
        parsed_ranges.append((date_from, date_till))

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
    conn: Connection,
    property_id: int,
    rent_increases: List[Dict[str, Any]],
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


def rent_amount_for_period(
    row: Row,
    reference_date: date,
    rent_increases: List[Dict[str, Any]],
) -> float:
    for increase in rent_increases:
        start_dt = datetime.strptime(increase["date_from"], "%Y-%m-%d").date()
        end_dt = datetime.strptime(increase["date_till"], "%Y-%m-%d").date()
        if start_dt <= reference_date <= end_dt:
            return round(float(increase["rent_amount"]), 2)
    return round(float(row["rent_amount"] or 0), 2)


def sum_posted_payments_for_period(
    conn: Connection,
    property_id: int,
    reference_date: date,
) -> float:
    start_date = month_start(reference_date).strftime("%Y-%m-%d")
    end_date = last_day_of_month(reference_date).strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM payments
        WHERE property_id = ?
          AND status = 'POSTED'
          AND payment_date BETWEEN ? AND ?
        """,
        (property_id, start_date, end_date),
    ).fetchone()
    return round(float(row["total"] or 0), 2)


def latest_posted_payment(
    conn: Connection,
    property_id: int,
) -> Optional[Row]:
    return conn.execute(
        """
        SELECT *
        FROM payments
        WHERE property_id = ? AND status = 'POSTED'
        ORDER BY payment_date DESC, id DESC
        LIMIT 1
        """,
        (property_id,),
    ).fetchone()


def status_for_period(
    row: Row,
    reference_date: date,
    expected_rent: float,
    collected_amount: float,
) -> str:
    if collected_amount > expected_rent > 0:
        return "SURPLUS"
    if collected_amount == expected_rent and expected_rent > 0:
        return "PAID"
    if collected_amount > 0:
        return "PARTIALLY_PAID"

    rent_due_day = row["rent_due_day"]
    if rent_due_day is None:
        return "PENDING"

    today = date.today()
    due_day = min(int(rent_due_day), last_day_of_month(reference_date).day)
    due_date = reference_date.replace(day=due_day)

    if reference_date.year < today.year or (
        reference_date.year == today.year and reference_date.month < today.month
    ):
        return "LATE"

    if reference_date.year == today.year and reference_date.month == today.month and today > due_date:
        return "LATE"

    return "PENDING"


def sync_property_cache(conn: Connection, property_id: int) -> None:
    property_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
    if not property_row:
        return

    reference_date = date.today()
    rent_increases = get_rent_increases_for_property(property_id, conn)
    expected_rent = rent_amount_for_period(property_row, reference_date, rent_increases)
    collected_amount = sum_posted_payments_for_period(conn, property_id, reference_date)
    status = status_for_period(property_row, reference_date, expected_rent, collected_amount)
    latest_payment = latest_posted_payment(conn, property_id)

    conn.execute(
        """
        UPDATE properties
        SET status = ?,
            last_paid_date = ?,
            last_payment_amount = ?,
            current_month_paid_amount = ?
        WHERE id = ?
        """,
        (
            status,
            latest_payment["payment_date"] if latest_payment else None,
            round(float(latest_payment["amount"]), 2) if latest_payment else None,
            collected_amount,
            property_id,
        ),
    )


def enrich_property(
    row: Row,
    reference_date: date,
    conn: Connection,
) -> Dict[str, Any]:
    property_id = int(row["id"])
    rent_increases = get_rent_increases_for_property(property_id, conn)
    current_rent_amount = rent_amount_for_period(row, reference_date, rent_increases)
    collected_amount = sum_posted_payments_for_period(conn, property_id, reference_date)
    balance_amount = round(max(current_rent_amount - collected_amount, 0), 2)
    surplus_amount = round(max(collected_amount - current_rent_amount, 0), 2)
    status = status_for_period(row, reference_date, current_rent_amount, collected_amount)
    due_date = None
    if row["rent_due_day"] is not None:
        due_date = reference_date.replace(
            day=min(int(row["rent_due_day"]), last_day_of_month(reference_date).day)
        ).strftime("%Y-%m-%d")

    return {
        **row_to_dict(row),
        "status": status,
        "selected_month": format_month(reference_date),
        "current_rent_amount": current_rent_amount,
        "current_month_paid_amount": collected_amount,
        "balance_amount": balance_amount,
        "surplus_amount": surplus_amount,
        "rent_increases": rent_increases,
        "due_date": due_date,
    }


def filter_and_sort_properties(
    items: List[Dict[str, Any]],
    query: str,
    status: str,
    sort: str,
) -> List[Dict[str, Any]]:
    normalized_query = normalize_name(query) if query.strip() else ""
    filtered = items

    if normalized_query:
        filtered = [
            item
            for item in filtered
            if normalized_query in normalize_name(item["tenant_name"])
            or normalized_query in normalize_name(item.get("property_name") or "")
            or normalized_query in normalize_name(item.get("unit_number") or "")
            or normalized_query in normalize_name(item.get("phone_number") or "")
            or normalized_query in normalize_name(item.get("property_address") or "")
        ]

    normalized_status = status.strip().upper()
    if normalized_status and normalized_status != "ALL":
        filtered = [item for item in filtered if item["status"] == normalized_status]

    if sort == "rent_desc":
        filtered.sort(key=lambda item: (-float(item["current_rent_amount"]), normalize_name(item["tenant_name"])))
    elif sort == "outstanding_desc":
        filtered.sort(key=lambda item: (-float(item["balance_amount"]), normalize_name(item["tenant_name"])))
    elif sort == "due_soon":
        filtered.sort(
            key=lambda item: (
                item["due_date"] or "9999-12-31",
                -float(item["balance_amount"]),
                normalize_name(item["tenant_name"]),
            )
        )
    elif sort == "recent_payment":
        filtered.sort(
            key=lambda item: (
                item.get("last_paid_date") or "0000-00-00",
                item.get("last_payment_amount") or 0,
            ),
            reverse=True,
        )
    elif sort == "tenant_desc":
        filtered.sort(key=lambda item: normalize_name(item["tenant_name"]), reverse=True)
    else:
        filtered.sort(key=lambda item: normalize_name(item["tenant_name"]))

    return filtered


def all_properties(
    reference_date: date,
    owner_username: str,
    *,
    query: str = "",
    status: str = "ALL",
    sort: str = "tenant_asc",
) -> List[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        rows = conn.execute(
            "SELECT * FROM properties WHERE owner_username = ? ORDER BY id DESC",
            (owner_username,),
        ).fetchall()
        items = [enrich_property(row, reference_date, conn) for row in rows]
    return filter_and_sort_properties(items, query, status, sort)


def find_property_by_sender_key(
    sender_key: Optional[str],
    *,
    owner_username: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not sender_key:
        return None
    with closing(get_connection()) as conn:
        if owner_username is None:
            row = conn.execute(
                """
                SELECT p.*
                FROM tenant_aliases ta
                JOIN properties p ON p.id = ta.property_id
                WHERE ta.sender_key = ?
                """,
                (sender_key,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT p.*
                FROM tenant_aliases ta
                JOIN properties p ON p.id = ta.property_id
                WHERE ta.sender_key = ?
                  AND p.owner_username = ?
                """,
                (sender_key, owner_username),
            ).fetchone()
        if not row:
            return None
        return enrich_property(row, date.today(), conn)


def candidate_properties(
    extracted_name: str,
    reference_date: date,
    *,
    owner_username: Optional[str] = None,
) -> List[Dict[str, Any]]:
    normalized_target = normalize_name(extracted_name)
    if not normalized_target:
        return []

    with closing(get_connection()) as conn:
        if owner_username is None:
            rows = conn.execute("SELECT * FROM properties ORDER BY id DESC").fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM properties WHERE owner_username = ? ORDER BY id DESC",
                (owner_username,),
            ).fetchall()
        matches: List[Dict[str, Any]] = []
        for row in rows:
            tenant_name = normalize_name(row["tenant_name"])
            property_name = normalize_name(row["property_name"] or "")
            if (
                normalized_target in tenant_name
                or tenant_name in normalized_target
                or normalized_target in property_name
            ):
                matches.append(enrich_property(row, reference_date, conn))
        return matches


def serialize_payment(row: Row) -> Dict[str, Any]:
    item = row_to_dict(row)
    item["amount"] = round(float(item["amount"]), 2)
    return item


def get_payment_history_for_property(
    conn: Connection,
    property_id: int,
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    query = [
        """
        SELECT *
        FROM payments
        WHERE property_id = ?
        """
    ]
    params: List[Any] = [property_id]

    if date_from:
        query.append("AND payment_date >= ?")
        params.append(date_from)
    if date_to:
        query.append("AND payment_date <= ?")
        params.append(date_to)

    query.append("ORDER BY payment_date DESC, id DESC")
    rows = conn.execute("\n".join(query), tuple(params)).fetchall()
    return [serialize_payment(row) for row in rows]


def summarize_payment_history(payments: List[Dict[str, Any]]) -> Dict[str, Any]:
    posted_payments = [item for item in payments if item["status"] == "POSTED"]
    reversed_payments = [item for item in payments if item["status"] != "POSTED"]
    return {
        "posted_count": len(posted_payments),
        "reversed_count": len(reversed_payments),
        "collected_total": round(sum(float(item["amount"]) for item in posted_payments), 2),
        "last_collected_on": posted_payments[0]["payment_date"] if posted_payments else None,
    }


def build_monthly_history(
    conn: Connection,
    property_row: Row,
    *,
    months: int = 6,
    reference_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    anchor = month_start(reference_date or date.today())
    rent_increases = get_rent_increases_for_property(int(property_row["id"]), conn)
    history: List[Dict[str, Any]] = []

    for offset in range(months - 1, -1, -1):
        month_ref = add_months(anchor, -offset)
        expected = rent_amount_for_period(property_row, month_ref, rent_increases)
        collected = sum_posted_payments_for_period(conn, int(property_row["id"]), month_ref)
        balance = round(max(expected - collected, 0), 2)
        surplus = round(max(collected - expected, 0), 2)
        history.append(
            {
                "month": format_month(month_ref),
                "expected": expected,
                "collected": collected,
                "balance": balance,
                "surplus": surplus,
                "status": status_for_period(property_row, month_ref, expected, collected),
            }
        )

    return history


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
    *,
    owner_username: Optional[str],
) -> int:
    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO unmatched_payments (
                owner_username,
                raw_message,
                extracted_tenant_name,
                sender_key,
                amount,
                payment_date,
                candidates_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_username,
                message,
                extracted_name,
                sender_key,
                amount,
                payment_date,
                json.dumps(candidates),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def set_unmatched_payment_status(
    conn: Connection,
    unmatched_payment_id: int,
    status: str,
    *,
    owner_username: Optional[str] = None,
    note: Optional[str] = None,
    matched_property_id: Optional[int] = None,
    matched_payment_id: Optional[int] = None,
) -> None:
    if owner_username is None:
        existing = conn.execute(
            "SELECT * FROM unmatched_payments WHERE id = ?",
            (unmatched_payment_id,),
        ).fetchone()
    else:
        existing = conn.execute(
            "SELECT * FROM unmatched_payments WHERE id = ? AND owner_username = ?",
            (unmatched_payment_id, owner_username),
        ).fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="Unmatched payment not found.")

    conn.execute(
        """
        UPDATE unmatched_payments
        SET status = ?,
            matched_property_id = ?,
            matched_payment_id = ?,
            resolution_note = ?,
            reviewed_at = ?
        WHERE id = ?
        """,
        (
            status,
            matched_property_id,
            matched_payment_id,
            note,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            unmatched_payment_id,
        ),
    )


def parse_amount(message: str) -> float:
    match = re.search(r"Rs\.?\s?([\d,]+\.\d+)", message)
    if not match:
        raise ValueError("Could not extract amount from payment message.")
    return validate_amount(float(match.group(1).replace(",", "")), "Payment amount")


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


def create_payment(
    property_id: int,
    amount: float,
    payment_date: str,
    *,
    owner_username: Optional[str] = None,
    source: str,
    sender_key: Optional[str] = None,
    raw_message: Optional[str] = None,
    unmatched_payment_id: Optional[int] = None,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    validated_amount = validate_amount(amount, "Payment amount")
    normalized_date = parse_iso_date(payment_date, "Payment date")

    with closing(get_connection()) as conn:
        if owner_username is None:
            property_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
        else:
            property_row = conn.execute(
                "SELECT * FROM properties WHERE id = ? AND owner_username = ?",
                (property_id, owner_username),
            ).fetchone()
        if not property_row:
            raise HTTPException(status_code=404, detail="Property not found.")

        cursor = conn.execute(
            """
            INSERT INTO payments (
                property_id,
                amount,
                payment_date,
                source,
                status,
                sender_key,
                raw_message,
                unmatched_payment_id,
                note
            )
            VALUES (?, ?, ?, ?, 'POSTED', ?, ?, ?, ?)
            """,
            (
                property_id,
                validated_amount,
                normalized_date,
                source,
                sender_key,
                raw_message,
                unmatched_payment_id,
                clean_optional_text(note),
            ),
        )
        payment_id = int(cursor.lastrowid)

        if unmatched_payment_id is not None:
            set_unmatched_payment_status(
                conn,
                unmatched_payment_id,
                "MATCHED",
                owner_username=owner_username,
                note=note,
                matched_property_id=property_id,
                matched_payment_id=payment_id,
            )

        sync_property_cache(conn, property_id)
        conn.commit()

        updated_row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
        payment_row = conn.execute("SELECT * FROM payments WHERE id = ?", (payment_id,)).fetchone()
        summary = enrich_property(updated_row, month_start(datetime.strptime(normalized_date, "%Y-%m-%d").date()), conn)

    return {
        "payment": serialize_payment(payment_row),
        "matched_property": summary,
        "status": summary["status"],
        "balance_amount": summary["balance_amount"],
        "current_month_paid_amount": summary["current_month_paid_amount"],
        "surplus_amount": summary["surplus_amount"],
    }


def process_payment_message(message: str, *, owner_username: Optional[str] = None) -> Dict[str, Any]:
    try:
        amount = parse_amount(message)
        payment_date = parse_payment_date(message)
        sender_details = parse_sender_details(message)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    matched_by_alias = find_property_by_sender_key(
        sender_details["sender_key"],
        owner_username=owner_username,
    )
    if matched_by_alias:
        result = create_payment(
            int(matched_by_alias["id"]),
            amount,
            payment_date,
            owner_username=owner_username,
            source="auto_alias",
            sender_key=sender_details["sender_key"],
            raw_message=message,
        )
        record_sender_alias(
            int(matched_by_alias["id"]),
            sender_details["sender_key"],
            sender_details["sender_name"],
        )
        return {
            "status": result["status"],
            "match_source": "saved_sender",
            "matched_property": result["matched_property"],
            "sender_key": sender_details["sender_key"],
            "extracted_tenant_name": sender_details["sender_name"],
            "amount": amount,
            "balance_amount": result["balance_amount"],
            "current_month_paid_amount": result["current_month_paid_amount"],
            "surplus_amount": result["surplus_amount"],
            "payment": result["payment"],
        }

    reference_date = month_start(datetime.strptime(payment_date, "%Y-%m-%d").date())
    candidates = candidate_properties(
        sender_details["sender_name"] or "",
        reference_date,
        owner_username=owner_username,
    )
    fallback_candidates = candidates or (
        all_properties(reference_date, owner_username) if owner_username is not None else []
    )
    unmatched_id = save_unmatched_payment(
        message,
        sender_details["sender_name"] or "",
        sender_details["sender_key"],
        amount,
        payment_date,
        fallback_candidates,
        owner_username=owner_username,
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
    *,
    owner_username: Optional[str] = None,
    unmatched_payment_id: Optional[int] = None,
    sender_key: Optional[str] = None,
    raw_message: Optional[str] = None,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    result = create_payment(
        property_id,
        amount,
        normalized_date,
        owner_username=owner_username,
        source="manual_match",
        sender_key=sender_key,
        raw_message=raw_message,
        unmatched_payment_id=unmatched_payment_id,
        note=note,
    )
    record_sender_alias(property_id, sender_key, None)
    return {
        "status": result["status"],
        "match_source": "manual",
        "matched_property": result["matched_property"],
        "sender_key": sender_key,
        "balance_amount": result["balance_amount"],
        "current_month_paid_amount": result["current_month_paid_amount"],
        "surplus_amount": result["surplus_amount"],
        "payment": result["payment"],
    }


def build_monthly_trends(reference_date: date, owner_username: str) -> List[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        property_rows = conn.execute(
            "SELECT * FROM properties WHERE owner_username = ?",
            (owner_username,),
        ).fetchall()
        trends: List[Dict[str, Any]] = []
        for offset in range(5, -1, -1):
            month_ref = add_months(month_start(reference_date), -offset)
            expected = 0.0
            collected = 0.0
            outstanding = 0.0
            for row in property_rows:
                rent_increases = get_rent_increases_for_property(int(row["id"]), conn)
                expected_rent = rent_amount_for_period(row, month_ref, rent_increases)
                collected_amount = sum_posted_payments_for_period(conn, int(row["id"]), month_ref)
                expected += expected_rent
                collected += collected_amount
                outstanding += max(expected_rent - collected_amount, 0)

            unmatched_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM unmatched_payments
                WHERE to_char(payment_date, 'YYYY-MM') = ?
                  AND owner_username = ?
                """,
                (format_month(month_ref), owner_username),
            ).fetchone()
            trends.append(
                {
                    "month": format_month(month_ref),
                    "expected": round(expected, 2),
                    "collected": round(collected, 2),
                    "outstanding": round(outstanding, 2),
                    "unmatched": int(unmatched_row["total"] or 0),
                }
            )
        return trends


def build_reminders(
    reference_date: date,
    properties: List[Dict[str, Any]],
    *,
    owner_username: str,
) -> List[Dict[str, Any]]:
    reminders: List[Dict[str, Any]] = []
    today = date.today()

    expected_total = round(sum(float(item["current_rent_amount"]) for item in properties), 2)
    collected_total = round(sum(float(item["current_month_paid_amount"]) for item in properties), 2)
    reminders.append(
        {
            "type": "summary",
            "severity": "info",
            "title": f"{format_month(reference_date)} collection summary",
            "description": f"Collected Rs. {collected_total:.2f} out of Rs. {expected_total:.2f}.",
        }
    )

    for item in properties:
        balance = float(item["balance_amount"])
        if balance <= 0 or not item.get("due_date"):
            continue

        due_date = datetime.strptime(item["due_date"], "%Y-%m-%d").date()
        days_until_due = (due_date - today).days
        if reference_date.year == today.year and reference_date.month == today.month:
            if 0 <= days_until_due <= 3:
                reminders.append(
                    {
                        "type": "upcoming_due",
                        "severity": "medium",
                        "title": f"{item['tenant_name']} is due soon",
                        "description": f"Rs. {balance:.2f} is still open for {item.get('property_name') or 'this property'} by {item['due_date']}.",
                        "property_id": item["id"],
                    }
                )
            elif days_until_due < 0:
                reminders.append(
                    {
                        "type": "overdue",
                        "severity": "high",
                        "title": f"{item['tenant_name']} is overdue",
                        "description": f"Rs. {balance:.2f} is overdue for {item.get('property_name') or 'this property'}.",
                        "property_id": item["id"],
                    }
                )
        elif reference_date < today.replace(day=1):
            reminders.append(
                {
                    "type": "month_overdue",
                    "severity": "high",
                    "title": f"{item['tenant_name']} still has a past-month balance",
                    "description": f"Rs. {balance:.2f} remained unpaid for {format_month(reference_date)}.",
                    "property_id": item["id"],
                }
            )

    with closing(get_connection()) as conn:
        unmatched_open = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM unmatched_payments
            WHERE status = 'UNMATCHED'
              AND owner_username = ?
            """
            ,
            (owner_username,),
        ).fetchone()
    if int(unmatched_open["total"] or 0) > 0:
        reminders.append(
            {
                "type": "unmatched",
                "severity": "high",
                "title": "Unmatched payments need review",
                "description": f"{int(unmatched_open['total'])} payment(s) are still waiting in the inbox.",
            }
        )

    return reminders[:10]


def build_attention_items(properties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = [
        item
        for item in properties
        if item["status"] in {"LATE", "PARTIALLY_PAID", "SURPLUS"} or float(item["balance_amount"]) > 0
    ]
    items.sort(
        key=lambda item: (
            -float(item["balance_amount"]),
            -float(item["surplus_amount"]),
            normalize_name(item["tenant_name"]),
        )
    )
    return items[:6]


def build_dashboard(reference_date: date, owner_username: str) -> Dict[str, Any]:
    properties = all_properties(reference_date, owner_username)
    reminders = build_reminders(reference_date, properties, owner_username=owner_username)
    attention_items = build_attention_items(properties)
    trends = build_monthly_trends(reference_date, owner_username)

    return {
        "period": format_month(reference_date),
        "metrics": {
            "expected": round(sum(float(item["current_rent_amount"]) for item in properties), 2),
            "collected": round(sum(float(item["current_month_paid_amount"]) for item in properties), 2),
            "outstanding": round(sum(float(item["balance_amount"]) for item in properties), 2),
            "surplus": round(sum(float(item["surplus_amount"]) for item in properties), 2),
            "unmatched": len(
                [
                    item
                    for item in get_unmatched_payments(owner_username=owner_username)
                    if item["status"] == "UNMATCHED"
                ]
            ),
            "needs_attention": len(attention_items),
        },
        "reminders": reminders,
        "attention_items": attention_items,
        "unpaid_items": [item for item in properties if float(item["balance_amount"]) > 0][:6],
        "trends": trends,
    }


def validate_property_payload(payload: PropertyCreateRequest) -> Dict[str, Any]:
    tenant_name = payload.tenant_name.strip()
    if not tenant_name:
        raise HTTPException(status_code=400, detail="Tenant name is required.")

    property_name = payload.property_name.strip()
    if not property_name:
        raise HTTPException(status_code=400, detail="Property name is required.")

    lease_start = clean_optional_text(payload.lease_start)
    lease_end = clean_optional_text(payload.lease_end)
    if lease_start:
        lease_start = parse_iso_date(lease_start, "Lease start")
    if lease_end:
        lease_end = parse_iso_date(lease_end, "Lease end")
    if lease_start and lease_end and lease_end < lease_start:
        raise HTTPException(status_code=400, detail="Lease end must be on or after lease start.")

    if payload.rent_due_day is not None and not 1 <= payload.rent_due_day <= 31:
        raise HTTPException(status_code=400, detail="Rent due day must be between 1 and 31.")

    return {
        "tenant_name": tenant_name,
        "property_name": property_name,
        "rent_amount": validate_amount(payload.rent_amount, "Rent amount"),
        "rent_due_day": payload.rent_due_day,
        "lease_start": lease_start,
        "lease_end": lease_end,
        "phone_number": clean_optional_text(payload.phone_number),
        "unit_number": clean_optional_text(payload.unit_number),
        "property_address": clean_optional_text(payload.property_address),
        "security_deposit": (
            validate_amount(payload.security_deposit, "Security deposit", allow_zero=True)
            if payload.security_deposit is not None
            else None
        ),
        "lease_terms": clean_optional_text(payload.lease_terms),
        "emergency_contact_name": clean_optional_text(payload.emergency_contact_name),
        "emergency_contact_phone": clean_optional_text(payload.emergency_contact_phone),
        "rent_increases": normalize_rent_increases(payload.rent_increases),
    }


def get_unmatched_payments(
    status: str = "UNMATCHED",
    *,
    owner_username: Optional[str] = None,
) -> List[Dict[str, Any]]:
    with closing(get_connection()) as conn:
        params: List[Any] = []
        query = "SELECT * FROM unmatched_payments"
        filters: List[str] = []
        if owner_username is not None:
            filters.append("owner_username = ?")
            params.append(owner_username)
        if status and status.upper() != "ALL":
            filters.append("status = ?")
            params.append(status.upper())
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY payment_date DESC, id DESC"
        rows = conn.execute(query, tuple(params)).fetchall()

    items = []
    for row in rows:
        item = row_to_dict(row)
        item["amount"] = round(float(item["amount"]), 2)
        raw_candidates = item.pop("candidates_json")
        item["candidates"] = json.loads(raw_candidates) if isinstance(raw_candidates, str) else raw_candidates
        items.append(item)
    return items


def build_twiml_message(message: str) -> str:
    escaped = escape(message)
    return f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{escaped}</Message></Response>'


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
                wa_id,
                original_message,
                amount,
                payment_date,
                sender_key,
                sender_name,
                unmatched_payment_id,
                candidates_json
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
    raw_candidates = item.pop("candidates_json")
    item["candidates"] = json.loads(raw_candidates) if isinstance(raw_candidates, str) else raw_candidates
    return item


def clear_whatsapp_pending_match(wa_id: str) -> None:
    with closing(get_connection()) as conn:
        conn.execute("DELETE FROM whatsapp_pending_matches WHERE wa_id = ?", (wa_id,))
        conn.commit()


def summarize_match(result: Dict[str, Any]) -> str:
    matched_property = result["matched_property"]
    parts = [
        f"Matched to {matched_property['tenant_name']}.",
        f"Status: {result['status']}.",
        f"Paid this month: Rs. {result['current_month_paid_amount']:.2f}.",
    ]
    if float(result.get("surplus_amount") or 0) > 0:
        parts.append(f"Surplus: Rs. {float(result['surplus_amount']):.2f}.")
    else:
        parts.append(f"Balance: Rs. {float(result['balance_amount']):.2f}.")
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
    whatsapp_user = get_user_by_whatsapp_number(wa_id)
    if not whatsapp_user:
        clear_whatsapp_pending_match(wa_id)
        return "This WhatsApp number is no longer linked to an app user. Please reconfigure the user and try again."

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
    if candidate.get("owner_username") != whatsapp_user["username"]:
        clear_whatsapp_pending_match(wa_id)
        return "That pending match does not belong to this app user anymore. Please resend the bank message."
    result = manual_match_payment(
        property_id=int(candidate["id"]),
        amount=float(pending["amount"]),
        normalized_date=pending["payment_date"],
        owner_username=whatsapp_user["username"],
        unmatched_payment_id=pending.get("unmatched_payment_id"),
        sender_key=pending.get("sender_key"),
        raw_message=pending.get("original_message"),
        note="Matched from WhatsApp selection.",
    )
    clear_whatsapp_pending_match(wa_id)
    return summarize_match(result)


def process_whatsapp_message(payload: WhatsAppInboundPayload) -> str:
    body = payload.body.strip()
    if not body:
        return "Send the bank credit message text exactly as received, or reply with a tenant number if I asked you to choose."

    whatsapp_user = get_user_by_whatsapp_number(payload.wa_id) if payload.wa_id else None
    if payload.wa_id and not whatsapp_user:
        return "This WhatsApp number is not linked to any app user yet. Add the user's WhatsApp number in the app config first."
    owner_username = whatsapp_user["username"] if whatsapp_user else None

    if payload.wa_id:
        selection_reply = handle_whatsapp_selection(body, payload.wa_id)
        if selection_reply:
            return selection_reply

    try:
        result = process_payment_message(body, owner_username=owner_username)
    except HTTPException as exc:
        return str(exc.detail)

    if result["status"] != "UNMATCHED":
        return summarize_match(result)

    candidates = result.get("candidates", [])[:9]
    candidate_owners = {item.get("owner_username") for item in candidates if item.get("owner_username")}
    if len(candidate_owners) > 1:
        candidates = []
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


def sync_users_from_config() -> None:
    with closing(get_connection()) as conn:
        for user in APP_USER_CONFIGS:
            conn.execute(
                """
                INSERT INTO users (username, password, whatsapp_number, last_login_at, whatsapp_onboarding_sent_at)
                VALUES (?, ?, ?, NULL, NULL)
                ON CONFLICT(username) DO UPDATE SET
                    password = excluded.password,
                    whatsapp_number = excluded.whatsapp_number,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    user["username"],
                    user["password"],
                    user["whatsapp_number"],
                ),
            )
        conn.commit()


def backfill_owner_usernames() -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            """
            UPDATE properties
            SET owner_username = ?
            WHERE owner_username IS NULL OR owner_username = ''
            """,
            (DEFAULT_APP_USERNAME,),
        )
        conn.execute(
            """
            UPDATE unmatched_payments
            SET owner_username = ?
            WHERE owner_username IS NULL
              AND matched_property_id IS NOT NULL
              AND matched_property_id IN (
                  SELECT id FROM properties WHERE owner_username = ?
              )
            """,
            (DEFAULT_APP_USERNAME, DEFAULT_APP_USERNAME),
        )
        conn.execute(
            """
            UPDATE unmatched_payments
            SET owner_username = ?
            WHERE owner_username IS NULL OR owner_username = ''
            """,
            (DEFAULT_APP_USERNAME,),
        )
        conn.commit()


@app.on_event("startup")
def startup() -> None:
    init_db()
    sync_users_from_config()
    backfill_owner_usernames()
    with closing(get_connection()) as conn:
        property_rows = conn.execute("SELECT id FROM properties").fetchall()
        for row in property_rows:
            sync_property_cache(conn, int(row["id"]))
        conn.commit()


@app.post("/auth/login")
def login(payload: LoginRequest, response: Response) -> Dict[str, Any]:
    username = payload.username.strip()
    password = payload.password

    user_row = get_user_by_username(username)
    expected_password = user_row["password"] if user_row else None
    if not expected_password or expected_password != password:
        raise HTTPException(status_code=401, detail="Invalid username or password.")

    first_login = not user_row.get("last_login_at")
    onboarding_status = "not_attempted"
    if first_login:
        onboarding_status = send_first_login_onboarding(username)
    update_user_login_tracking(username)

    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=build_session_token(username),
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="lax",
        max_age=SESSION_MAX_AGE_HOURS * 3600,
        path="/",
    )
    return {
        "username": username,
        "first_login": first_login,
        "whatsapp_onboarding_status": onboarding_status,
    }


@app.post("/auth/logout")
def logout(response: Response) -> Dict[str, str]:
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
    )
    return {"status": "logged_out"}


@app.get("/auth/me")
def auth_me(request: Request) -> Dict[str, str]:
    username = require_authenticated_user(request)
    return {"username": username}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/dashboard")
def dashboard(request: Request, month: Optional[str] = None) -> Dict[str, Any]:
    username = get_request_username(request)
    reference_date = parse_month_reference(month)
    return build_dashboard(reference_date, username)


@app.get("/reminders")
def reminders(request: Request, month: Optional[str] = None) -> List[Dict[str, Any]]:
    username = get_request_username(request)
    reference_date = parse_month_reference(month)
    properties = all_properties(reference_date, username)
    return build_reminders(reference_date, properties, owner_username=username)


@app.get("/properties")
def get_properties(
    request: Request,
    month: Optional[str] = None,
    q: str = "",
    status: str = "ALL",
    sort: str = "tenant_asc",
) -> List[Dict[str, Any]]:
    username = get_request_username(request)
    reference_date = parse_month_reference(month)
    return all_properties(reference_date, username, query=q, status=status, sort=sort)


@app.get("/properties/{property_id}/ledger")
def get_property_ledger(
    request: Request,
    property_id: int,
    month: Optional[str] = None,
    history_from: Optional[str] = None,
    history_to: Optional[str] = None,
) -> Dict[str, Any]:
    username = get_request_username(request)
    reference_date = parse_month_reference(month)
    normalized_history_from = parse_iso_date(history_from, "History from") if history_from else None
    normalized_history_to = parse_iso_date(history_to, "History to") if history_to else None
    if normalized_history_from and normalized_history_to and normalized_history_to < normalized_history_from:
        raise HTTPException(status_code=400, detail="History to must be on or after history from.")

    with closing(get_connection()) as conn:
        property_row = require_owned_property(conn, property_id, username)

        summary = enrich_property(property_row, reference_date, conn)
        payment_history = get_payment_history_for_property(
            conn,
            property_id,
            date_from=normalized_history_from,
            date_to=normalized_history_to,
        )
        monthly_history = build_monthly_history(conn, property_row, reference_date=reference_date)

    tenant_reminders = [
        item
        for item in build_reminders(reference_date, [summary], owner_username=username)
        if item.get("property_id") == property_id
    ]
    return {
        "property": summary,
        "payment_history": payment_history,
        "payment_history_summary": summarize_payment_history(payment_history),
        "payment_history_filters": {
            "history_from": normalized_history_from,
            "history_to": normalized_history_to,
        },
        "monthly_history": monthly_history,
        "reminders": tenant_reminders,
    }


@app.post("/properties")
def create_property(request: Request, payload: PropertyCreateRequest) -> Dict[str, Any]:
    username = get_request_username(request)
    data = validate_property_payload(payload)

    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO properties (
                owner_username,
                tenant_name,
                property_name,
                rent_amount,
                rent_due_day,
                lease_start,
                lease_end,
                phone_number,
                unit_number,
                property_address,
                security_deposit,
                lease_terms,
                emergency_contact_name,
                emergency_contact_phone,
                status,
                last_paid_date,
                last_payment_amount,
                current_month_paid_amount
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', NULL, NULL, 0)
            """,
            (
                username,
                data["tenant_name"],
                data["property_name"],
                data["rent_amount"],
                data["rent_due_day"],
                data["lease_start"],
                data["lease_end"],
                data["phone_number"],
                data["unit_number"],
                data["property_address"],
                data["security_deposit"],
                data["lease_terms"],
                data["emergency_contact_name"],
                data["emergency_contact_phone"],
            ),
        )
        property_id = int(cursor.lastrowid)
        save_rent_increases(conn, property_id, data["rent_increases"])
        sync_property_cache(conn, property_id)
        conn.commit()
        row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
        return enrich_property(row, date.today().replace(day=1), conn)


@app.put("/properties/{property_id}")
def update_property(request: Request, property_id: int, payload: PropertyCreateRequest) -> Dict[str, Any]:
    username = get_request_username(request)
    data = validate_property_payload(payload)

    with closing(get_connection()) as conn:
        require_owned_property(conn, property_id, username)

        conn.execute(
            """
            UPDATE properties
            SET tenant_name = ?,
                property_name = ?,
                rent_amount = ?,
                rent_due_day = ?,
                lease_start = ?,
                lease_end = ?,
                phone_number = ?,
                unit_number = ?,
                property_address = ?,
                security_deposit = ?,
                lease_terms = ?,
                emergency_contact_name = ?,
                emergency_contact_phone = ?
            WHERE id = ?
            """,
            (
                data["tenant_name"],
                data["property_name"],
                data["rent_amount"],
                data["rent_due_day"],
                data["lease_start"],
                data["lease_end"],
                data["phone_number"],
                data["unit_number"],
                data["property_address"],
                data["security_deposit"],
                data["lease_terms"],
                data["emergency_contact_name"],
                data["emergency_contact_phone"],
                property_id,
            ),
        )
        save_rent_increases(conn, property_id, data["rent_increases"])
        sync_property_cache(conn, property_id)
        conn.commit()
        row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
        return enrich_property(row, date.today().replace(day=1), conn)


@app.delete("/properties/{property_id}")
def delete_property(request: Request, property_id: int) -> Dict[str, Any]:
    username = get_request_username(request)
    with closing(get_connection()) as conn:
        require_owned_property(conn, property_id, username)

        conn.execute("DELETE FROM rent_increases WHERE property_id = ?", (property_id,))
        conn.execute("DELETE FROM tenant_aliases WHERE property_id = ?", (property_id,))
        conn.execute("DELETE FROM payments WHERE property_id = ?", (property_id,))
        conn.execute("DELETE FROM properties WHERE id = ?", (property_id,))
        conn.commit()
    return {"status": "deleted", "property_id": property_id}


@app.get("/unmatched-payments")
def unmatched_payments(request: Request, status: str = "UNMATCHED") -> List[Dict[str, Any]]:
    username = get_request_username(request)
    return get_unmatched_payments(status=status, owner_username=username)


@app.post("/process-payment")
def process_payment(request: Request, payload: PaymentRequest) -> Dict[str, Any]:
    username = get_request_username(request)
    return process_payment_message(payload.message, owner_username=username)


@app.post("/manual-match")
def manual_match(request: Request, payload: ManualMatchRequest) -> Dict[str, Any]:
    username = get_request_username(request)
    normalized_date = parse_iso_date(payload.date, "Date")
    return manual_match_payment(
        property_id=payload.property_id,
        amount=payload.amount,
        normalized_date=normalized_date,
        owner_username=username,
        unmatched_payment_id=payload.unmatched_payment_id,
        sender_key=payload.sender_key,
    )


@app.post("/unmatched-payments/{unmatched_payment_id}/match")
def review_match(request: Request, unmatched_payment_id: int, payload: InboxMatchRequest) -> Dict[str, Any]:
    username = get_request_username(request)
    with closing(get_connection()) as conn:
        unmatched_row = require_owned_unmatched_payment(conn, unmatched_payment_id, username)
        if unmatched_row["status"] != "UNMATCHED":
            raise HTTPException(status_code=400, detail="This payment has already been reviewed.")

    result = manual_match_payment(
        property_id=payload.property_id,
        amount=float(unmatched_row["amount"]),
        normalized_date=unmatched_row["payment_date"],
        owner_username=username,
        unmatched_payment_id=unmatched_payment_id,
        sender_key=payload.sender_key or unmatched_row["sender_key"],
        raw_message=unmatched_row["raw_message"],
        note=payload.note,
    )
    return result


@app.post("/unmatched-payments/{unmatched_payment_id}/reject")
def reject_unmatched_payment(
    request: Request,
    unmatched_payment_id: int,
    payload: ReviewDecisionRequest,
) -> Dict[str, Any]:
    username = get_request_username(request)
    with closing(get_connection()) as conn:
        set_unmatched_payment_status(
            conn,
            unmatched_payment_id,
            "REJECTED",
            owner_username=username,
            note=payload.note or "Rejected from dashboard.",
        )
        conn.commit()
    return {"status": "REJECTED", "unmatched_payment_id": unmatched_payment_id}


@app.post("/unmatched-payments/{unmatched_payment_id}/duplicate")
def mark_duplicate_unmatched_payment(
    request: Request,
    unmatched_payment_id: int,
    payload: ReviewDecisionRequest,
) -> Dict[str, Any]:
    username = get_request_username(request)
    with closing(get_connection()) as conn:
        set_unmatched_payment_status(
            conn,
            unmatched_payment_id,
            "DUPLICATE",
            owner_username=username,
            note=payload.note or "Marked as duplicate from dashboard.",
        )
        conn.commit()
    return {"status": "DUPLICATE", "unmatched_payment_id": unmatched_payment_id}


@app.post("/payments/{payment_id}/undo")
def undo_payment(request: Request, payment_id: int, payload: UndoPaymentRequest) -> Dict[str, Any]:
    username = get_request_username(request)
    with closing(get_connection()) as conn:
        payment_row = conn.execute(
            """
            SELECT pay.*
            FROM payments pay
            JOIN properties p ON p.id = pay.property_id
            WHERE pay.id = ?
              AND p.owner_username = ?
            """,
            (payment_id, username),
        ).fetchone()
        if not payment_row:
            raise HTTPException(status_code=404, detail="Payment not found.")
        if payment_row["status"] != "POSTED":
            raise HTTPException(status_code=400, detail="Only posted payments can be undone.")

        conn.execute(
            """
            UPDATE payments
            SET status = 'REVERSED',
                reversal_reason = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (clean_optional_text(payload.reason) or "Undone from dashboard.", payment_id),
        )

        if payment_row["unmatched_payment_id"] is not None:
            conn.execute(
                """
                UPDATE unmatched_payments
                SET status = 'UNMATCHED',
                    matched_property_id = NULL,
                    matched_payment_id = NULL,
                    resolution_note = NULL,
                    reviewed_at = NULL
                WHERE id = ?
                  AND owner_username = ?
                """,
                (payment_row["unmatched_payment_id"], username),
            )

        sync_property_cache(conn, int(payment_row["property_id"]))
        conn.commit()

        property_row = require_owned_property(conn, int(payment_row["property_id"]), username)
        summary = enrich_property(property_row, date.today().replace(day=1), conn)

    return {
        "status": "REVERSED",
        "payment_id": payment_id,
        "property_id": payment_row["property_id"],
        "matched_property": summary,
    }


@app.post("/whatsapp/onboarding")
def send_whatsapp_onboarding(
    request: Request,
    payload: WhatsAppOutboundRequest,
) -> Dict[str, Any]:
    username = get_request_username(request)
    user_row = get_user_by_username(username)
    whatsapp_number = user_row["whatsapp_number"] if user_row else None
    if not whatsapp_number:
        raise HTTPException(status_code=400, detail="No WhatsApp number is linked to this app user.")

    message = clean_optional_text(payload.message)
    if message:
        message_sid = send_whatsapp_message(whatsapp_number, message)
    else:
        if not TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID:
            raise HTTPException(
                status_code=400,
                detail="Set TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID to send onboarding outside an active WhatsApp conversation.",
            )
        message_sid = send_whatsapp_message(
            whatsapp_number,
            content_sid=TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID,
        )
        mark_user_onboarding_sent(username)
    return {
        "status": "sent",
        "to": format_whatsapp_address(whatsapp_number),
        "message": message or "template_onboarding",
        "sid": message_sid,
    }


@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(
    Body: str = Form(default=""),
    WaId: Optional[str] = Form(default=None),
    ProfileName: Optional[str] = Form(default=None),
) -> Response:
    payload = WhatsAppInboundPayload(body=Body, wa_id=WaId, profile_name=ProfileName)
    reply = process_whatsapp_message(payload)
    return Response(content=build_twiml_message(reply), media_type="application/xml")
