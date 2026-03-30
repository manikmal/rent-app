import importlib
import os
import sys
import unittest
from pathlib import Path

from fastapi.testclient import TestClient


BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


class RentAppTestCase(unittest.TestCase):
    def setUp(self):
        os.environ["DATABASE_URL"] = os.getenv(
            "DATABASE_URL",
            "postgresql://rent_app:rent_app@postgres:5432/rent_app",
        )
        os.environ["APP_USERS"] = "admin:testpass"
        os.environ["APP_SESSION_SECRET"] = "test-secret"
        os.environ["ALLOW_ORIGINS"] = "http://localhost"
        os.environ["COOKIE_SECURE"] = "false"

        import main

        self.main = importlib.reload(main)
        self.main.init_db()
        with self.main.get_connection() as conn:
            conn.execute("TRUNCATE TABLE payments RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE rent_increases RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE tenant_aliases RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE unmatched_payments RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE whatsapp_pending_matches")
            conn.execute("TRUNCATE TABLE properties RESTART IDENTITY CASCADE")
            conn.commit()
        self.client = TestClient(self.main.app)

    def tearDown(self):
        pass

    def login(self):
        response = self.client.post(
            "/auth/login",
            json={"username": "admin", "password": "testpass"},
        )
        self.assertEqual(response.status_code, 200)

    def create_property(self, tenant_name="John Doe", property_name="Flat 2A", rent_amount=12000):
        return self.client.post(
            "/properties",
            json={
                "tenant_name": tenant_name,
                "property_name": property_name,
                "rent_amount": rent_amount,
                "rent_due_day": 5,
                "lease_start": "2026-03-01",
                "lease_end": "2026-12-31",
                "phone_number": "9999999999",
                "unit_number": "2A",
                "property_address": "Test Address",
                "security_deposit": 20000,
                "lease_terms": "11 month lease",
                "emergency_contact_name": "Jane Doe",
                "emergency_contact_phone": "8888888888",
                "rent_increases": [],
            },
        )

    def test_dashboard_requires_auth(self):
        response = self.client.get("/dashboard?month=2026-03")
        self.assertEqual(response.status_code, 401)

        self.login()
        auth_me = self.client.get("/auth/me")
        self.assertEqual(auth_me.status_code, 200)
        self.assertEqual(auth_me.json()["username"], "admin")

    def test_create_property_updates_dashboard(self):
        self.login()
        create_response = self.create_property()
        self.assertEqual(create_response.status_code, 200)

        properties_response = self.client.get("/properties?month=2026-03")
        self.assertEqual(properties_response.status_code, 200)
        self.assertEqual(len(properties_response.json()), 1)

        dashboard_response = self.client.get("/dashboard?month=2026-03")
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertEqual(dashboard_response.json()["metrics"]["expected"], 12000.0)

    def test_review_match_and_undo_flow(self):
        self.login()
        create_response = self.create_property(tenant_name="John")
        self.assertEqual(create_response.status_code, 200)
        property_id = create_response.json()["id"]

        process_response = self.client.post(
            "/process-payment",
            json={"message": "Rs. 12000.00 credited on 29-Mar-26 by NEFT-ABC123-JOHN"},
        )
        self.assertEqual(process_response.status_code, 200)
        self.assertEqual(process_response.json()["status"], "UNMATCHED")
        unmatched_id = process_response.json()["unmatched_payment_id"]

        match_response = self.client.post(
            f"/unmatched-payments/{unmatched_id}/match",
            json={"property_id": property_id, "sender_key": "ABC123"},
        )
        self.assertEqual(match_response.status_code, 200)
        payment_id = match_response.json()["payment"]["id"]

        ledger_response = self.client.get(f"/properties/{property_id}/ledger?month=2026-03")
        self.assertEqual(ledger_response.status_code, 200)
        self.assertEqual(len(ledger_response.json()["payment_history"]), 1)
        self.assertEqual(ledger_response.json()["payment_history"][0]["status"], "POSTED")

        undo_response = self.client.post(f"/payments/{payment_id}/undo", json={})
        self.assertEqual(undo_response.status_code, 200)

        reopened_inbox = self.client.get("/unmatched-payments?status=UNMATCHED")
        self.assertEqual(reopened_inbox.status_code, 200)
        self.assertEqual(len(reopened_inbox.json()), 1)


if __name__ == "__main__":
    unittest.main()
