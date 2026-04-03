import importlib
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

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
        os.environ["APP_USERS"] = "admin:testpass:919999000001,alice:alicepass:919999000002"
        os.environ["APP_SESSION_SECRET"] = "test-secret"
        os.environ["ALLOW_ORIGINS"] = "http://localhost"
        os.environ["COOKIE_SECURE"] = "false"
        os.environ["TWILIO_ACCOUNT_SID"] = "ACtest"
        os.environ["TWILIO_API_KEY"] = "SKtest"
        os.environ["TWILIO_API_SECRET"] = "secret"
        os.environ["TWILIO_WHATSAPP_FROM"] = "whatsapp:+16624305856"
        os.environ["TWILIO_WHATSAPP_ONBOARDING_CONTENT_SID"] = "HXtest"

        import main

        self.main = importlib.reload(main)
        self.main.init_db()
        self.main.sync_users_from_config()
        self.main.backfill_owner_usernames()
        with self.main.get_connection() as conn:
            conn.execute("TRUNCATE TABLE users RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE payments RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE rent_increases RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE tenant_aliases RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE unmatched_payments RESTART IDENTITY CASCADE")
            conn.execute("TRUNCATE TABLE whatsapp_pending_matches")
            conn.execute("TRUNCATE TABLE properties RESTART IDENTITY CASCADE")
            conn.commit()
        self.main.sync_users_from_config()
        self.client = TestClient(self.main.app)

    def tearDown(self):
        pass

    def login(self, username="admin", password="testpass", client=None):
        active_client = client or self.client
        response = active_client.post(
            "/auth/login",
            json={"username": username, "password": password},
        )
        self.assertEqual(response.status_code, 200)
        return active_client

    def create_property(
        self,
        tenant_name="John Doe",
        property_name="Flat 2A",
        rent_amount=12000,
        client=None,
    ):
        active_client = client or self.client
        return active_client.post(
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
        self.assertEqual(create_response.json()["owner_username"], "admin")

        properties_response = self.client.get("/properties?month=2026-03")
        self.assertEqual(properties_response.status_code, 200)
        self.assertEqual(len(properties_response.json()), 1)

        dashboard_response = self.client.get("/dashboard?month=2026-03")
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertEqual(dashboard_response.json()["metrics"]["expected"], 12000.0)

    def test_users_only_see_their_own_properties(self):
        admin_client = TestClient(self.main.app)
        alice_client = TestClient(self.main.app)

        self.login(client=admin_client)
        self.login(username="alice", password="alicepass", client=alice_client)

        admin_create = self.create_property(tenant_name="Admin Tenant", property_name="Flat A", client=admin_client)
        alice_create = self.create_property(tenant_name="Alice Tenant", property_name="Flat B", client=alice_client)
        self.assertEqual(admin_create.status_code, 200)
        self.assertEqual(alice_create.status_code, 200)

        admin_property_id = admin_create.json()["id"]
        alice_property_id = alice_create.json()["id"]

        admin_properties = admin_client.get("/properties?month=2026-03")
        alice_properties = alice_client.get("/properties?month=2026-03")
        self.assertEqual([item["tenant_name"] for item in admin_properties.json()], ["Admin Tenant"])
        self.assertEqual([item["tenant_name"] for item in alice_properties.json()], ["Alice Tenant"])

        admin_dashboard = admin_client.get("/dashboard?month=2026-03")
        alice_dashboard = alice_client.get("/dashboard?month=2026-03")
        self.assertEqual(admin_dashboard.json()["metrics"]["expected"], 12000.0)
        self.assertEqual(alice_dashboard.json()["metrics"]["expected"], 12000.0)

        forbidden_ledger = alice_client.get(f"/properties/{admin_property_id}/ledger?month=2026-03")
        self.assertEqual(forbidden_ledger.status_code, 404)

        forbidden_delete = admin_client.delete(f"/properties/{alice_property_id}")
        self.assertEqual(forbidden_delete.status_code, 404)

    def test_whatsapp_message_routes_to_user_by_whatsapp_number(self):
        alice_client = TestClient(self.main.app)
        self.login(username="alice", password="alicepass", client=alice_client)
        create_response = self.create_property(tenant_name="John", property_name="Alice Flat", client=alice_client)
        self.assertEqual(create_response.status_code, 200)

        webhook_response = self.client.post(
            "/webhooks/whatsapp",
            data={
                "Body": "Rs. 12000.00 credited on 29-Mar-26 by NEFT-ABC123-JOHN",
                "WaId": "919999000002",
                "ProfileName": "Alice",
            },
        )
        self.assertEqual(webhook_response.status_code, 200)
        self.assertIn("Reply with the tenant number to match", webhook_response.text)

        with self.main.get_connection() as conn:
            unmatched_row = conn.fetch_one("SELECT * FROM unmatched_payments ORDER BY id DESC LIMIT 1")
            self.assertIsNotNone(unmatched_row)
            self.assertEqual(unmatched_row["owner_username"], "alice")
            raw_candidates = unmatched_row["candidates_json"]
            candidates = json.loads(raw_candidates) if isinstance(raw_candidates, str) else raw_candidates
            self.assertEqual([item["owner_username"] for item in candidates], ["alice"])

    def test_whatsapp_message_rejects_unknown_sender_number(self):
        webhook_response = self.client.post(
            "/webhooks/whatsapp",
            data={
                "Body": "Rs. 12000.00 credited on 29-Mar-26 by NEFT-ABC123-JOHN",
                "WaId": "919999000099",
                "ProfileName": "Unknown",
            },
        )
        self.assertEqual(webhook_response.status_code, 200)
        self.assertIn("not linked to any app user", webhook_response.text)

    def test_send_whatsapp_onboarding_uses_linked_number(self):
        self.login()
        with patch.object(self.main, "send_whatsapp_message", return_value="SM123") as mock_send:
            response = self.client.post("/whatsapp/onboarding", json={})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "sent")
        self.assertEqual(response.json()["to"], "whatsapp:+919999000001")
        self.assertEqual(response.json()["message"], "template_onboarding")
        mock_send.assert_called_once_with(
            "919999000001",
            content_sid="HXtest",
        )

    def test_first_login_attempts_onboarding_only_once(self):
        with patch.object(self.main, "send_first_login_onboarding", return_value="sent") as mock_onboarding:
            first_response = self.client.post(
                "/auth/login",
                json={"username": "admin", "password": "testpass"},
            )
            second_response = self.client.post(
                "/auth/login",
                json={"username": "admin", "password": "testpass"},
            )

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertTrue(first_response.json()["first_login"])
        self.assertFalse(second_response.json()["first_login"])
        self.assertEqual(first_response.json()["whatsapp_onboarding_status"], "sent")
        self.assertEqual(second_response.json()["whatsapp_onboarding_status"], "not_attempted")
        mock_onboarding.assert_called_once_with("admin")

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
