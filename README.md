# Rent Management MVP

Small-production rent collection dashboard for a tiny team. The app is set up for a simple 3-user deployment with cookie-based login, a production frontend served by Nginx, a FastAPI backend, PostgreSQL persistence, Caddy HTTPS termination, and month-aware rent tracking.

## Features

- Tenant and property records with phone, unit, address, deposit, lease terms, and emergency contacts
- Monthly rent tracking with rent increase schedules
- Payment ledger per tenant
- Unmatched payment review inbox with approve, reject, duplicate, and undo actions
- Reminders and dashboard trends
- Cookie-based login for small-team access
- WhatsApp webhook flow for forwarding bank messages

## Minimal Production Setup

- `frontend` serves a built React app with Nginx
- `backend` runs FastAPI against PostgreSQL
- `caddy` serves the public site and handles HTTPS
- `postgres` stores app data on a persistent Docker volume
- Docker Compose runs the full stack on one Oracle VM

## Configure

Copy the example env file and set real values:

```bash
cp .env.example .env
```

Important variables:

- `DOMAIN`
  Your public domain or subdomain pointed at the Oracle VM
- `DATABASE_URL`
  PostgreSQL connection string used by the backend
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
  PostgreSQL container credentials
- `APP_USERS`
  Format: `user1:password1,user2:password2`
- `APP_SESSION_SECRET`
  Use a long random secret in production
- `COOKIE_SECURE`
  Set to `true` when running behind HTTPS
- `ALLOW_ORIGINS`
  Set this to your real production origin if you expose the backend directly

## Run

Start the stack:

```bash
./scripts/docker-up.sh -d
```

Open:

- Site: http://localhost
- Backend docs: http://localhost/api/docs

Stop the stack:

```bash
./scripts/docker-down.sh
```

## Default Local Login

If you do not provide `APP_USERS`, the app falls back to:

- Username: `admin`
- Password: `changeme`

Change that before any real deployment.

## Migration

If you have an existing SQLite database, migrate it into Postgres after the new stack is up:

```bash
python3 scripts/migrate_sqlite_to_postgres.py
```

Set `SQLITE_PATH` if your SQLite database lives somewhere else.

## Backups

For production on Oracle, use `pg_dump` against the `postgres` container or database and store the dump outside the VM.

## Core API

- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`
- `GET /dashboard?month=YYYY-MM`
- `GET /reminders?month=YYYY-MM`
- `GET /properties?month=YYYY-MM&q=&status=&sort=`
- `GET /properties/{property_id}/ledger?month=YYYY-MM`
- `POST /properties`
- `PUT /properties/{property_id}`
- `DELETE /properties/{property_id}`
- `GET /unmatched-payments?status=UNMATCHED`
- `POST /process-payment`
- `POST /manual-match`
- `POST /unmatched-payments/{id}/match`
- `POST /unmatched-payments/{id}/reject`
- `POST /unmatched-payments/{id}/duplicate`
- `POST /payments/{id}/undo`
- `POST /webhooks/whatsapp`

## Tests

Backend tests live in [backend/test_main.py](/Users/manikmalhotra/Documents/projects/rent-app/backend/test_main.py).

If you want to run them inside the backend image after build:

```bash
docker exec rent-backend python -m unittest /app/test_main.py
```

## Oracle Deployment

1. Create an Oracle Always Free VM and install Docker plus the Compose plugin.
2. Open VM ingress for ports `80` and `443`.
3. Point your domain to the VM public IP.
4. Copy `.env.example` to `.env` and set real values, especially `DOMAIN`, `POSTGRES_PASSWORD`, `APP_USERS`, `APP_SESSION_SECRET`, and `COOKIE_SECURE=true`.
5. Start the stack with `./scripts/docker-up.sh -d`.
6. Set Twilio WhatsApp sandbox webhook to:

```text
https://<your-domain>/webhooks/whatsapp
```

## Notes

- The frontend is built for production instead of running the React dev server.
- Backend API routes are exposed publicly through Caddy at `/api/*`.
- The WhatsApp webhook stays at `/webhooks/whatsapp` so Twilio does not need the `/api` prefix.
