# Self-Hosting On Your Own Machine

This app can be hosted on a Mac now and moved to a Windows laptop later.

The stack is already set up for this:

- `caddy` handles public HTTP/HTTPS
- `frontend` serves the production React build through Nginx
- `backend` serves FastAPI
- `postgres` stores data locally in a Docker volume

## Recommended Setup

Use:

- your own domain or subdomain
- router port forwarding for `80` and `443`
- Caddy for automatic HTTPS
- Docker Compose to keep the stack together

This is a practical setup for a low-traffic personal or internal app.

## What You Need

1. A domain or subdomain
2. Your public home IP address
3. Router port forwarding:
   - external `80` -> your Mac or Windows laptop `80`
   - external `443` -> your Mac or Windows laptop `443`
4. A machine that stays awake and online

## DNS

Point your domain or subdomain to your home public IP.

Examples:

- `rent.yourdomain.com -> your public IP`
- `app.yourdomain.com -> your public IP`

If your home IP changes often, use dynamic DNS at your DNS provider and keep the DNS record updated.

## App Configuration

Copy `.env.example` to `.env` and set:

```bash
cp .env.example .env
```

Recommended values:

```env
DOMAIN=rent.yourdomain.com
SITE_ADDRESS=rent.yourdomain.com
POSTGRES_PASSWORD=change-this
APP_USERS=admin:change-this
APP_SESSION_SECRET=replace-with-a-long-random-secret
COOKIE_SECURE=true
ALLOW_ORIGINS=https://rent.yourdomain.com
```

## Start The Stack

```bash
./scripts/docker-up.sh -d
```

Verify:

- `https://rent.yourdomain.com`
- `https://rent.yourdomain.com/api/health`

## Temporary Tunnel Mode

If you are using a temporary tunnel before buying a domain:

```env
SITE_ADDRESS=http://:80
COOKIE_SECURE=false
ALLOW_ORIGINS=http://localhost,https://localhost,http://127.0.0.1
```

In this mode, Cloudflare or another tunnel provides the public HTTPS layer and Caddy only serves local HTTP.

## Twilio Webhook

Set the WhatsApp webhook to:

```text
https://rent.yourdomain.com/webhooks/whatsapp
```

## Mac Notes

- Disable sleep while hosting, or the app will go offline.
- In macOS firewall or router settings, allow inbound traffic to ports `80` and `443`.
- Docker Desktop must stay running.

## Windows Notes

When you move later to a Windows laptop:

- install Docker Desktop
- copy the repo and `.env`
- keep ports `80` and `443` forwarded to that laptop
- restart with the same `./scripts/docker-up.sh -d` flow from a compatible shell, or use `docker compose up --build -d`

Because the app now runs fully in Docker, the move from Mac to Windows should mostly be an environment move, not an app rewrite.

## Practical Caveats

- If the machine sleeps, the app goes down.
- If your public IP changes and DNS is stale, the app goes down.
- Home internet uptime is usually worse than a proper VM.
- This is suitable for light real use, but not high-availability production.
