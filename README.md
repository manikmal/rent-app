# Rent Management MVP

Minimal rent management system with lease parsing, rent tracking, payment matching, and manual correction.

## Stack

- FastAPI backend
- React frontend
- SQLite persisted in Docker volume mounted at `/data`
- Ollama with `llama3`
- `pdfplumber` for PDF extraction

## Run

1. Make sure Ollama is running locally and the `llama3` model is available:

   ```bash
   ollama pull llama3
   ollama serve
   ```

   The backend expects Ollama on `http://host.docker.internal:11434` from inside Docker.
   On Linux, the compose file maps `host.docker.internal` to the Docker host with `host-gateway`.

2. Start the app:

   ```bash
   ./scripts/docker-up.sh
   ```

3. Open:

   - Frontend: http://localhost:3000
   - Backend docs: http://localhost:8000/docs

## API Summary

- `POST /upload-lease`
- `GET /properties`
- `GET /unmatched-payments`
- `POST /process-payment`
- `POST /manual-match`

## Notes

- `tenant_name` and `rent_amount` are provided manually during lease upload.
- Lease metadata is extracted from the PDF text through Ollama.
- The backend accepts either `OLLAMA_URL=http://host.docker.internal:11434` or a full endpoint such as `http://host.docker.internal:11434/api/generate`.
- Payment message parsing uses regex only for amount, date, and tenant key extraction.
- Scanned/image-only PDFs are not OCR'd; only text that `pdfplumber` or Word parsers can read is sent to Ollama.
- If plain `docker compose up --build` fails with a `~/.docker/buildx` permission error on this machine, use `./scripts/docker-up.sh`. It uses a temporary Docker config and disables BuildKit for this project only, so no `sudo` is required.
