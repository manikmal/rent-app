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

2. Start the app:

   ```bash
   docker-compose up --build
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
- Payment message parsing uses regex only for amount, date, and tenant key extraction.
