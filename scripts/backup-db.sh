#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${REPO_ROOT}/backups"
TIMESTAMP="$(date +"%Y%m%d-%H%M%S")"
TARGET_FILE="${BACKUP_DIR}/rent-management-${TIMESTAMP}.sql"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
fi

POSTGRES_DB="${POSTGRES_DB:-rent_app}"
POSTGRES_USER="${POSTGRES_USER:-rent_app}"

mkdir -p "${BACKUP_DIR}"

echo "Creating backup at ${TARGET_FILE}"
docker exec rent-postgres pg_dump -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" > "${TARGET_FILE}"
echo "Backup complete."
