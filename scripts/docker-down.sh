#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_TMP_CONFIG="${TMPDIR:-/tmp}/rent-app-docker-config"

mkdir -p "$DOCKER_TMP_CONFIG"

cd "$REPO_ROOT"
DOCKER_CONFIG="$DOCKER_TMP_CONFIG" DOCKER_BUILDKIT=0 docker compose down "$@"
