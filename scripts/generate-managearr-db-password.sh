#!/usr/bin/env bash
# One-time setup helper: generates a strong random password for the
# Managearr PostgreSQL user and writes it to secrets/managearr_db_password.txt
# (gitignored - see .gitignore). The password is never printed to stdout.
#
# Usage:
#   ./scripts/generate-managearr-db-password.sh
#   docker compose -f compose.managearr.yml up --build
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRET_DIR="$REPO_ROOT/secrets"
SECRET_FILE="$SECRET_DIR/managearr_db_password.txt"

if [ -f "$SECRET_FILE" ]; then
  echo "Refusing to overwrite existing secret: $SECRET_FILE" >&2
  echo "Delete it yourself first if you really want to rotate the password." >&2
  exit 1
fi

mkdir -p "$SECRET_DIR"
umask 077

if command -v openssl >/dev/null 2>&1; then
  openssl rand -base64 32 >"$SECRET_FILE"
else
  # Fallback with no external dependency beyond /dev/urandom.
  head -c 32 /dev/urandom | base64 >"$SECRET_FILE"
fi
chmod 600 "$SECRET_FILE"

echo "Generated a new database password at $SECRET_FILE (value not printed)."
echo "Compose reads it via POSTGRES_PASSWORD_FILE / MANAGEARR_DB_PASSWORD_FILE."
