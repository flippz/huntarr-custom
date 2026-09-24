# secrets/

Holds file-based credentials mounted read-only into the Managearr
Compose stack (`compose.managearr.yml`). Nothing in this directory
except this file is committed to git - see the `secrets/` rule in
`.gitignore`.

## Managearr PostgreSQL password

Before first `docker compose -f compose.managearr.yml up`, generate the
database password:

```bash
./scripts/generate-managearr-db-password.sh
```

This writes `secrets/managearr_db_password.txt` (mode `0600`, never
printed to stdout). Both the `postgres` service
(`POSTGRES_PASSWORD_FILE`) and the `managearr` service
(`MANAGEARR_DB_PASSWORD_FILE`) read the same file, so PostgreSQL and the
app always agree on the credential without it ever appearing in the
compose file, an image layer, or process environment as plain text.

Losing this file after PostgreSQL has already initialized its data
volume means the password can no longer be proven to PostgreSQL from
the app; reset it with `ALTER USER managearr WITH PASSWORD '...'` inside
the running `postgres` container and update the secret file to match
(then restart `managearr`).
