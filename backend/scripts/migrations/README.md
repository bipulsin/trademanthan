# Database migrations

Run SQL files against the **PostgreSQL** `trademanthan` database (adjust connection as needed).

Example:

```bash
psql "postgresql://USER:PASS@HOST:5432/trademanthan" -f backend/scripts/migrations/001_users_admin_columns.sql
```

After adding columns, restart the API so SQLAlchemy picks up the schema.
