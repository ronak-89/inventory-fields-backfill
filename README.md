# Inventory Fields Backfill

**Standalone backfill** for the inventory `deleted_at` and `created_at` migration. Same structure as `scrape-manuals-for-inventory`: cursor-based batching, MongoDB checkpoint, and Docker so you can deploy on any server and run against ~3.5M records.

## Overview

- **`backfill_inventory_fields.py`** — One script, **one pass** for both fields: (1) ensures Typesense schema has `created_at` and `deleted_at`, (2) **Phase 1:** cursor over products from Supabase — each batch updates Supabase (`deleted_at = 0` where null) and Typesense (`deleted_at = 0` and `created_at` from Supabase as Unix timestamp), (3) **Phase 2:** documents that exist only in Typesense (not in Supabase) get `created_at = 0` and `deleted_at = 0` set manually. Single checkpoint; resumable.

### Field semantics

- **`deleted_at`** — Set to `0` everywhere: in Supabase where it is null (Phase 1), and in Typesense for every product (Phase 1) and for Typesense-only docs (Phase 2). After backfill, `0` = active, `>= 1` = deleted (timestamp); no NULLs.

- **`created_at`** — **Taken from Supabase** for products that exist there: Phase 1 reads `created_at` from `public.products` and syncs it to Typesense as a Unix timestamp. For documents that exist **only in Typesense** (no row in Supabase), we **manually set** `created_at = 0` in Phase 2 so every Typesense document has a defined value for filtering.

### Why update Supabase when `deleted_at` already exists (as NULL)?

In Supabase, existing rows have `deleted_at = NULL` for “not deleted.” We still backfill to `0` so that:

- **One semantics everywhere:** `0` = active, `>= 1` = deleted (timestamp). No mixed NULL vs 0.
- **One filter everywhere:** After backfill you can use `deleted_at < 1` in both Supabase and Typesense. If we left NULL, Supabase would need `(deleted_at IS NULL OR deleted_at < 1)` because in SQL `NULL < 1` is unknown, so NULL rows would be excluded by a plain `deleted_at < 1`.
- **Simpler app code:** Creates set `deleted_at = 0`; deletes set `deleted_at = timestamp`; list/search uses `deleted_at < 1` only.

So the backfill normalizes “no value” (NULL) into the canonical “active” value (0) once; after that, no NULL handling is needed.

### Flow after backfill

1. **Backfill** — Run `backfill_inventory_fields.py` once.
2. **App code** — In list/count/search: filter with `deleted_at < 1` (Supabase and Typesense). On create: set `deleted_at = 0`. On delete: set `deleted_at = <timestamp>` (soft delete). No NULL checks.
3. **Ongoing** — New rows get `deleted_at = 0`; deleted rows get `deleted_at >= 1`. No further backfill needed.

The script uses **cursor-based pagination**, saves progress in **MongoDB** (checkpoint), handles **SIGINT/SIGTERM**, and is **self-contained** in this folder.

## Structure

```
inventory-fields-backfill/
├── backfill_inventory_fields.py   # Main script: schema + deleted_at + created_at
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .env.example
├── .gitignore
└── utils/
    ├── __init__.py
    ├── checkpoint.py
    ├── db.py
    └── typesense_client.py
```

## Prerequisites

1. **Environment** — Copy `.env.example` to `.env` and set:

   **Required:**

   - `DB_HOST`, `DB_DATABASE`, `DB_USER`, `DB_PASSWORD`, `DB_PORT` — PostgreSQL (Supabase) for `public.products`
   - `MONGO_URI` — MongoDB for checkpoint
   - `TYPESENSE_HOST`, `TYPESENSE_PORT`, `TYPESENSE_API_KEY` — Typesense (`products_search` collection)

   **Optional (defaults):**

   - `BATCH_SIZE` — Rows per batch (default: 1000)
   - `SLEEP_BETWEEN_BATCHES` — Seconds between batches (default: 1.0)
   - `TYPESENSE_PAGE_SIZE` — Typesense page size in phase 2 of created_at (default: 250)
   - `MONGO_DB_NAME`, `MONGO_CHECKPOINT_COLLECTION` — Checkpoint DB/collection name

2. **Docker** (optional) — For containerized run.

## Deployment (same as scrape-manuals-for-inventory)

### 1. Push to a repo (optional)

```bash
cd inventory-fields-backfill
git init
git add .
git commit -m "Inventory fields backfill"
git remote add origin <your-repo-url>
git push -u origin main
```

### 2. On the server

```bash
git clone <your-repo-url> inventory-fields-backfill
cd inventory-fields-backfill
cp .env.example .env
# Edit .env with real credentials
```

### 3. Run with Docker Compose

```bash
docker-compose up -d --build backfill-inventory-fields
docker-compose logs -f backfill-inventory-fields
```

### 4. Run with Docker

```bash
docker build -t inventory-fields-backfill:latest .
docker run -d --name backfill-fields --env-file .env --restart unless-stopped inventory-fields-backfill:latest
```

### 5. Run locally (no Docker)

```bash
pip install -r requirements.txt
export $(cat .env | xargs)   # or use dotenv via .env
python backfill_inventory_fields.py
```

## Checkpoints (MongoDB)

- **Collection:** `MONGO_CHECKPOINT_COLLECTION` (default: `inventory_backfill_checkpoint`)
- **Single document** `_id`: `backfill_inventory_fields` — stores `phase`, `last_id`, `last_page`, `batch_no`, `total_processed`, `updated_supabase`, `updated_typesense`, `default_set_count`

### Resume behavior

If the script stops (crash, kill, SIGTERM, etc.) and you run it again, it **resumes from where it stopped**:

- **Phase 1:** We save `last_id` after each batch; restart fetches `id > last_id`. No re-processing of the last batch.
- **Phase 2:** We save `last_page = page + 1`; restart starts from that page.

Checkpoint is written after each batch and on SIGINT/SIGTERM. Updates are idempotent. You can stop or move to another server (same MongoDB and env).

## Order of execution

**`backfill_inventory_fields.py`** — **Phase 1:** One cursor over products from Supabase; each batch sets `deleted_at = 0` in Supabase (where null) and in Typesense, and syncs `created_at` from Supabase to Typesense (as Unix timestamp). **Phase 2:** Documents that exist only in Typesense (not in Supabase) get `created_at = 0` and `deleted_at = 0` set manually. Resumable via one MongoDB checkpoint.

## Configuration for large data (~3.5M)

- Increase `BATCH_SIZE` (e.g. 2000–5000) if DB and Typesense can handle it.
- Keep `SLEEP_BETWEEN_BATCHES` to avoid overloading (e.g. 1.0–2.0).
- Ensure MongoDB and PostgreSQL timeouts and connection limits are sufficient for long-running jobs.

## Troubleshooting

### MongoDB SSL handshake failed (e.g. on staging / in Docker)

If you see `ServerSelectionTimeoutError: SSL handshake failed ... TLSV1_ALERT_INTERNAL_ERROR` when connecting to MongoDB Atlas from the container (while it works locally), the image was updated to install and refresh CA certificates. Rebuild and run:

```bash
docker compose build --no-cache backfill-inventory-fields
docker compose up -d backfill-inventory-fields
```

If it still fails, switch the Dockerfile base image from `python:3.11-slim` to `python:3.11` (full image) so the container uses the same OpenSSL and certs as a full Debian environment.

## Reference

- Migration design and field semantics: see repo docs **`docs/inventory_fields_migration_guide.md`** and **`docs/inventory_fields_migration_summary.md`**.
- Structure and deployment pattern: **`scrape-manuals-for-inventory/`** in this repo.
