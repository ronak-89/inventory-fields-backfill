# Inventory Fields Backfill

**Standalone backfill scripts** for the inventory `deleted_at` and `created_at` migration. Same structure as `scrape-manuals-for-inventory`: cursor-based batching, MongoDB checkpoint, and Docker so you can deploy on any server and run against ~3.5M records.

## Overview

- **`backfill_deleted_at.py`** — Sets `deleted_at = 0` for all existing products in PostgreSQL (Supabase) and Typesense. Cursor over `public.products` by `id`; batch updates both stores; resumable via MongoDB checkpoint.
- **`backfill_created_at_from_supabase.py`** — Syncs `created_at` from Supabase to Typesense. Phase 1: cursor over products, update Typesense per batch. Phase 2: paginate Typesense, set `created_at = 0` for docs not in Supabase. Resumable via checkpoint.

### Why update Supabase when `deleted_at` already exists (as NULL)?

In Supabase, existing rows have `deleted_at = NULL` for “not deleted.” We still backfill to `0` so that:

- **One semantics everywhere:** `0` = active, `>= 1` = deleted (timestamp). No mixed NULL vs 0.
- **One filter everywhere:** After backfill you can use `deleted_at < 1` in both Supabase and Typesense. If we left NULL, Supabase would need `(deleted_at IS NULL OR deleted_at < 1)` because in SQL `NULL < 1` is unknown, so NULL rows would be excluded by a plain `deleted_at < 1`.
- **Simpler app code:** Creates set `deleted_at = 0`; deletes set `deleted_at = timestamp`; list/search uses `deleted_at < 1` only.

So the backfill normalizes “no value” (NULL) into the canonical “active” value (0) once; after that, no NULL handling is needed.

### Flow after backfill

1. **Backfill** — Run `backfill_deleted_at.py` (then `backfill_created_at_from_supabase.py`).
2. **App code** — In list/count/search: filter with `deleted_at < 1` (Supabase and Typesense). On create: set `deleted_at = 0`. On delete: set `deleted_at = <timestamp>` (soft delete). No NULL checks.
3. **Ongoing** — New rows get `deleted_at = 0`; deleted rows get `deleted_at >= 1`. No further backfill needed.

Both scripts:

- Use **cursor-based pagination** (no full-table load).
- Save progress in **MongoDB** (checkpoint) so you can stop/restart or run on another server.
- Handle **SIGINT/SIGTERM** (save checkpoint and exit).
- Are **self-contained** in this folder (no dependency on parent monorepo at runtime).

## Structure

```
inventory-fields-backfill/
├── backfill_deleted_at.py              # Backfill deleted_at = 0
├── backfill_created_at_from_supabase.py # Backfill created_at (Supabase → Typesense)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .env.example
├── .gitignore
└── utils/
    ├── __init__.py
    ├── checkpoint.py    # MongoDB checkpoint load/save
    ├── db.py            # PostgreSQL connection + batch fetch/update
    └── typesense_client.py  # Typesense client + document update
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

**Run only deleted_at backfill (default):**

```bash
docker-compose up -d --build backfill-deleted-at
docker-compose logs -f backfill-deleted-at
```

**Run only created_at backfill (e.g. on another server):**

```bash
docker-compose --profile created-at up -d --build backfill-created-at
docker-compose --profile created-at logs -f backfill-created-at
```

### 4. Run with Docker (single script)

```bash
# Build once
docker build -t inventory-fields-backfill:latest .

# Run deleted_at backfill
docker run -d --name backfill-deleted-at --env-file .env --restart unless-stopped inventory-fields-backfill:latest

# Run created_at backfill (different container)
docker run -d --name backfill-created-at --env-file .env --restart unless-stopped inventory-fields-backfill:latest python backfill_created_at_from_supabase.py
```

### 5. Run locally (no Docker)

```bash
pip install -r requirements.txt
export $(cat .env | xargs)   # or use dotenv via .env
python backfill_deleted_at.py
# or
python backfill_created_at_from_supabase.py
```

## Checkpoints (MongoDB)

- **Collection:** `MONGO_CHECKPOINT_COLLECTION` (default: `inventory_backfill_checkpoint`)
- **deleted_at:** document `_id`: `backfill_deleted_at` — stores `last_id`, `total_processed`, `updated_supabase`, `updated_typesense`
- **created_at:** document `_id`: `backfill_created_at` — stores `phase`, `last_id`, `last_page`, `total_processed`, `updated_typesense`, `default_set_count`

You can stop (or move to another server with same MongoDB and env); on next run the script resumes from the checkpoint.

## Order of execution

1. Run **`backfill_deleted_at.py`** first (so all products have `deleted_at = 0` in both stores).
2. Then run **`backfill_created_at_from_supabase.py`** (sync `created_at` to Typesense, then set default for Typesense-only docs).

You can run them on the same machine (one after the other) or on different servers, as long as they share the same DB, Typesense, and MongoDB checkpoint.

## Configuration for large data (~3.5M)

- Increase `BATCH_SIZE` (e.g. 2000–5000) if DB and Typesense can handle it.
- Keep `SLEEP_BETWEEN_BATCHES` to avoid overloading (e.g. 1.0–2.0).
- Ensure MongoDB and PostgreSQL timeouts and connection limits are sufficient for long-running jobs.

## Reference

- Migration design and field semantics: see repo docs **`docs/inventory_fields_migration_guide.md`** and **`docs/inventory_fields_migration_summary.md`**.
- Structure and deployment pattern: **`scrape-manuals-for-inventory/`** in this repo.
