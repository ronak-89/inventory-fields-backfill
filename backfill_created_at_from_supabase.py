"""
Standalone backfill script: sync created_at from Supabase (PostgreSQL) to Typesense.

- Phase 1: Cursor over public.products (id, created_at); for each batch update Typesense.
- Phase 2: Paginate Typesense; for docs not in Postgres set created_at = 0.
- MongoDB checkpoint for resumability (~3.5M records).
- Graceful shutdown (SIGINT/SIGTERM) saves checkpoint.

Deploy on any server; same structure as scrape-manuals-for-inventory.
"""

import os
import sys
import signal
import time
from datetime import datetime, timezone

# Add current directory for local utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

from utils.db import (
    ensure_db_alive,
    fetch_products_batch,
    get_conn,
    ids_exists_in_products,
    close_db,
)
from utils.checkpoint import (
    get_checkpoint_collection,
    load_checkpoint,
    save_checkpoint,
    close_checkpoint_client,
)
from utils.typesense_client import (
    get_typesense_client,
    TYPESENSE_COLLECTION,
    update_document_ignore_not_found,
    ensure_backfill_schema_fields,
)

# ================= CONFIG =================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
TYPESENSE_PAGE_SIZE = int(os.getenv("TYPESENSE_PAGE_SIZE", "250"))
SLEEP_BETWEEN_BATCHES = float(os.getenv("SLEEP_BETWEEN_BATCHES", "1.0"))

MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "checkpoint_db")
MONGO_COLLECTION = os.getenv("MONGO_CHECKPOINT_COLLECTION", "inventory_backfill_checkpoint")
CHECKPOINT_ID_CREATED_AT = "backfill_created_at"

DEFAULT_CREATED_AT = 0  # For Typesense docs not in Supabase
# ==========================================

def print_flush(*args, **kwargs):
    print(*args, **kwargs, flush=True)


def created_at_to_timestamp(value) -> int:
    """Convert Supabase created_at (str/datetime/int/float) to epoch int."""
    if value is None:
        return DEFAULT_CREATED_AT
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return DEFAULT_CREATED_AT
    if hasattr(value, "timestamp"):
        return int(value.timestamp())
    return DEFAULT_CREATED_AT


# ---------- CHECKPOINT SCHEMA ----------
CREATED_AT_CHECKPOINT_DEFAULT = {
    "phase": 1,  # 1 = sync from Supabase to Typesense, 2 = set default for Typesense-only
    "total_processed": 0,
    "updated_typesense": 0,
    "default_set_count": 0,
    "last_id": "",
    "last_page": 0,
    "batch_no": 0,
}


def load_state():
    return load_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID_CREATED_AT, CREATED_AT_CHECKPOINT_DEFAULT)


def save_state(state):
    save_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID_CREATED_AT, state)


# ---------- SIGNAL HANDLING ----------
shutdown_requested = False
_state = {}


def handle_signal(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    print_flush("\n🛑 Signal received — saving checkpoint and exiting...")
    try:
        save_state(_state)
    except Exception as e:
        print_flush(f"⚠️ Error saving checkpoint on shutdown: {e}")
    finally:
        cleanup()
        sys.exit(0)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# ---------- INIT ----------
def validate_env():
    required = [
        "DB_HOST", "DB_DATABASE", "DB_USER", "DB_PASSWORD", "DB_PORT",
        "MONGO_URI",
        "TYPESENSE_HOST", "TYPESENSE_PORT", "TYPESENSE_API_KEY",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise ValueError(f"Missing env: {', '.join(missing)}")


def init_connections():
    validate_env()
    ensure_db_alive()
    get_checkpoint_collection(MONGO_COLLECTION)
    get_typesense_client()
    # Ensure Typesense schema has created_at and deleted_at before backfill
    ensure_backfill_schema_fields()


def cleanup():
    close_db()
    close_checkpoint_client()
    print_flush("✅ Connections closed")


# ---------- PHASE 1: Supabase -> Typesense ----------
def run_phase1():
    """Cursor over products; update Typesense created_at for each batch."""
    state = load_state()
    last_id = state.get("last_id", "")
    total_processed = int(state.get("total_processed") or 0)
    updated_typesense = int(state.get("updated_typesense") or 0)
    batch_no = int(state.get("batch_no") or 0)

    _state["phase"] = 1
    _state["total_processed"] = total_processed
    _state["updated_typesense"] = updated_typesense
    _state["default_set_count"] = int(state.get("default_set_count") or 0)
    _state["last_id"] = last_id
    _state["last_page"] = 0
    _state["batch_no"] = batch_no

    if last_id:
        print_flush(
            f"📂 Phase 1 resumed from checkpoint | last_id={last_id[:8]}... | "
            f"total_processed={total_processed} | batch_no={batch_no}"
        )

    while True:
        if shutdown_requested:
            return
        print_flush(f"🔍 Phase 1: Fetching batch (cursor id > {last_id or 'start'})...")
        try:
            rows = fetch_products_batch(last_id, BATCH_SIZE, "id, created_at")
        except Exception as e:
            print_flush(f"❌ Fetch failed: {e}")
            time.sleep(5)
            continue

        if not rows:
            print_flush("✅ Phase 1 complete — no more products")
            _state["phase"] = 2
            _state["last_page"] = 0
            save_state(_state)
            return

        last_row = rows[-1]
        last_id = last_row["id"]

        updated = 0
        for r in rows:
            if shutdown_requested:
                break
            pid = r["id"]
            created_at = r.get("created_at")
            ts = created_at_to_timestamp(created_at)
            if update_document_ignore_not_found(pid, {"created_at": ts}):
                updated += 1

        batch_no += 1
        total_processed += len(rows)
        updated_typesense += updated
        _state["total_processed"] = total_processed
        _state["updated_typesense"] = updated_typesense
        _state["last_id"] = last_id
        _state["batch_no"] = batch_no
        save_state(_state)

        print_flush(
            f"📦 Phase 1 Batch #{batch_no} | rows={len(rows)} | "
            f"updated_typesense={updated} | total_processed={total_processed}"
        )
        time.sleep(SLEEP_BETWEEN_BATCHES)


# ---------- PHASE 2: Typesense-only docs -> created_at = 0 ----------
def run_phase2():
    """Paginate Typesense; for docs not in Postgres set created_at = 0."""
    state = load_state()
    page = int(state.get("last_page") or 0)
    default_set_count = int(state.get("default_set_count") or 0)
    batch_no = int(state.get("batch_no") or 0)

    _state["phase"] = 2
    _state["total_processed"] = int(state.get("total_processed") or 0)
    _state["updated_typesense"] = int(state.get("updated_typesense") or 0)
    _state["default_set_count"] = default_set_count
    _state["last_id"] = state.get("last_id", "")
    _state["last_page"] = page
    _state["batch_no"] = batch_no

    if page > 0:
        print_flush(
            f"📂 Phase 2 resumed from checkpoint | page={page} | "
            f"default_set_count={default_set_count} | batch_no={batch_no}"
        )

    ts_client = get_typesense_client()
    while True:
        if shutdown_requested:
            return
        try:
            # id cannot be used in query_by; use a searchable field to paginate (q=* matches all)
            result = ts_client.collections[TYPESENSE_COLLECTION].documents.search(
                {
                    "q": "*",
                    "query_by": "product_name",
                    "per_page": TYPESENSE_PAGE_SIZE,
                    "page": page,
                }
            )
        except Exception as e:
            print_flush(f"❌ Typesense search failed: {e}")
            time.sleep(5)
            continue

        hits = result.get("hits", [])
        if not hits:
            print_flush("✅ Phase 2 complete — no more Typesense pages")
            return

        doc_ids = []
        for hit in hits:
            doc = hit.get("document", {})
            doc_id = doc.get("id")
            if doc_id:
                doc_ids.append(doc_id)

        if doc_ids:
            conn = get_conn()
            exists = ids_exists_in_products(conn, doc_ids)
            for pid in doc_ids:
                if pid not in exists:
                    if update_document_ignore_not_found(pid, {"created_at": DEFAULT_CREATED_AT}):
                        default_set_count += 1

        batch_no += 1
        _state["default_set_count"] = default_set_count
        # Save next page so resume starts from there (no re-process of this page)
        _state["last_page"] = page + 1
        _state["batch_no"] = batch_no
        save_state(_state)

        print_flush(
            f"📦 Phase 2 Batch #{batch_no} | page={page} | "
            f"docs={len(doc_ids)} | default_set={default_set_count}"
        )
        page += 1
        time.sleep(SLEEP_BETWEEN_BATCHES)


# ---------- MAIN ----------
def run():
    print_flush("🚀 Backfill created_at: Supabase → Typesense (cursor-based, resumable)")
    try:
        init_connections()
        state = load_state()
        phase = state.get("phase", 1)

        if phase == 1:
            run_phase1()
        if not shutdown_requested and (phase == 2 or _state.get("phase") == 2):
            run_phase2()

        print_flush(
            f"🎉 Done | total_processed={_state.get('total_processed', 0)} | "
            f"updated_typesense={_state.get('updated_typesense', 0)} | "
            f"default_set={_state.get('default_set_count', 0)}"
        )
    except KeyboardInterrupt:
        print_flush("\n⚠️ Interrupted by user")
    except Exception as e:
        print_flush(f"❌ Fatal error: {e}")
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    run()
