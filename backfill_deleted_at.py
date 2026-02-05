"""
Standalone backfill script: set deleted_at = 0 for all existing products.

- Uses cursor-based pagination over public.products (id ORDER BY id).
- Updates both PostgreSQL (Supabase) and Typesense in batches.
- MongoDB checkpoint for resumability (~3.5M records).
- Graceful shutdown (SIGINT/SIGTERM) saves checkpoint.

Deploy on any server; run via Docker or python. Same structure as scrape-manuals-for-inventory.
"""

import os
import sys
import signal
import time
from datetime import datetime

# Add current directory for local utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

from utils.db import (
    ensure_db_alive,
    fetch_products_batch,
    get_conn,
    update_products_deleted_at,
    commit,
    rollback,
    close_db,
)
from utils.checkpoint import (
    get_checkpoint_collection,
    load_checkpoint,
    save_checkpoint,
    close_checkpoint_client,
)
from utils.typesense_client import update_document_ignore_not_found, ensure_backfill_schema_fields

# ================= CONFIG =================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
SLEEP_BETWEEN_BATCHES = float(os.getenv("SLEEP_BETWEEN_BATCHES", "1.0"))

MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "checkpoint_db")
MONGO_COLLECTION = os.getenv("MONGO_CHECKPOINT_COLLECTION", "inventory_backfill_checkpoint")
CHECKPOINT_ID_DELETED_AT = "backfill_deleted_at"

DEFAULT_DELETED_AT = 0  # Not deleted
# ==========================================

def print_flush(*args, **kwargs):
    print(*args, **kwargs, flush=True)


# ---------- CHECKPOINT SCHEMA ----------
DELETED_AT_CHECKPOINT_DEFAULT = {
    "total_processed": 0,
    "updated_supabase": 0,
    "updated_typesense": 0,
    "last_id": "",
    "batch_no": 0,
}


def load_state():
    return load_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID_DELETED_AT, DELETED_AT_CHECKPOINT_DEFAULT)


def save_state(state):
    save_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID_DELETED_AT, state)


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
    # Ensure Typesense schema has created_at and deleted_at before backfill
    ensure_backfill_schema_fields()


def cleanup():
    close_db()
    close_checkpoint_client()
    print_flush("✅ Connections closed")


# ---------- PROCESS BATCH ----------
def process_batch(rows, state):
    """Update deleted_at=0 for rows that have deleted_at IS NULL. Update Supabase + Typesense."""
    to_update = [r["id"] for r in rows if r.get("deleted_at") is None]
    if not to_update:
        return 0, 0, state

    conn = get_conn()
    updated_supabase = 0
    updated_typesense = 0
    try:
        updated_supabase = update_products_deleted_at(conn, to_update, DEFAULT_DELETED_AT)
        commit(conn)
    except Exception as e:
        print_flush(f"⚠️ Supabase batch update error: {e}")
        rollback(conn)
        return 0, 0, state

    for pid in to_update:
        if shutdown_requested:
            break
        if update_document_ignore_not_found(pid, {"deleted_at": DEFAULT_DELETED_AT}):
            updated_typesense += 1

    state["total_processed"] = state.get("total_processed", 0) + len(rows)
    state["updated_supabase"] = state.get("updated_supabase", 0) + updated_supabase
    state["updated_typesense"] = state.get("updated_typesense", 0) + updated_typesense
    return updated_supabase, updated_typesense, state


# ---------- MAIN ----------
def run():
    print_flush("🚀 Backfill deleted_at = 0 (cursor-based, resumable)")
    try:
        init_connections()
        state = load_state()
        last_id = state.get("last_id", "")
        total_processed = int(state.get("total_processed") or 0)
        updated_supabase = int(state.get("updated_supabase") or 0)
        updated_typesense = int(state.get("updated_typesense") or 0)
        batch_no = int(state.get("batch_no") or 0)

        _state["total_processed"] = total_processed
        _state["updated_supabase"] = updated_supabase
        _state["updated_typesense"] = updated_typesense
        _state["last_id"] = last_id
        _state["batch_no"] = batch_no

        if last_id:
            print_flush(
                f"📂 Resumed from checkpoint | last_id={last_id[:8]}... | "
                f"total_processed={total_processed} | batch_no={batch_no}"
            )

        while True:
            if shutdown_requested:
                break
            print_flush(f"🔍 Fetching batch (cursor id > {last_id or 'start'})...")
            try:
                rows = fetch_products_batch(last_id, BATCH_SIZE, "id, deleted_at")
            except Exception as e:
                print_flush(f"❌ Fetch failed: {e}")
                time.sleep(5)
                continue

            if not rows:
                print_flush("✅ No more records — backfill complete")
                break

            batch_no += 1
            last_row = rows[-1]
            last_id = last_row["id"]

            u_sb, u_ts, state = process_batch(rows, dict(_state))
            _state.update(state)
            _state["last_id"] = last_id
            _state["batch_no"] = batch_no
            save_state(_state)

            print_flush(
                f"📦 Batch #{batch_no} | rows={len(rows)} | "
                f"updated_supabase={u_sb} | updated_typesense={u_ts} | "
                f"total_processed={_state['total_processed']}"
            )
            time.sleep(SLEEP_BETWEEN_BATCHES)

        print_flush(
            f"🎉 Done | total_processed={_state['total_processed']} | "
            f"updated_supabase={_state['updated_supabase']} | "
            f"updated_typesense={_state['updated_typesense']}"
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
