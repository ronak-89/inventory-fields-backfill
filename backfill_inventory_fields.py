"""
Single-pass backfill: deleted_at and created_at together per batch.

- Ensures Typesense schema has created_at and deleted_at.
- Phase 1 (one pass): Cursor over products from Supabase. For each batch:
  - Supabase: set deleted_at=0 where null.
  - Typesense: set deleted_at=0 and created_at=<from Supabase created_at as timestamp>.
  So created_at is always taken from Supabase for products that exist there.
- Phase 2: Documents that exist in Typesense but NOT in Supabase get created_at=0 and deleted_at=0
  (manually set, since we have no Supabase row to copy from).

Resumable via MongoDB checkpoint. SIGINT/SIGTERM saves and exits.
"""

import os
import sys
import signal
import time
from datetime import datetime, timezone

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
    ids_exists_in_products,
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

MONGO_COLLECTION = os.getenv("MONGO_CHECKPOINT_COLLECTION", "inventory_backfill_checkpoint")
CHECKPOINT_ID = "backfill_inventory_fields"

DEFAULT_DELETED_AT = 0
DEFAULT_CREATED_AT = 0
# ==========================================

def print_flush(*args, **kwargs):
    print(*args, **kwargs, flush=True)


def created_at_to_timestamp(value) -> int:
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


# ---------- CHECKPOINT ----------
# phase 1 = combined deleted_at + created_at pass; phase 2 = Typesense-only created_at=0
CHECKPOINT_DEFAULT = {
    "phase": 1,
    "last_id": "",
    "last_page": 0,
    "batch_no": 0,
    "total_processed": 0,
    "updated_supabase": 0,
    "updated_typesense": 0,
    "default_set_count": 0,
}

current_step = "phase1"
_state = {}
shutdown_requested = False


def save_current_checkpoint():
    save_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID, _state)


def handle_signal(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    print_flush("\n🛑 Signal received — saving checkpoint and exiting...")
    try:
        save_current_checkpoint()
    except Exception as e:
        print_flush(f"⚠️ Error saving checkpoint: {e}")
    finally:
        cleanup()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


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
    ensure_backfill_schema_fields()


def cleanup():
    close_db()
    close_checkpoint_client()
    print_flush("✅ Connections closed")


# ---------- PHASE 1: One pass — deleted_at + created_at together ----------
def run_phase1():
    """One cursor over products from Supabase; each batch: Supabase deleted_at=0, Typesense deleted_at=0 and created_at from Supabase (as timestamp)."""
    global current_step, _state
    current_step = "phase1"
    state = load_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID, CHECKPOINT_DEFAULT)
    last_id = state.get("last_id", "")
    _state = {
        "phase": 1,
        "last_id": last_id,
        "last_page": 0,
        "batch_no": int(state.get("batch_no") or 0),
        "total_processed": int(state.get("total_processed") or 0),
        "updated_supabase": int(state.get("updated_supabase") or 0),
        "updated_typesense": int(state.get("updated_typesense") or 0),
        "default_set_count": int(state.get("default_set_count") or 0),
    }
    batch_no = _state["batch_no"]
    if last_id:
        print_flush(
            f"📂 [Phase 1] Resumed | last_id={last_id[:8]}... | batch_no={batch_no} | "
            f"total_processed={_state['total_processed']}"
        )

    while True:
        if shutdown_requested:
            return
        print_flush(f"🔍 [Phase 1] Fetching batch (cursor id > {last_id or 'start'})...")
        try:
            rows = fetch_products_batch(last_id, BATCH_SIZE, "id, deleted_at, created_at")
        except Exception as e:
            print_flush(f"❌ Fetch failed: {e}")
            time.sleep(5)
            continue
        if not rows:
            print_flush("✅ [Phase 1] Complete — no more products")
            _state["phase"] = 2
            _state["last_page"] = 0
            save_current_checkpoint()
            return

        last_id = rows[-1]["id"]
        to_update_supabase = [r["id"] for r in rows if r.get("deleted_at") is None]
        updated_supabase = 0
        updated_typesense = 0

        if to_update_supabase:
            conn = get_conn()
            try:
                updated_supabase = update_products_deleted_at(conn, to_update_supabase, DEFAULT_DELETED_AT)
                commit(conn)
            except Exception as e:
                print_flush(f"⚠️ Supabase batch update error: {e}")
                rollback(conn)

        for r in rows:
            if shutdown_requested:
                break
            pid = r["id"]
            ts = created_at_to_timestamp(r.get("created_at"))
            # Typesense: both deleted_at and created_at in one update
            if update_document_ignore_not_found(pid, {"deleted_at": DEFAULT_DELETED_AT, "created_at": ts}):
                updated_typesense += 1

        batch_no += 1
        _state["total_processed"] += len(rows)
        _state["updated_supabase"] += updated_supabase
        _state["updated_typesense"] += updated_typesense
        _state["last_id"] = last_id
        _state["batch_no"] = batch_no
        save_current_checkpoint()

        print_flush(
            f"📦 [Phase 1] Batch #{batch_no} | rows={len(rows)} | "
            f"updated_sb={updated_supabase} | updated_ts={updated_typesense} | total={_state['total_processed']} | "
            f"saved_cursor={last_id[:8]}..."
        )
        time.sleep(SLEEP_BETWEEN_BATCHES)


# ---------- PHASE 2: Typesense-only docs → created_at=0, deleted_at=0 ----------
def run_phase2():
    """Paginate Typesense; for docs not in Supabase set created_at=0 and deleted_at=0."""
    global current_step, _state
    current_step = "phase2"
    state = load_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID, CHECKPOINT_DEFAULT)
    page = int(state.get("last_page") or 0)
    default_set_count = int(state.get("default_set_count") or 0)
    batch_no = int(state.get("batch_no") or 0)
    _state = {
        "phase": 2,
        "last_id": state.get("last_id", ""),
        "last_page": page,
        "batch_no": batch_no,
        "total_processed": int(state.get("total_processed") or 0),
        "updated_supabase": int(state.get("updated_supabase") or 0),
        "updated_typesense": int(state.get("updated_typesense") or 0),
        "default_set_count": default_set_count,
    }
    if page > 0:
        print_flush(f"📂 [Phase 2] Resumed | page={page} | default_set={default_set_count} | batch_no={batch_no}")

    ts_client = get_typesense_client()
    while True:
        if shutdown_requested:
            return
        try:
            result = ts_client.collections[TYPESENSE_COLLECTION].documents.search(
                {"q": "*", "query_by": "product_name", "per_page": TYPESENSE_PAGE_SIZE, "page": page}
            )
        except Exception as e:
            print_flush(f"❌ Typesense search failed: {e}")
            time.sleep(5)
            continue
        hits = result.get("hits", [])
        if not hits:
            print_flush("✅ [Phase 2] Complete — no more pages")
            return
        doc_ids = [h.get("document", {}).get("id") for h in hits if h.get("document", {}).get("id")]
        if doc_ids:
            conn = get_conn()
            exists = ids_exists_in_products(conn, doc_ids)
            for pid in doc_ids:
                if pid not in exists and update_document_ignore_not_found(
                    pid, {"created_at": DEFAULT_CREATED_AT, "deleted_at": DEFAULT_DELETED_AT}
                ):
                    default_set_count += 1
        batch_no += 1
        _state["default_set_count"] = default_set_count
        _state["last_page"] = page + 1
        _state["batch_no"] = batch_no
        save_current_checkpoint()
        print_flush(
            f"📦 [Phase 2] Batch #{batch_no} | page={page} | docs={len(doc_ids)} | default_set={default_set_count}"
        )
        page += 1
        time.sleep(SLEEP_BETWEEN_BATCHES)


# ---------- MAIN ----------
def run():
    print_flush("🚀 Backfill inventory fields: deleted_at + created_at together (single pass), resumable")
    try:
        init_connections()
        state = load_checkpoint(MONGO_COLLECTION, CHECKPOINT_ID, CHECKPOINT_DEFAULT)
        phase = int(state.get("phase") or 1)
        if phase == 1:
            run_phase1()
        if not shutdown_requested and (phase == 2 or _state.get("phase") == 2):
            run_phase2()
        if not shutdown_requested:
            print_flush(
                "🎉 All done | total_processed={} | updated_sb={} | updated_ts={} | default_set={}".format(
                    _state.get("total_processed", 0),
                    _state.get("updated_supabase", 0),
                    _state.get("updated_typesense", 0),
                    _state.get("default_set_count", 0),
                )
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
