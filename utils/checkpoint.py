"""
MongoDB checkpoint for resumable backfill.
Same pattern as scrape-manuals-for-inventory.
"""
import os
from datetime import datetime
from typing import Any, Optional

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "checkpoint_db")

# Will be set by init
_client = None
_db = None


def get_checkpoint_collection(collection_name: str):
    """Get MongoDB collection for checkpoint."""
    global _client, _db
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        _db = _client[MONGO_DB_NAME]
    return _db[collection_name]


def load_checkpoint(
    collection_name: str,
    checkpoint_id: str,
    default: dict,
) -> dict:
    """Load checkpoint from MongoDB. Creates with default if missing."""
    col = get_checkpoint_collection(collection_name)
    doc = col.find_one({"_id": checkpoint_id})
    if not doc:
        doc = {"_id": checkpoint_id, **default}
        col.insert_one(doc)
        return dict(default)
    # Return only checkpoint fields; normalize types (MongoDB may return int32/float for numbers)
    out = {}
    for k in default:
        v = doc.get(k, default.get(k))
        if isinstance(default.get(k), int) and v is not None and not isinstance(v, bool):
            try:
                out[k] = int(v)
            except (TypeError, ValueError):
                out[k] = default.get(k)
        else:
            out[k] = v
    return out


def save_checkpoint(
    collection_name: str,
    checkpoint_id: str,
    data: dict,
) -> None:
    """Save checkpoint to MongoDB."""
    col = get_checkpoint_collection(collection_name)
    doc = {
        "_id": checkpoint_id,
        **data,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    col.replace_one({"_id": checkpoint_id}, doc, upsert=True)


def close_checkpoint_client() -> None:
    """Close MongoDB client."""
    global _client
    if _client:
        try:
            _client.close()
        except Exception:
            pass
        _client = None
