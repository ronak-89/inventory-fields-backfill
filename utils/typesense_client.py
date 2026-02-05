"""
Typesense client for products_search collection.
"""
import os
from typing import Any, List

try:
    import typesense
except ImportError:
    typesense = None

TYPESENSE_COLLECTION = "products_search"
_client = None

# Schema fields required for backfill (added via PATCH if missing)
BACKFILL_SCHEMA_FIELDS = [
    {
        "name": "created_at",
        "type": "int64",
        "optional": True,
        "facet": False,
        "index": True,
        "sort": True,
        "store": True,
    },
    {
        "name": "deleted_at",
        "type": "int64",
        "optional": True,
        "facet": False,
        "index": True,
        "sort": True,
        "store": True,
    },
]


def get_typesense_client():
    """Get or create Typesense client from env."""
    global _client
    if _client is None:
        if typesense is None:
            raise RuntimeError("typesense package not installed")
        _client = typesense.Client(
            {
                "nodes": [
                    {
                        "host": os.getenv("TYPESENSE_HOST", "localhost"),
                        "port": os.getenv("TYPESENSE_PORT", "8108"),
                        "protocol": os.getenv("TYPESENSE_PROTOCOL", "http"),
                    }
                ],
                "api_key": os.getenv("TYPESENSE_API_KEY", ""),
                "connection_timeout_seconds": int(os.getenv("TYPESENSE_CONNECTION_TIMEOUT", "10")),
            }
        )
    return _client


def update_document(doc_id: str, fields: dict) -> bool:
    """Update a single document in products_search. Returns True on success."""
    try:
        get_typesense_client().collections[TYPESENSE_COLLECTION].documents[doc_id].update(
            fields
        )
        return True
    except Exception:
        return False


def update_document_ignore_not_found(doc_id: str, fields: dict) -> bool:
    """Update document; return True if updated, False if not found or error."""
    try:
        get_typesense_client().collections[TYPESENSE_COLLECTION].documents[doc_id].update(
            fields
        )
        return True
    except typesense.exceptions.ObjectNotFound:
        return False
    except Exception:
        return False


def ensure_backfill_schema_fields() -> None:
    """
    Ensure products_search collection has created_at and deleted_at in schema.
    Adds them via PATCH if missing. Call once at script startup before backfill.
    """
    client = get_typesense_client()
    coll = client.collections[TYPESENSE_COLLECTION]
    try:
        schema = coll.retrieve()
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve Typesense collection schema: {e}") from e

    existing_names = {f.get("name") for f in schema.get("fields", [])}
    to_add: List[dict] = [f for f in BACKFILL_SCHEMA_FIELDS if f["name"] not in existing_names]
    if not to_add:
        return

    names_to_add = [f["name"] for f in to_add]
    try:
        coll.update({"fields": to_add})
        print(f"✅ Typesense schema: added fields {names_to_add}", flush=True)
    except Exception as e:
        raise RuntimeError(f"Failed to add schema fields {names_to_add}: {e}") from e
