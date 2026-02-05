"""
Typesense client for products_search collection.
"""
import os
from typing import Any

try:
    import typesense
except ImportError:
    typesense = None

TYPESENSE_COLLECTION = "products_search"
_client = None


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
