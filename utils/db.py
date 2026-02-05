"""
PostgreSQL connection for products table (Supabase-backed).
Same pattern as scrape-manuals-for-inventory: single connection, ensure_db_alive.
"""
import os
from typing import List, Optional, Any

import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONNECTION_TIMEOUT = int(os.getenv("DB_CONNECTION_TIMEOUT", "10"))

_conn = None


def get_db_connection():
    """Get PostgreSQL connection to products database."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
        connect_timeout=DB_CONNECTION_TIMEOUT,
    )


def ensure_db_alive():
    """Reconnect if connection is closed or broken."""
    global _conn
    if _conn is None:
        _conn = get_db_connection()
        return
    try:
        if _conn.closed:
            raise psycopg2.InterfaceError("connection closed")
        with _conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        try:
            if _conn and not _conn.closed:
                _conn.close()
        except Exception:
            pass
        _conn = get_db_connection()


def get_conn():
    """Return the global connection (call ensure_db_alive before use)."""
    global _conn
    if _conn is None:
        _conn = get_db_connection()
    return _conn


def fetch_products_batch(
    last_id: Optional[str],
    batch_size: int,
    columns: str = "id, deleted_at",
) -> List[dict]:
    """Fetch a batch of products ordered by id (cursor-based)."""
    ensure_db_alive()
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if not last_id:
            cur.execute(
                f"""
                SELECT {columns}
                FROM public.products
                ORDER BY id
                LIMIT %s
                """,
                (batch_size,),
            )
        else:
            cur.execute(
                f"""
                SELECT {columns}
                FROM public.products
                WHERE id > %s
                ORDER BY id
                LIMIT %s
                """,
                (last_id, batch_size),
            )
        return cur.fetchall()


def update_products_deleted_at(conn, product_ids: List[str], value: int) -> int:
    """Set deleted_at = value for given product IDs. Returns row count."""
    if not product_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.products
            SET deleted_at = %s
            WHERE id = ANY(%s)
            """,
            (value, product_ids),
        )
        return cur.rowcount


def commit(conn):
    """Commit connection."""
    conn.commit()


def rollback(conn):
    """Rollback connection."""
    conn.rollback()


def ids_exists_in_products(conn, ids: List[str]) -> set:
    """Return set of IDs that exist in public.products."""
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.products WHERE id = ANY(%s)",
            (ids,),
        )
        return {row[0] for row in cur.fetchall()}


def close_db():
    """Close database connection."""
    global _conn
    if _conn and not _conn.closed:
        try:
            _conn.close()
        except Exception:
            pass
    _conn = None
