# db.py
import os
from contextlib import contextmanager
import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS qb_oauth_tokens (
            id SMALLINT PRIMARY KEY CHECK (id = 1),
            realm_id TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            refresh_token_issued_at TIMESTAMPTZ NOT NULL,
            access_token TEXT,
            access_token_expires_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id BIGSERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'running',
            total_invoices INT,
            succeeded INT NOT NULL DEFAULT 0,
            failed INT NOT NULL DEFAULT 0,
            log TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS upload_invoices (
            id BIGSERIAL PRIMARY KEY,
            upload_id BIGINT REFERENCES uploads(id) ON DELETE CASCADE,
            invoice_number TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            qb_id TEXT,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (invoice_number, transaction_type)
        );
        """)


def get_token_row():
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM qb_oauth_tokens WHERE id = 1")
        return cur.fetchone()


def upsert_token_row(*, realm_id, refresh_token, refresh_token_issued_at,
                     access_token=None, access_token_expires_at=None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO qb_oauth_tokens (id, realm_id, refresh_token,
                refresh_token_issued_at, access_token, access_token_expires_at)
            VALUES (1, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                realm_id = EXCLUDED.realm_id,
                refresh_token = EXCLUDED.refresh_token,
                refresh_token_issued_at = EXCLUDED.refresh_token_issued_at,
                access_token = COALESCE(EXCLUDED.access_token, qb_oauth_tokens.access_token),
                access_token_expires_at = COALESCE(EXCLUDED.access_token_expires_at,
                                                   qb_oauth_tokens.access_token_expires_at),
                updated_at = now()
        """, (realm_id, refresh_token, refresh_token_issued_at,
              access_token, access_token_expires_at))


def create_upload(filename):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("INSERT INTO uploads (filename) VALUES (%s) RETURNING id", (filename,))
        return cur.fetchone()["id"]


def finalize_upload(upload_id, *, status, total_invoices, succeeded, failed, log):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
            UPDATE uploads SET finished_at = now(), status = %s, total_invoices = %s,
                succeeded = %s, failed = %s, log = %s WHERE id = %s
        """, (status, total_invoices, succeeded, failed, log, upload_id))


def record_invoice(upload_id, invoice_number, transaction_type, status,
                   qb_id=None, error_message=None):
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO upload_invoices (upload_id, invoice_number, transaction_type,
                qb_id, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (invoice_number, transaction_type) DO UPDATE SET
                status = EXCLUDED.status,
                qb_id = COALESCE(EXCLUDED.qb_id, upload_invoices.qb_id),
                error_message = EXCLUDED.error_message
        """, (upload_id, invoice_number, transaction_type, qb_id, status, error_message))


def is_already_processed(invoice_number, transaction_type) -> bool:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM upload_invoices
            WHERE invoice_number = %s AND transaction_type = %s AND status = 'success'
        """, (invoice_number, transaction_type))
        return cur.fetchone() is not None