import os
import re
import threading
import time
from functools import lru_cache

import duckdb
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

PARQUET_FILES = [
    "s3://my-db/part1.parquet",
    "s3://my-db/part2a.parquet",
    "s3://my-db/part2b_new.parquet",
]
PQ_LIST = str(PARQUET_FILES)  # DuckDB list syntax

COLUMNS   = ["name", "fathersName", "phoneNumber", "aadharNumber",
             "otherNumber", "address", "district", "pincode", "state", "town"]
NUM_COLS  = ["phoneNumber", "aadharNumber", "otherNumber", "pincode"]
TEXT_COLS = ["name", "fathersName", "address", "district", "state", "town"]

# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(title="My Database API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# DuckDB connection pool (thread-local)
# Har thread ka alag connection — parallel requests block nahi hote
# ---------------------------------------------------------------------------
_local = threading.local()
_ready = threading.Event()


def _make_conn() -> duckdb.DuckDBPyConnection:
    """Ek naya S3-ready DuckDB connection banao."""
    c = duckdb.connect()
    c.execute("INSTALL httpfs"); c.execute("LOAD httpfs")
    c.execute(f"""
    CREATE OR REPLACE SECRET hf (
        TYPE s3,
        KEY_ID '{ACCESS_KEY}',
        SECRET '{SECRET_KEY}',
        ENDPOINT 's3.hf.co/adityasinghlko000',
        URL_STYLE 'path',
        REGION 'us-east-1'
    )""")
    # S3 performance settings
    c.execute("SET threads TO 8")
    c.execute("SET s3_url_compatibility_mode = true")
    return c


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Thread-local connection laao, nahi hai to banao."""
    if not hasattr(_local, "con"):
        _local.con = _make_conn()
    return _local.con


# ---------------------------------------------------------------------------
# Startup: ek connection test karo, phir ready
# ---------------------------------------------------------------------------
def _startup():
    try:
        print("[startup] S3 connection test ho raha hai...")
        c = _make_conn()
        # Sirf ek chhota sa test — poora data load nahi
        row = c.execute(
            f"SELECT name, phoneNumber FROM read_parquet({PQ_LIST}, union_by_name=true) LIMIT 1"
        ).fetchone()
        print(f"[startup] Connection OK, sample row: {row}")
        # Main thread ka connection bhi set karo
        _local.con = c
        _ready.set()
        print("[startup] API ready!")
    except Exception as e:
        print(f"[startup] ERROR: {e}")


threading.Thread(target=_startup, daemon=True).start()


# ---------------------------------------------------------------------------
# Search — cached (8192 entries), S3 pe direct query
# ---------------------------------------------------------------------------
@lru_cache(maxsize=8192)
def _run_search(q: str, limit: int) -> tuple:
    con = _get_conn()
    cols_sql = ", ".join(COLUMNS)

    if re.fullmatch(r"\d+", q):
        # --- Numeric: exact match, har column alag ---
        # Parquet row-group statistics se DuckDB most files skip kar deta hai
        out: list = []
        for col in NUM_COLS:
            rows = con.execute(
                f"""SELECT {cols_sql}
                    FROM read_parquet({PQ_LIST}, union_by_name=true)
                    WHERE {col} = ?
                    LIMIT ?""",
                [q, limit],
            ).fetchall()
            out.extend(rows)
            if len(out) >= limit:
                break
        return tuple(out[:limit])

    else:
        # --- Text: ILIKE sirf text columns pe ---
        cond   = " OR ".join(f"{c} ILIKE ?" for c in TEXT_COLS)
        params = [f"%{q}%"] * len(TEXT_COLS) + [limit]
        rows = con.execute(
            f"""SELECT {cols_sql}
                FROM read_parquet({PQ_LIST}, union_by_name=true)
                WHERE {cond}
                LIMIT ?""",
            params,
        ).fetchall()
        return tuple(rows)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "status": "online" if _ready.is_set() else "loading",
        "owner": "@its_Secretz",
        "message": "API is running",
    }


@app.get("/health")
def health():
    return {"status": "healthy", "owner": "@its_Secretz"}


@app.get("/search")
def search(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
):
    if not _ready.is_set():
        return {"query": q, "count": 0, "status": "loading", "results": []}
    q = q.strip()
    t = time.time()
    rows = _run_search(q, limit)
    elapsed = round(time.time() - t, 3)
    return {
        "query": q,
        "count": len(rows),
        "time_s": elapsed,
        "owner": "@its_Secretz",
        "results": [dict(zip(COLUMNS, row)) for row in rows],
    }