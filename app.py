import os
import re
import threading
from functools import lru_cache

import duckdb
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOCAL_PARQUET = os.environ.get("LOCAL_PARQUET")
ACCESS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY    = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
DB_DIR  = os.environ.get("DB_DIR", "/data" if os.path.isdir("/data") else "/tmp")
DB_PATH = os.path.join(DB_DIR, "search.duckdb")
# Uvicorn workers ki ginti (pre-warm ke liye)
WORKERS = int(os.environ.get("WORKERS", "4"))

PARQUET_FILES = [LOCAL_PARQUET] if LOCAL_PARQUET else [
    "s3://my-db/part1.parquet",
    "s3://my-db/part2a.parquet",
    "s3://my-db/part2b_new.parquet",
]

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
# Thread-local read-only connection pool
# ---------------------------------------------------------------------------
_local = threading.local()
_ready = threading.Event()


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Har thread ka alag read-only connection. Build ke baad hi kaam karta hai."""
    if not hasattr(_local, "con"):
        _local.con = duckdb.connect(DB_PATH, read_only=True)
    return _local.con


# ---------------------------------------------------------------------------
# One-time DB build  (background thread)
# ---------------------------------------------------------------------------
def _build_db():
    w = duckdb.connect(DB_PATH)   # write connection — sirf yahan
    try:
        exists = w.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='data'"
        ).fetchone()[0]

        if not exists:
            if not LOCAL_PARQUET:
                w.execute("INSTALL httpfs"); w.execute("LOAD httpfs")
                w.execute(f"""
                CREATE OR REPLACE SECRET hf (
                    TYPE s3, KEY_ID '{ACCESS_KEY}', SECRET '{SECRET_KEY}',
                    ENDPOINT 's3.hf.co/adityasinghlko000',
                    URL_STYLE 'path', REGION 'us-east-1'
                )""")

            cols = ", ".join(f"CAST({c} AS VARCHAR) AS {c}" for c in COLUMNS)
            w.execute(
                f"CREATE TABLE data AS SELECT {cols} "
                f"FROM read_parquet({PARQUET_FILES}, union_by_name=true)"
            )
            # Numeric index — exact phone/aadhar/pincode lookups instant
            for c in NUM_COLS:
                w.execute(f"CREATE INDEX idx_{c} ON data({c})")

            w.execute("CHECKPOINT")

    finally:
        w.close()   # zaroori — is ke band hone ke baad hi read-only connections khulenge

    # --- Pre-warm: N threads banao, har ek ka connection cache ho jata hai ---
    def _warm(dummy):
        con = _get_conn()
        con.execute("SELECT 1").fetchone()      # connection initialize karo
        _run_search("test", 1)                  # query planner bhi warm karo

    threads = [threading.Thread(target=_warm, args=(i,)) for i in range(WORKERS)]
    for t in threads: t.start()
    for t in threads: t.join()

    _ready.set()


threading.Thread(target=_build_db, daemon=True).start()


# ---------------------------------------------------------------------------
# Cached search — LRU 8192 entries
# ---------------------------------------------------------------------------
@lru_cache(maxsize=8192)
def _run_search(q: str, limit: int) -> tuple:
    con = _get_conn()

    if re.fullmatch(r"\d+", q):
        # Numeric: har column alag — index use hota hai (OR me index skip ho jata)
        out: list = []
        for col in NUM_COLS:
            rows = con.execute(
                f"SELECT {', '.join(COLUMNS)} FROM data WHERE {col} = ? LIMIT ?",
                [q, limit],
            ).fetchall()
            out.extend(rows)
            if len(out) >= limit:
                break
        return tuple(out[:limit])

    else:
        # Text: sirf text columns pe ILIKE (10 cols se 6 cols — kaafi faster)
        cond = " OR ".join(f"{c} ILIKE ?" for c in TEXT_COLS)
        rows = con.execute(
            f"SELECT {', '.join(COLUMNS)} FROM data WHERE {cond} LIMIT ?",
            [f"%{q}%"] * len(TEXT_COLS) + [limit],
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
    rows = _run_search(q, limit)
    return {
        "query": q,
        "count": len(rows),
        "owner": "@its_Secretz",
        "results": [dict(zip(COLUMNS, row)) for row in rows],
    }