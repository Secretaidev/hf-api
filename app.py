import os
import duckdb
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="My Database API")

# CORS (frontend se call karne ke liye zaroori)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

con = duckdb.connect()

con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

con.execute(f"""
CREATE SECRET hf (
    TYPE s3,
    KEY_ID '{ACCESS_KEY}',
    SECRET '{SECRET_KEY}',
    ENDPOINT 's3.hf.co/adityasinghlko000',
    URL_STYLE 'path',
    REGION 'us-east-1'
)
""")

PARQUET_FILES = [
    "s3://my-db/part1.parquet",
    "s3://my-db/part2a.parquet",
    "s3://my-db/part2b_new.parquet",
]

COLUMNS = [
    "name",
    "fathersName",
    "phoneNumber",
    "aadharNumber",
    "otherNumber",
    "address",
    "district",
    "pincode",
    "state",
    "town",
]


@app.get("/")
def root():
    return {
        "status": "online",
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
    q_safe = q.replace("'", "''")

    where_clause = " OR ".join(
        f"lower(CAST({col} AS VARCHAR)) LIKE lower('%{q_safe}%')"
        for col in COLUMNS
    )

    sql = f"""
    SELECT {", ".join(COLUMNS)}
    FROM read_parquet({PARQUET_FILES}, union_by_name=true)
    WHERE {where_clause}
    LIMIT {limit}
    """

    rows = con.execute(sql).fetchall()

    return {
        "query": q,
        "count": len(rows),
        "owner": "@its_Secretz",
        "results": [dict(zip(COLUMNS, row)) for row in rows],
    }