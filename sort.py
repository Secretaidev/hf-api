#!/usr/bin/env python3
import duckdb, time, os, sys, gc, shutil, threading

KEY = "HFAKfwV25funOIh8AuOKoLOPNpBfMGI"
SEC = "169d8dc685e022cd1531d023c38c9282919c1f7b1f13cf6ccf826e7257fdf09a"
S3_ENDPOINT = "s3.hf.co/adityasinghlko000"
BUCKET = "my-db"

COLS = "name, fathersName, phoneNumber, aadharNumber, otherNumber, address, district, pincode, state, town"
TEMP_DIR = "/root/dt"
LOG_FILE = "/root/sort.log"

JOBS = [
    ("/root/Downloads/part2b_new.parquet", "/root/Downloads/sorted_part2b.parquet", "sorted_part2b.parquet"),
    ("/root/Downloads/part2a.parquet",     "/root/Downloads/sorted_part2a.parquet",     "sorted_part2a.parquet"),
    ("/root/Downloads/part1.parquet",      "/root/Downloads/sorted_part1.parquet",      "sorted_part1.parquet"),
]

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs("/root/Downloads", exist_ok=True)

def get_cgroup_memory_mb():
    try:
        with open("/sys/fs/cgroup/memory.current", "r") as f:
            return int(f.read().strip()) / (1024 * 1024)
    except Exception:
        try:
            import resource
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        except Exception:
            return 0.0

def get_cgroup_memory_limit_mb():
    try:
        with open("/sys/fs/cgroup/memory.max", "r") as f:
            val = f.read().strip()
            if val != "max":
                return int(val) / (1024 * 1024)
    except Exception:
        pass
    return 953.6

def log(msg):
    ts = time.strftime("%H:%M:%S")
    mem = get_cgroup_memory_mb()
    limit = get_cgroup_memory_limit_mb()
    mem_str = f" [RAM: {mem:.1f}MB/{limit:.0f}MB]" if mem > 0 else ""
    line = f"[{ts}]{mem_str} {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

peak_ram = 0.0
monitoring = True

def memory_watcher():
    global peak_ram, monitoring
    while monitoring:
        current = get_cgroup_memory_mb()
        if current > peak_ram:
            peak_ram = current
        time.sleep(2)

def create_duckdb_sort_con():
    c = duckdb.connect()
    c.execute(f"SET temp_directory='{TEMP_DIR}'")
    c.execute("SET memory_limit='600MB'")
    c.execute("SET threads=2")
    c.execute("SET preserve_insertion_order=false")
    c.execute("SET max_temp_directory_size='800GB'")
    return c

def upload_to_s3(local_file, s3_key_name):
    log(f"Uploading {local_file} -> s3://{BUCKET}/{s3_key_name} ...")
    t0 = time.time()
    try:
        import boto3
        from botocore.client import Config
        from boto3.s3.transfer import TransferConfig

        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{S3_ENDPOINT}",
            aws_access_key_id=KEY,
            aws_secret_access_key=SEC,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, signature_version="s3v4")
        )
        transfer_config = TransferConfig(
            multipart_threshold=16 * 1024 * 1024,
            max_concurrency=2,
            multipart_chunksize=8 * 1024 * 1024,
            use_threads=True
        )
        s3.upload_file(local_file, BUCKET, s3_key_name, Config=transfer_config)
        mins = (time.time() - t0) / 60
        log(f"  Upload completed via boto3 in {mins:.1f} min!")
        return True
    except Exception as e:
        log(f"  boto3 notice: {e}, using DuckDB fallback uploader...")

    c = duckdb.connect()
    try:
        c.execute("SET memory_limit='500MB'")
        c.execute("SET threads=1")
        c.execute("SET s3_uploader_thread_limit=1")
        c.execute("INSTALL httpfs; LOAD httpfs")
        c.execute(f"""
        CREATE OR REPLACE SECRET hf_up (
            TYPE s3,
            KEY_ID '{KEY}',
            SECRET '{SEC}',
            ENDPOINT '{S3_ENDPOINT}',
            URL_STYLE 'path',
            REGION 'us-east-1'
        )""")
        c.execute(f"""
        COPY (SELECT * FROM read_parquet('{local_file}'))
        TO 's3://{BUCKET}/{s3_key_name}'
        (FORMAT parquet, ROW_GROUP_SIZE 50000, COMPRESSION zstd)
        """)
        mins = (time.time() - t0) / 60
        log(f"  Upload completed via DuckDB in {mins:.1f} min!")
        return True
    finally:
        c.close()

def main():
    global monitoring
    log("=== STARTING LOW-MEMORY OUT-OF-CORE SORTER ===")
    limit = get_cgroup_memory_limit_mb()
    log(f"Container cgroup limit: {limit:.1f} MB | DuckDB memory_limit: 600MB | Threads: 2")

    mem_thread = threading.Thread(target=memory_watcher, daemon=True)
    mem_thread.start()

    for src_path, local_sorted_path, s3_name in JOBS:
        if not os.path.exists(src_path):
            log(f"SKIP — source file not found: {src_path}")
            continue

        src_size_gb = os.path.getsize(src_path) / 1e9
        log(f"--- Processing {src_path} ({src_size_gb:.1f} GB) ---")

        if os.path.exists(local_sorted_path) and os.path.getsize(local_sorted_path) > 1024 * 1024:
            log(f"Local sorted file already exists: {local_sorted_path}. Skipping sort phase.")
        else:
            log(f"Phase 1: Sorting locally to {local_sorted_path} ...")
            t_sort = time.time()
            con = create_duckdb_sort_con()
            try:
                query = f"""
                COPY (
                    SELECT {COLS}
                    FROM read_parquet('{src_path}')
                    ORDER BY phoneNumber NULLS LAST
                )
                TO '{local_sorted_path}'
                (FORMAT parquet, ROW_GROUP_SIZE 50000, COMPRESSION zstd)
                """
                con.execute(query)
                mins = (time.time() - t_sort) / 60
                sorted_size_gb = os.path.getsize(local_sorted_path) / 1e9
                log(f"Phase 1 DONE in {mins:.1f} min! Output: {sorted_size_gb:.1f} GB (Peak RAM: {peak_ram:.1f} MB)")
            except Exception as e:
                log(f"ERROR during sort: {e}")
                sys.exit(1)
            finally:
                con.close()
                del con
                gc.collect()
                for f in os.listdir(TEMP_DIR):
                    p = os.path.join(TEMP_DIR, f)
                    try:
                        if os.path.isfile(p): os.remove(p)
                        elif os.path.isdir(p): shutil.rmtree(p)
                    except Exception: pass

        log(f"Phase 2: Uploading {s3_name} to Hugging Face S3 ...")
        upload_to_s3(local_sorted_path, s3_name)
        gc.collect()

    monitoring = False
    log(f"=== ALL COMPLETED SUCCESSFULLY! Peak RAM was {peak_ram:.1f} MB ===")

if __name__ == "__main__":
    main()
