import duckdb,time,os,sys
KEY="HFAKfwV25funOIh8AuOKoLOPNpBfMGI"
SEC="169d8dc685e022cd1531d023c38c9282919c1f7b1f13cf6ccf826e7257fdf09a"
C="name,fathersName,phoneNumber,aadharNumber,otherNumber,address,district,pincode,state,town"
os.makedirs("/root/dt",exist_ok=True)
def mk():
 c=duckdb.connect()
 c.execute("SET temp_directory='/root/dt'")
 c.execute("SET memory_limit='8GB'")
 c.execute("SET threads=8")
 c.execute("INSTALL httpfs;LOAD httpfs")
 c.execute(f"CREATE OR REPLACE SECRET h(TYPE s3,KEY_ID '{KEY}',SECRET '{SEC}',ENDPOINT 's3.hf.co/adityasinghlko000',URL_STYLE 'path',REGION 'us-east-1')")
 return c
J=[("/root/Downloads/part2b_new.parquet","s3://my-db/sorted_part2b.parquet"),("/root/Downloads/part2a.parquet","s3://my-db/sorted_part2a.parquet"),("/root/Downloads/part1.parquet","s3://my-db/sorted_part1.parquet")]
def lg(m):
 s=f"[{time.strftime('%H:%M:%S')}] {m}"
 print(s,flush=True)
 open("/root/sort.log","a").write(s+"\n")
lg("START")
for src,dst in J:
 if not os.path.exists(src):lg(f"SKIP {src}");continue
 lg(f"Sorting {src} ({os.path.getsize(src)/1e9:.1f}GB)")
 c=mk();t=time.time()
 try:c.execute(f"COPY(SELECT {C} FROM read_parquet('{src}')ORDER BY phoneNumber NULLS LAST)TO '{dst}'(FORMAT parquet,ROW_GROUP_SIZE 50000,COMPRESSION zstd)")
 except Exception as e:lg(f"ERR {e}");sys.exit(1)
 finally:lg(f"Done {(time.time()-t)/60:.1f}min");c.close()
lg("ALL DONE!")
