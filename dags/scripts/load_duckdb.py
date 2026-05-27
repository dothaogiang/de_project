import duckdb
import os
from google.cloud import storage

def load_to_duckdb(gcs_bucket: str, gcp_key_path: str, duckdb_path: str):
    # Tải file Parquet từ GCS về local
    os.makedirs("/tmp/parquet", exist_ok=True)
    client = storage.Client.from_service_account_json(gcp_key_path)
    bucket = client.bucket(gcs_bucket)
    
    blobs = list(bucket.list_blobs(prefix="processed/user_movie_review/"))
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            local_path = f"/tmp/parquet/{os.path.basename(blob.name)}"
            blob.download_to_filename(local_path)
            print(f"Đã tải: {blob.name}")

    # Load vào DuckDB
    con = duckdb.connect(duckdb_path)
    con.execute("""
        CREATE OR REPLACE TABLE user_movie_review AS
        SELECT * FROM read_parquet('/tmp/parquet/*.parquet')
    """)
    
    count = con.execute("SELECT COUNT(*) FROM user_movie_review").fetchone()[0]
    print(f"DuckDB: đã load {count} dòng vào bảng user_movie_review")
    
    # Xem thử dữ liệu
    print(con.execute("SELECT * FROM user_movie_review LIMIT 5").df())
    con.close()

if __name__ == "__main__":
    import sys
    load_to_duckdb(
        gcs_bucket=sys.argv[1],
        gcp_key_path=sys.argv[2],
        duckdb_path=sys.argv[3]
    )
