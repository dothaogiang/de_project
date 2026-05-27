from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count

def run_spark_job(gcs_bucket: str, gcp_key_path: str):
    spark = SparkSession.builder \
        .appName("MovieReviewPipeline") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_path) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()

    # Đọc 2 file CSV từ GCS
    movie_df = spark.read.csv(
        f"gs://{gcs_bucket}/raw/movie_review.csv",
        header=True, inferSchema=True
    )
    purchase_df = spark.read.csv(
        f"gs://{gcs_bucket}/raw/user_purchase.csv",
        header=True, inferSchema=True
    )

    print(f"Movie reviews: {movie_df.count()} dòng")
    print(f"User purchases: {purchase_df.count()} dòng")

    # Đổi tên cột để join được
    movie_df = movie_df.withColumnRenamed("cid", "customer_id")
    purchase_df = purchase_df.withColumnRenamed("CustomerID", "customer_id")

    # Join 2 bảng theo customer_id
    joined_df = purchase_df.join(movie_df, on="customer_id", how="left")

    # Tính tổng tiền và số review theo từng khách hàng
    result_df = joined_df.groupBy("customer_id").agg(
        spark_sum(col("Quantity") * col("UnitPrice")).alias("amount_spent"),
        count("review_str").alias("num_reviews")
    )

    # Ghi kết quả về GCS dạng Parquet
    result_df.write.mode("overwrite").parquet(
        f"gs://{gcs_bucket}/processed/user_movie_review/"
    )

    print("Đã ghi xong Parquet lên GCS!")
    spark.stop()

if __name__ == "__main__":
    import sys
    gcs_bucket = sys.argv[1]
    gcp_key_path = sys.argv[2]
    run_spark_job(gcs_bucket, gcp_key_path)