import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, coalesce, explode, when
from pyspark.sql.types import IntegerType, StringType, FloatType
from dotenv import load_dotenv


class BronzeToSilverETL:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def process_movies(self, bronze_path: str) -> DataFrame:
        """Đúc Node: Movie"""
        print(f"[INFO] Đang mổ xẻ không gian Movies tại: {bronze_path}")
        raw_df = self.spark.read.option("multiline", "true").json(bronze_path)

        # Lấy thông tin cơ bản của phim
        movies_df = raw_df.select(
            col("id").cast(IntegerType()),
            coalesce(col("title"), lit("Unknown Title")).cast(StringType()),
            col("popularity").cast(FloatType()),
            coalesce(col("release_date"), lit("1900-01-01")).cast(StringType()),
            col("vote_average").cast(FloatType()),
            col("vote_count").cast(IntegerType())
        ).dropDuplicates(["id"]).repartition(4)

        return movies_df

    def process_persons(self, bronze_path: str) -> DataFrame:
        """Đúc Node: Person (Cast & Crew VIP)"""
        print(f"[INFO] Đang mổ xẻ không gian Persons tại: {bronze_path}")
        raw_df = self.spark.read.option("multiline", "true").json(bronze_path)

        persons_df = raw_df.select(
            col("id").cast(IntegerType()),
            col("name").cast(StringType()),
            coalesce(col("known_for_department"), lit("Acting")).alias("primary_role").cast(StringType()),
            col("popularity").cast(FloatType())
        ).dropDuplicates(["id"]).repartition(4)

        return persons_df

    def process_edges(self, bronze_movies_path: str):
        """Kéo sợi quang học (Edges) kết nối Person và Movie"""
        print(f"[INFO] Đang dệt lưới Đồ thị (Edges) từ: {bronze_movies_path}")
        raw_df = self.spark.read.option("multiline", "true").json(bronze_movies_path)

        # 1. CẠNH: ACTED_IN (Diễn viên)
        cast_df = raw_df.select(col("id").alias("movie_id"), explode(col("credits.cast")).alias("cast"))
        acted_in_edges = cast_df.select(
            col("cast.id").alias("person_id").cast(IntegerType()),
            col("movie_id").cast(IntegerType()),
            coalesce(col("cast.character"), lit("Unnamed")).alias("role_details").cast(StringType()),
            lit("ACTED_IN").alias("rel_type"),
            lit(1.0).alias("weight")  # Trọng số mặc định
        ).dropDuplicates(["person_id", "movie_id"])

        # 2. CẠNH: CREW VIP (Đạo diễn, Biên kịch, Nhạc sĩ)
        crew_df = raw_df.select(col("id").alias("movie_id"), explode(col("credits.crew")).alias("crew"))

        # Lọc ra các VIP
        vip_jobs = ["Director", "Screenplay", "Writer", "Original Music Composer"]
        vip_crew_df = crew_df.filter(col("crew.job").isin(vip_jobs))

        # Phân loại Cạnh (Mapping)
        crew_edges = vip_crew_df.select(
            col("crew.id").alias("person_id").cast(IntegerType()),
            col("movie_id").cast(IntegerType()),
            col("crew.job").alias("role_details").cast(StringType()),
            when(col("crew.job") == "Director", "DIRECTED")
            .when(col("crew.job").isin("Screenplay", "Writer"), "WROTE")
            .otherwise("COMPOSED_MUSIC").alias("rel_type"),
            lit(2.0).alias("weight")  # VIP Crew có trọng số ảnh hưởng lớn hơn diễn viên phụ
        ).dropDuplicates(["person_id", "movie_id", "rel_type"])

        return acted_in_edges, crew_edges

    def write_to_silver(self, df: DataFrame, table_name: str, silver_path: str):
        full_path = f"{silver_path}/{table_name}"
        df.write.mode("overwrite").parquet(full_path)
        print(f"[SUCCESS] Ghi thành công Bảng {table_name} (Parquet) -> {full_path}")


if __name__ == "__main__":
    load_dotenv()

    # DÙNG MẠNG NỘI BỘ DOCKER (minio:9000) THAY VÌ LOCALHOST
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_USER = os.getenv("MINIO_USER", "admin")
    MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "AdminSecretKey123!")

    print("\n=== KHỞI ĐỘNG LÒ LUYỆN BẠC PYSPARK ===")

    spark = SparkSession.builder \
        .appName("Bronze_to_Silver_ETL_V2") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
 \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
 \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
 \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
 \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    etl = BronzeToSilverETL(spark)

    # ĐỊNH TUYẾN CHUẨN XÁC VỚI DỮ LIỆU ĐÊM QUA
    bronze_movies_path = "s3a://bronze-movies-deep/*.json"
    bronze_persons_path = "s3a://bronze-persons-deep/*.json"
    silver_base_path = "s3a://silver-layer/tmdb"

    try:
        # 1. Đúc Node
        df_movies = etl.process_movies(bronze_movies_path)
        etl.write_to_silver(df_movies, "nodes_movies", silver_base_path)

        df_persons = etl.process_persons(bronze_persons_path)
        etl.write_to_silver(df_persons, "nodes_persons", silver_base_path)

        # 2. Rút sợi quang học (Edges)
        df_acted, df_crew = etl.process_edges(bronze_movies_path)
        etl.write_to_silver(df_acted, "edges_acted_in", silver_base_path)
        etl.write_to_silver(df_crew, "edges_vip_crew", silver_base_path)

    except Exception as e:
        print(f"\n[FATAL ERROR] PySpark gãy đổ: {e}")
    finally:
        spark.stop()
        print("=== HOÀN TẤT RÈN ĐÚC BẠC ===")