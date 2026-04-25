import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.infrastructure.neo4j_client import Neo4jStorageClient


def get_spark_session():
    """Tái sử dụng Cỗ máy Spark đã được bọc thép từ Tầng Bạc."""
    load_dotenv()
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_USER = os.getenv("MINIO_USER", "admin")
    MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "AdminSecretKey123!")

    return SparkSession.builder \
        .appName("Bronze_to_Silver_ETL") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .getOrCreate()


class GraphLoader:
    def __init__(self, spark):
        self.spark = spark
        # Mở cổng không gian Bolt 7687 tới Neo4j
        self.neo4j_client = Neo4jStorageClient()
        self.driver = self.neo4j_client.get_driver()

    def load_gold_layer(self):
        print("\n[1] Đang hút Dữ liệu Tinh khiết từ MinIO (Tầng Bạc)...")
        df_movies = self.spark.read.parquet("s3a://silver/tmdb/movies_flattened")
        df_edges = self.spark.read.parquet("s3a://silver/tmdb/acted_in_edges")

        # Rút trích Đỉnh và Cạnh độc lập (Sử dụng Pandas để nạp vào RAM cục bộ vì dữ liệu < 100k dòng)
        movies_df = df_movies.select("id", "title", "release_date", "popularity").dropDuplicates(["id"]).toPandas()
        actors_df = df_edges.select("actor_id", "actor_name").dropDuplicates(["actor_id"]).toPandas()
        edges_df = df_edges.select("actor_id", "movie_id", "character_name").dropDuplicates().toPandas()

        print("\n=== BẮT ĐẦU NẠP ĐỒ THỊ THEO THỨ TỰ TÔ-PÔ (CHỐNG DEADLOCK) ===")

        with self.driver.session() as session:
            # BƯỚC 1: NẠP ĐỈNH MOVIE
            print(f"[2] Đang nạp {len(movies_df)} đỉnh Phim (Movies)...")
            # UNWIND là vòng lặp For siêu tốc của Cypher, nạp hàng vạn dòng cùng lúc
            session.run("""
                UNWIND $rows AS row
                MERGE (m:Movie {id: row.id})
                SET m.title = row.title, 
                    m.release_date = row.release_date, 
                    m.popularity = row.popularity
            """, rows=movies_df.to_dict('records'))

            # BƯỚC 2: NẠP ĐỈNH ACTOR
            print(f"[3] Đang nạp {len(actors_df)} đỉnh Diễn viên (Actors)...")
            session.run("""
                UNWIND $rows AS row
                MERGE (p:Person {id: row.actor_id})
                SET p.name = row.actor_name
            """, rows=actors_df.to_dict('records'))

            # BƯỚC 3: NẠP CẠNH ACTED_IN
            print(f"[4] Đang nối {len(edges_df)} dây cáp quang (Edges)...")
            session.run("""
                UNWIND $rows AS row
                MATCH (p:Person {id: row.actor_id})
                MATCH (m:Movie {id: row.movie_id})
                MERGE (p)-[r:ACTED_IN]->(m)
                SET r.character = row.character_name
            """, rows=edges_df.to_dict('records'))

        print("\n[SUCCESS] ĐÃ CHẾ TẠO THÀNH CÔNG KHÔNG GIAN ĐỒ THỊ TẦNG VÀNG!")


if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        loader = GraphLoader(spark)
        loader.load_gold_layer()
    except Exception as e:
        print(f"[FATAL ERROR] Không thể nạp Đồ thị: {e}")
    finally:
        spark.stop()