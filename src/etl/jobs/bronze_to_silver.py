from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, coalesce
from pyspark.sql.types import IntegerType, StringType


class BronzeToSilverETL:
    def __init__(self, spark: SparkSession, catalog_name: str = "local", db_name: str = "silver"):
        self.spark = spark
        self.catalog = catalog_name
        self.db = db_name

    def process_movies(self, raw_df: DataFrame) -> DataFrame:
        """
        Chuẩn hóa bảng Movies: Xử lý Null và cân bằng Data Skew.
        """
        # 1. Imputation (Thay thế Null bằng giá trị mặc định)
        # Sử dụng Đại số boolean để ánh xạ các tập hợp rỗng
        clean_df = raw_df \
            .withColumn("release_year", coalesce(col("release_year"), lit(1900)).cast(IntegerType())) \
            .withColumn("title", coalesce(col("title"), lit("Unknown Title")).cast(StringType()))

        # 2. Xử lý Data Skew bằng Phân bổ lại Không gian ngẫu nhiên
        # Ép PySpark xáo trộn (shuffle) dữ liệu đều ra 4 phân vùng
        balanced_df = clean_df.repartition(4)

        return balanced_df

    def process_acted_in_edges(self, raw_df: DataFrame) -> DataFrame:
        """
        Chuẩn hóa bảng Acted_In: Giữ lại Edge Properties (character_name).
        """
        clean_df = raw_df \
            .withColumn("character_name", coalesce(col("character_name"), lit("Unnamed Role")))

        return clean_df.repartition(4)

    def write_to_iceberg(self, df: DataFrame, table_name: str):
        """
        Ghi dữ liệu xuống tầng Silver của Iceberg bằng định dạng Parquet với ACID.
        """
        full_table_path = f"{self.catalog}.{self.db}.{table_name}"

        # Hàm writeTo() của Iceberg hỗ trợ Schema Evolution tự động
        df.writeTo(full_table_path) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .option("mergeSchema", "true") \
            .append()  # Sử dụng Append, Iceberg sẽ tự động xử lý con trỏ Snapshot