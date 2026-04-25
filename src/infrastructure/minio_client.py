import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

class MinioStorageClient:
    _instance = None # Áp dụng mẫu thiết kế Singleton

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MinioStorageClient, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Khởi tạo kết nối vật lý tới MinIO Container."""
        load_dotenv()
        
        self.endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = os.getenv("MINIO_USER")
        self.secret_key = os.getenv("MINIO_PASSWORD")
        self.secure = os.getenv("MINIO_SECURE", "False").lower() == "true"

        if not self.access_key or not self.secret_key:
            raise ValueError("[FATAL] Thiếu thông tin xác thực MinIO trong file .env!")

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )
        print(f"[INFO] Đã thiết lập kết nối không gian tới MinIO tại {self.endpoint}")

    def get_client(self) -> Minio:
        """Trả về đối tượng Minio client thuần túy."""
        return self.client

    def ensure_bucket_exists(self, bucket_name: str):
        """Hàm hình thức hóa: Đảm bảo không gian Bucket tồn tại trước khi ghi."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"[INFO] Đã khởi tạo Bucket mới: {bucket_name}")
        except S3Error as e:
            print(f"[ERROR] Lỗi hệ thống khi kiểm tra Bucket: {e}")
            raise
