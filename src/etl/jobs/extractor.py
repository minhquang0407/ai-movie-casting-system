import os
import time
import json
import io
import requests
from datetime import datetime
from minio import Minio
from minio.error import S3Error


class TMDBExtractor:
    def __init__(self, api_key: str, minio_client: Minio, bucket_name: str = "bronze"):
        """
        Khởi tạo hệ thống trích xuất tuân thủ Định lý Mũi tên Thời gian và Token Bucket.
        """
        self.api_key = api_key
        self.minio = minio_client
        self.bucket_name = bucket_name
        self.base_url = "https://api.themoviedb.org/3"
        # Đảm bảo không gian Bronze Layer tồn tại

        if not self.minio.bucket_exists(self.bucket_name):
            self.minio.make_bucket(self.bucket_name)

    def _get_hwm(self) -> str:
        """Đọc Dấu mực nước cao (High-Water Mark) từ MinIO."""
        try:
            response = self.minio.get_object(self.bucket_name, "hwm_state.json")
            data = json.loads(response.read().decode('utf-8'))
            return data.get("last_updated", "2000-01-01")
        except S3Error:
            return "2000-01-01"  # Trạng thái sơ khởi nếu chưa từng chạy

    def _update_hwm(self, current_date: str):
        """Cập nhật trạng thái HWM nguyên tử."""
        state = {"last_updated": current_date}
        state_bytes = json.dumps(state).encode('utf-8')

        self.minio.put_object(
            bucket_name=self.bucket_name,
            object_name="hwm_state.json",
            data=io.BytesIO(state_bytes),
            length=len(state_bytes),
            content_type="application/json"
        )

    def extract_recent_movies(self) -> int:
        """
        Luồng trích xuất chính (Incremental Load).
        """
        hwm_date = self._get_hwm()
        print(f"[INFO] Bắt đầu trích xuất từ HWM: {hwm_date}")

        current_page = 315
        total_pages = 1000  # Giả định ban đầu
        saved_records = 0

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        # Vòng lặp phân trang (Pagination Loop)
        while current_page <= total_pages:
            url = f"{self.base_url}/discover/movie?page={current_page}&primary_release_date.gte={hwm_date}"

            response = requests.get(url, headers=headers)

            # Cực hạn: Xử lý Rate Limiting (Lỗi 429)
            if response.status_code == 429:
                print("[WARNING] Chạm trần Rate Limit. Kích hoạt phạt lùi (Exponential Backoff)...")
                time.sleep(5.0)
                continue

            response.raise_for_status()

            data = response.json()
            total_pages = data.get("total_pages", 1)
            results = data.get("results", [])

            if not results:
                print("[INFO] Đã lấy hết!")
                break

            # Lưu dữ liệu thô vào MinIO (Data Lake)
            month_folder = datetime.now().strftime("%Y-%m")
            object_name = f"raw/tmdb/{month_folder}/movies_page_{current_page}.json"

            raw_bytes = json.dumps(results).encode('utf-8')
            self.minio.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=io.BytesIO(raw_bytes),
                length=len(raw_bytes),
                content_type="application/json"
            )

            saved_records += len(results)
            print(f"[SUCCESS] Đã lưu trang {current_page}/{total_pages} ({len(results)} bản ghi).")

            current_page += 1

            # Hàm trễ thời gian (Delay Operator) để định hình băng thông
            time.sleep(0.25)

            # Cập nhật HWM cho lần chạy tháng sau
        self._update_hwm(datetime.now().strftime("%Y-%m-%d"))
        return saved_records

    def extract_credits(self, target_month: str = None) -> int:
        """
        Luồng trích xuất Cạnh (Đạo diễn & Diễn viên) dựa trên Phim đã tải.
        Giải quyết bài toán N+1 Queries với Token Bucket.
        """
        if not target_month:
            target_month = datetime.now().strftime("%Y-%m")

        prefix = f"raw/tmdb/{target_month}/movies_page_"
        print(f"\n[INFO] Bắt đầu quét các bộ phim trong không gian: {prefix}")

        movie_ids = set()

        # 1. Quét không gian MinIO để thu thập ID phim
        try:
            objects = self.minio.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            for obj in objects:
                response = self.minio.get_object(self.bucket_name, obj.object_name)
                movies_data = json.loads(response.read().decode('utf-8'))
                for movie in movies_data:
                    movie_ids.add(movie["id"])
        except Exception as e:
            print(f"[ERROR] Lỗi khi đọc không gian MinIO: {e}")
            return 0

        total_movies = len(movie_ids)
        print(f"[SUCCESS] Đã thu thập {total_movies} Đỉnh (Movies) cần tìm Cạnh (Credits).")

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        saved_credits = 0

        # 2. Vòng lặp N+1 Queries để lấy Credits
        for idx, movie_id in enumerate(movie_ids, 1):
            url = f"{self.base_url}/movie/{movie_id}/credits"
            object_name = f"raw/tmdb/{target_month}/credits_{movie_id}.json"

            # Bỏ qua nếu đã tải rồi (Idempotency - Tính lũy đẳng)
            try:
                self.minio.stat_object(self.bucket_name, object_name)
                print(f"[{idx}/{total_movies}] Bỏ qua phim {movie_id} (Đã tồn tại Credits).")
                continue
            except S3Error:
                pass  # File chưa tồn tại, bắt đầu tải

            response = requests.get(url, headers=headers)

            # Xử lý Rate Limit cực hạn
            while response.status_code == 429:
                print(f"[WARNING] Rate Limit tại phim {movie_id}. Phạt lùi 5 giây...")
                time.sleep(5.0)
                response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"[ERROR] Bỏ qua phim {movie_id} do lỗi API: {response.status_code}")
                continue

            data = response.json()
            raw_bytes = json.dumps(data).encode('utf-8')

            # Lưu Đỉnh Dị thể và Cạnh vào MinIO
            self.minio.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=io.BytesIO(raw_bytes),
                length=len(raw_bytes),
                content_type="application/json"
            )
            saved_credits += 1
            print(f"[{idx}/{total_movies}] Đã lưu Credits cho phim {movie_id}.")

            # Toán tử trễ bảo vệ băng thông
            time.sleep(0.25)

        return saved_credits

if __name__ == "__main__":
    from infrastructure.minio_client import MinioStorageClient  # Giả định đường dẫn của bạn

    # 1. Khởi tạo Storage Client bằng Singleton
    storage = MinioStorageClient()
    minio_client = storage.get_client()
    storage.ensure_bucket_exists("bronze")

    # 2. Khởi tạo Extractor
    API_KEY = os.getenv("TMDB_API_KEY")
    extractor = TMDBExtractor(api_key=API_KEY, minio_client=minio_client)

    # 3. Chạy luồng
    extractor.extract_credits()