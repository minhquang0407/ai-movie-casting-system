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

        current_page = 1
        total_pages = 1  # Giả định ban đầu
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


if __name__ == "__main__":
    from dotenv import load_dotenv

    # 1. Tải các biến từ file .env vào không gian hệ điều hành
    load_dotenv()

    API_KEY = os.getenv("TMDB_API_KEY")
    MINIO_USER = os.getenv("MINIO_USER")
    MINIO_PASS = os.getenv("MINIO_PASSWORD")

    if not API_KEY or API_KEY == "your_real_key_here":
        print("[FATAL ERROR] Chưa cấu hình TMDB_API_KEY thật trong file .env!")
        exit(1)

    # 2. Khởi tạo kết nối tới MinIO Container (đang chạy ở cổng 9000)
    client = Minio(
        "localhost:9000",
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )

    # 3. Kích hoạt cỗ máy trích xuất
    extractor = TMDBExtractor(api_key=API_KEY, minio_client=client)

    print("=== KHỞI ĐỘNG HỆ THỐNG TRÍCH XUẤT TMDB ===")
    total = extractor.extract_recent_movies()
    print(f"=== HOÀN TẤT: Đã tải về {total} bộ phim mới ===")