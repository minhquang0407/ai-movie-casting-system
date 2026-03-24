import os
from neo4j import GraphDatabase
from dotenv import load_dotenv


class Neo4jStorageClient:
    """
    Cầu nối vật lý (Driver) từ Python tới không gian Đồ thị Neo4j.
    Áp dụng mẫu thiết kế Singleton để tránh lãng phí kết nối mạng (Connection Pooling).
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Neo4jStorageClient, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        load_dotenv()

        # Cổng 7687 là giao thức Bolt siêu tốc của Neo4j
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")

        if not self.password:
            raise ValueError("[FATAL] Thiếu NEO4J_PASSWORD trong file .env!")

        # Khởi tạo Connection Pool
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        self.verify_connectivity()

    def verify_connectivity(self):
        """Kiểm chứng bắt tay mạng lưới (Network Handshake) với Server."""
        try:
            self.driver.verify_connectivity()
            print(f"[INFO] Đã thiết lập kết nối không gian tới Neo4j tại {self.uri}")
        except Exception as e:
            print(f"[FATAL ERROR] Động cơ Neo4j từ chối kết nối: {e}")
            raise

    def get_driver(self):
        """Trả về đối tượng driver để thực thi Cypher."""
        return self.driver

    def close(self):
        """Đóng kết nối giải phóng RAM khi hệ thống tắt."""
        if self.driver:
            self.driver.close()