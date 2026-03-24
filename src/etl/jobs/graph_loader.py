from src.infrastructure.neo4j_client import Neo4jStorageClient


class GraphLoader:
    def __init__(self):
        # Lấy kết nối duy nhất từ Singleton
        self.neo4j_client = Neo4jStorageClient()
        self.driver = self.neo4j_client.get_driver()

    def load_movie_and_credits(self, movie_id: int, movie_title: str, actors: list):
        """
        Nạp Đỉnh và Cạnh theo cấu trúc Tô-pô (Topological Sort) để chống Deadlock.
        Sử dụng MERGE để đảm bảo tính Lũy đẳng (Idempotency).
        """
        with self.driver.session() as session:
            # 1. ĐẢM BẢO ĐỈNH GỐC (MOVIE) TỒN TẠI
            session.run("""
                MERGE (m:Movie {id: $id})
                SET m.title = $title
            """, id=movie_id, title=movie_title)

            if not actors:
                return

            # 2. ĐẢM BẢO CÁC ĐỈNH ĐÍCH (ACTORS) TỒN TẠI ĐỘC LẬP
            # Toán tử UNWIND giúp rải mảng JSON thành các hàng riêng biệt (như Vòng lặp For trong SQL)
            session.run("""
                UNWIND $actors AS actor
                MERGE (p:Person {id: actor.id})
                SET p.name = actor.name
            """, actors=actors)

            # 3. VẼ CẠNH (EDGES) KẾT NỐI
            # Lúc này cả Phim và Diễn viên đã có sẵn, Neo4j chỉ việc trượt trên ổ cứng để nối dây cáp quang
            session.run("""
                UNWIND $actors AS actor
                MATCH (m:Movie {id: $movie_id})
                MATCH (p:Person {id: actor.id})
                MERGE (p)-[:ACTED_IN]->(m)
            """, movie_id=movie_id, actors=actors)

            print(f"[SUCCESS] Đã chiếu (project) phim {movie_id} và {len(actors)} diễn viên vào Đồ thị.")


if __name__ == "__main__":
    # Test thử hệ thống với dữ liệu giả lập (Mock Data)
    loader = GraphLoader()

    mock_movie_id = 9999
    mock_title = "The Matrix: Resurrections"
    mock_actors = [
        {"id": 111, "name": "Keanu Reeves"},
        {"id": 222, "name": "Carrie-Anne Moss"},
    ]

    print("=== KHỞI ĐỘNG CỖ MÁY NẠP ĐỒ THỊ ===")
    loader.load_movie_and_credits(mock_movie_id, mock_title, mock_actors)
    print("=== HOÀN TẤT ===")