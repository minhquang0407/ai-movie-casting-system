from fastapi import FastAPI, HTTPException
import uvicorn
from src.infrastructure.neo4j_client import Neo4jStorageClient

# Khởi tạo API Gateway
app = FastAPI(
    title="AI Movie Casting API",
    description="Microservice lõi phục vụ truy xuất Đồ thị và Suy luận AI",
    version="1.0.0"
)


# Vòng đời sự kiện: Khi server bật lên, nó sẽ tự động kết nối Neo4j
@app.on_event("startup")
def startup_event():
    print("[SYSTEM] Đang khởi động kết nối Đồ thị Neo4j...")
    app.state.neo4j = Neo4jStorageClient()


@app.on_event("shutdown")
def shutdown_event():
    print("[SYSTEM] Đang đóng băng Không gian Đồ thị...")
    app.state.neo4j.close()


# Điểm cuối (Endpoint) trích xuất Ego-graph
@app.get("/api/v1/ego-graph/{actor_name}")
def get_ego_graph(actor_name: str):
    """
    Trích xuất mạng lưới lân cận 2-hop của một diễn viên.
    Áp dụng thuật toán O(1) Index-Free Adjacency của Neo4j.
    """
    driver = app.state.neo4j.get_driver()

    # Phương trình trượt không gian (Cypher) của bạn
    query = """
    MATCH (p1:Person {name: $actor_name})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
    WHERE p1 <> p2
    RETURN m.title AS movie_title, collect(p2.name) AS co_actors
    LIMIT 10
    """

    with driver.session() as session:
        result = session.run(query, actor_name=actor_name)
        records = [record.data() for record in result]

    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"Không tìm thấy dữ liệu diễn viên '{actor_name}' hoặc đồ thị bị cô lập."
        )

    return {
        "root_node": actor_name,
        "hop_count": 2,
        "ego_network": records
    }


# ... (Giữ nguyên các thư viện và code Ego-graph cũ ở trên)

@app.get("/api/v1/recommend/alternatives/{actor_name}")
def get_casting_recommendations(actor_name: str):
    """
    Cỗ máy Gợi ý: Tìm Diễn viên thay thế (Collaborative Filtering).
    Mô phỏng lại quá trình học đặc trưng của Graph Neural Network.
    """
    driver = app.state.neo4j.get_driver()

    # Đại số Đồ thị: Tìm người có mạng lưới láng giềng trùng lặp cao nhất (Jaccard Overlap)
    recommend_query = """
    // Hop 1: Tìm tất cả bạn diễn của mục tiêu
    MATCH (target:Person {name: $actor_name})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(co_actor:Person)

    // Hop 2: Tìm những diễn viên KHÁC cũng đóng chung với các "bạn diễn" đó
    MATCH (co_actor)-[:ACTED_IN]->(other_movie:Movie)<-[:ACTED_IN]-(candidate:Person)

    // Điều kiện lọc: Không gợi ý chính mình, và phim đó mục tiêu chưa từng đóng
    WHERE candidate <> target 
      AND NOT (target)-[:ACTED_IN]->(other_movie)

    // Xếp hạng (Rank) dựa trên độ giao thoa không gian
    RETURN candidate.name AS recommended_actor, 
           count(DISTINCT other_movie) AS similarity_score
    ORDER BY similarity_score DESC
    LIMIT 5
    """

    with driver.session() as session:
        result = session.run(recommend_query, actor_name=actor_name)
        recommendations = [record.data() for record in result]

    if not recommendations:
        raise HTTPException(
            status_code=404,
            detail=f"Không đủ dữ liệu mạng lưới để gợi ý cho diễn viên '{actor_name}'."
        )

    return {
        "target_actor": actor_name,
        "recommendation_type": "Collaborative Filtering (GNN Simulated)",
        "recommendations": recommendations
    }


@app.get("/api/v1/recommend/vector-ai/{actor_name}")
def get_vector_casting_recommendations(actor_name: str):
    """
    Inference siêu tốc độ: Dùng trực tiếp Vector Index của Neo4j
    (Các Vector này đã được GAT Huấn luyện và bơm vào từ trước).
    """
    driver = app.state.neo4j.get_driver()

    # Đại số Hình học: Tính khoảng cách Cosine Similarity giữa các Vector
    vector_search_query = """
    // Bước 1: Lấy Vector của Diễn viên mục tiêu
    MATCH (target:Person {name: $actor_name})

    // Bước 2: Gọi thuật toán Tìm kiếm Vector nội tại của Neo4j (K-Nearest Neighbors)
    // Giả sử ta đã tạo một index tên là 'actor_embeddings'
    CALL db.index.vector.queryNodes('actor_embeddings', 6, target.embedding) 
    YIELD node AS candidate, score

    // Bước 3: Lọc bỏ chính người đó ra khỏi danh sách
    WHERE candidate.name <> target.name

    RETURN candidate.name AS recommended_actor, score AS similarity_score
    ORDER BY similarity_score DESC
    LIMIT 5
    """

    with driver.session() as session:
        result = session.run(vector_search_query, actor_name=actor_name)
        recommendations = [{"actor": r["recommended_actor"], "score": round(r["similarity_score"], 4)} for r in result]

    if not recommendations:
        raise HTTPException(status_code=404, detail="Không tìm thấy Vector hoặc Diễn viên.")

    return {
        "target_actor": actor_name,
        "model": "GAT Pre-trained Embeddings + Neo4j Vector Search",
        "recommendations": recommendations
    }   
if __name__ == "__main__":
    # Khởi chạy server tại cổng 8000
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)