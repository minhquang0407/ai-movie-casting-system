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


if __name__ == "__main__":
    # Khởi chạy server tại cổng 8000
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)