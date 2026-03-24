import streamlit as st
import requests

st.set_page_config(page_title="AI Movie Casting", layout="wide")

st.title("🎬 Hệ thống Suy luận Đồ thị AI Movie Casting")
st.markdown("---")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Cỗ máy Truy xuất")
    actor_name = st.text_input("Tên Diễn viên (Ví dụ: Keanu Reeves):", "Keanu Reeves")

    if st.button("Trích xuất Mạng lưới lân cận", use_container_width=True):
        with st.spinner("Đang tính toán ma trận Đồ thị..."):
            url = f"http://localhost:8000/api/v1/ego-graph/{actor_name}"
            try:
                res = requests.get(url)
                if res.status_code == 200:
                    st.session_state['graph_data'] = res.json()
                    st.success("Tải đồ thị thành công!")
                else:
                    st.error(f"Lỗi {res.status_code}: Không tìm thấy dữ liệu.")
            except Exception as e:
                st.error(f"Không thể kết nối lõi API: {e}")

with col2:
    st.subheader("Không gian Phản hồi (Ego-Graph)")
    if 'graph_data' in st.session_state:
        data = st.session_state['graph_data']
        st.write(f"**Nút gốc (Root Node):** {data['root_node']}")

        # Hiển thị dữ liệu
        for item in data['ego_network']:
            with st.expander(f"Phim: {item['movie_title']}"):
                st.write("**Đóng chung với:**")
                st.write(", ".join(item['co_actors']))