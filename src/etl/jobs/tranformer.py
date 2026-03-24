import torch


class PyGTransformer:
    """
    Biên dịch Đồ thị Lân cận (Ego-graph JSON) thành Ma trận Toán học cho PyTorch Geometric.
    """

    def __init__(self):
        self.node_to_idx = {}
        self.idx_to_node = {}
        self.current_idx = 0

    def _add_node(self, name: str) -> int:
        """Cấp phát địa chỉ RAM (Index) cho mỗi Đỉnh mới xuất hiện."""
        if name not in self.node_to_idx:
            self.node_to_idx[name] = self.current_idx
            self.idx_to_node[self.current_idx] = name
            self.current_idx += 1
        return self.node_to_idx[name]

    def transform_ego_graph(self, root_node: str, ego_network_json: list):
        """
        Chuyển đổi JSON thành Tensor edge_index [2, E].
        """
        source_nodes = []
        target_nodes = []

        # 1. Cấp phát Index cho Đỉnh Gốc (Diễn viên chính)
        root_idx = self._add_node(root_node)

        # 2. Quét qua cấu trúc JSON từ Neo4j
        for movie_record in ego_network_json:
            movie_title = movie_record['movie_title']
            co_actors = movie_record['co_actors']

            # Cấp phát Index cho Phim
            movie_idx = self._add_node(movie_title)

            # Nối Diễn viên chính với Phim (Vô hướng)
            source_nodes.extend([root_idx, movie_idx])
            target_nodes.extend([movie_idx, root_idx])

            # Nối các Bạn diễn (Co-actors) với Phim (Vô hướng)
            for co_actor in co_actors:
                co_actor_idx = self._add_node(co_actor)
                source_nodes.extend([co_actor_idx, movie_idx])
                target_nodes.extend([movie_idx, co_actor_idx])

        # 3. Ép kiểu thành Tensor của PyTorch (Nằm trên RAM hoặc VRAM của GPU)
        edge_index = torch.tensor([source_nodes, target_nodes], dtype=torch.long)

        print(f"[SYSTEM] Kích thước Ma trận Cạnh (Edge Index): {edge_index.shape}")

        return edge_index, self.node_to_idx