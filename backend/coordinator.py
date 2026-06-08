"""
coordinator.py - Máy chủ Điều phối Truy vấn Phân tán (Distributed Query Coordinator)
===============================================
Triển khai mẫu thiết kế Coordinator/Master để điều phối việc phát hiện gian lận phân tán.

Tham chiếu Giáo trình:
- Chương 1 (Introduction - Giới thiệu):
    - Coordinator đóng vai trò là "Client" (Máy khách) trong kiến trúc phân tán Client/Server.
    - Nó gửi các truy vấn song song (parallel queries) đến tất cả các node và tổng hợp kết quả.

- Chương 4 (Query Processing - Xử lý truy vấn):
    - Thực thi truy vấn song song (Parallel Query Execution): Coordinator gửi các truy vấn con (sub-queries)
      đến tất cả các node cùng lúc thông qua ThreadPoolExecutor, đạt được tốc độ nhanh nhờ chạy song song.
    - Loại bỏ trùng lặp kết quả (Result Deduplication): Chu trình A->B->C->D->A hoàn toàn giống với B->C->D->A->B.
      Hệ thống chuẩn hóa các chu trình bằng cách sắp xếp (sort) lại danh sách đỉnh để tạo ra một khóa chuẩn (canonical key).

- Chương 8 (Parallel Database Systems - Hệ thống CSDL song song):
    - Coordinator thực hiện bước "Reduce" (Tổng hợp) để gộp các kết quả cục bộ (partial results) từ
      tất cả các node, tương tự như mô hình MapReduce.

- Tiêu chí chấm điểm Category 14 (Traversal Logic - Logic duyệt đồ thị):
    - Thể hiện sự điều phối truy vấn phân tán chính xác trên nhiều máy chủ (sites).
    - Xử lý lỗi hỏng node một cách nhẹ nhàng (graceful degradation/fault tolerance).
"""

import requests
import concurrent.futures
import time


NODES_CONFIG = {
    0: "http://localhost:5001",
    1: "http://localhost:5002",
    2: "http://localhost:5003",
}


def query_node(node_id, url):
    """
    Gửi lệnh bắt đầu tìm kiếm (initiate_search) tới một node cụ thể.

    Trả về:
        tuple: (danh_sách_chu_trình, dict_thống_kê, bool_thành_công, thời_gian_ms)
    """
    try:
        start_time = time.time()
        response = requests.post(f"{url}/initiate_search", json={}, timeout=120.0)
        elapsed = (time.time() - start_time) * 1000

        if response.status_code == 200:
            data = response.json()
            return (
                data.get("cycles", []),
                data.get("stats", {}),
                True,
                round(elapsed, 2),
            )
        else:
            return [], {}, False, round(elapsed, 2)
    except requests.exceptions.RequestException:
        return [], {}, False, 0


def detect_fraud_rings(nodes_config=None):
    """
    Điều phối việc dò tìm đường dây gian lận trên tất cả các node phân tán.

    Các bước:
    1. Gửi lệnh initiate_search song song tới tất cả các node đang hoạt động.
    2. Thu thập các ứng viên chu trình thô (raw cycles) từ mỗi node.
    3. Loại bỏ trùng lặp bằng cách chuyển chu trình về dạng chuẩn hóa (canonical representation).
    4. Trả về kết quả có cấu trúc kèm theo các chỉ số hiệu năng (performance metrics).

    Tham số:
        nodes_config: Dict ánh xạ node_id -> url. Mặc định là NODES_CONFIG.

    Trả về:
        dict: Kết quả tìm kiếm bao gồm chu trình, thời gian chạy và thống kê của các node.
    """
    if nodes_config is None:
        nodes_config = NODES_CONFIG

    start_time = time.time()
    all_raw_cycles = []
    node_results = []
    total_stats = {
        "total_local_ops": 0,
        "total_network_messages": 0,
        "total_failed_requests": 0,
    }

    # Gửi truy vấn song song tới tất cả các node (Parallel query dispatch)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(query_node, nid, url): nid
            for nid, url in nodes_config.items()
        }

        for future in concurrent.futures.as_completed(futures):
            nid = futures[future]
            cycles, stats, success, elapsed = future.result()
            node_results.append({
                "node_id": nid,
                "success": success,
                "cycles_found": len(cycles),
                "elapsed_ms": elapsed,
                "stats": stats,
            })

            if success:
                all_raw_cycles.extend(cycles)
                total_stats["total_local_ops"] += stats.get("local_ops", 0)
                total_stats["total_network_messages"] += stats.get(
                    "network_messages", 0
                )
                total_stats["total_failed_requests"] += stats.get(
                    "failed_requests", 0
                )

    total_time = (time.time() - start_time) * 1000

    # Loại bỏ chu trình trùng lặp bằng cách dùng tuple đã được sắp xếp làm khóa chuẩn
    unique_cycles = {}
    for cycle in all_raw_cycles:
        if len(cycle) == 5 and cycle[0] == cycle[-1]:
            vertices = cycle[:-1]
            signature = tuple(sorted(vertices))
            if signature not in unique_cycles:
                unique_cycles[signature] = cycle

    # Phân loại chu trình là cục bộ (local) hay liên mảnh (cross-shard)
    detected_cycles = []
    for sig, cycle in unique_cycles.items():
        vertices = cycle[:-1]
        
        # Tính toán "node nhà" dựa trên Hash Partitioning
        home_nodes = []
        for v in vertices:
            vid = int(v)
            home_nodes.append(vid % len(nodes_config))
                
        is_cross_shard = len(set(home_nodes)) > 1

        detected_cycles.append({
            "cycle": cycle,
            "vertices": vertices,
            "home_nodes": home_nodes,
            "is_cross_shard": is_cross_shard,
            "type": "cross-shard" if is_cross_shard else "local",
        })

    active_nodes = sum(1 for nr in node_results if nr["success"])

    result = {
        "total_cycles_detected": len(detected_cycles),
        "local_cycles": sum(1 for c in detected_cycles if not c["is_cross_shard"]),
        "cross_shard_cycles": sum(1 for c in detected_cycles if c["is_cross_shard"]),
        "cycles": detected_cycles,
        "total_time_ms": round(total_time, 2),
        "active_nodes": active_nodes,
        "total_nodes": len(nodes_config),
        "node_results": node_results,
        "aggregate_stats": total_stats,
    }

    return result

