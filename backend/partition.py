"""
partition.py - Bộ máy Phân mảnh Đồ thị (Graph Partitioning Engine)
========================================
Triển khai kỹ thuật Phân mảnh theo đỉnh gốc (Hash-based Vertex-Home Edge-Cut Partitioning) cho lưu trữ đồ thị phân tán.

Tham chiếu Giáo trình:
- Chương 2 (Distributed Database Design - Thiết kế CSDL phân tán):
    - Phần Horizontal Fragmentation (Phân mảnh ngang): Mỗi cạnh được phân bổ vào một mảnh
      dựa trên hàm băm của đỉnh nguồn (FromAccount). Điều này tương tự như Phân mảnh
      ngang cơ sở (Primary Horizontal Fragmentation) sử dụng một vị từ băm (hash predicate).
    - Chiến lược phân bổ (Allocation Strategy): "Node nhà" (Home node) của một đỉnh V là HomeNode(V) = Hash(V) % K,
      trong đó K là số lượng máy trạm (sites). Tất cả các cạnh xuất phát từ V được lưu tại node nhà của V.

- Chương 4 (Query Processing - Xử lý truy vấn):
    - Mô hình chi phí (Cost Model): Total_Cost = I/O_Cost + CPU_Cost + Communication_Cost
    - Tỷ lệ cắt cạnh (Edge-Cut Ratio) ảnh hưởng trực tiếp đến Chi phí giao tiếp (Communication_Cost),
      vì các cạnh cắt ngang ranh giới phân mảnh sẽ yêu cầu gửi tin nhắn giữa các node trong quá trình duyệt đồ thị.

- Tiêu chí chấm điểm Category 14 (Topology Analysis - Phân tích cấu trúc mạng):
    - Tỷ lệ cắt cạnh (Edge-Cut Ratio): Phần trăm số cạnh có 2 đỉnh nằm ở 2 mảnh khác nhau.
      Tỷ lệ càng thấp = phân mảnh càng tốt = ít phải giao tiếp liên mảnh (cross-shard communication).
    - Hệ số nhân bản đỉnh (Vertex Replication Factor): Số lượng mảnh trung bình mà mỗi đỉnh xuất hiện.
      Hệ số gần 1.0 = chi phí nhân bản (overhead) ở mức tối thiểu.
"""

import csv
import json
import os
import time


def parse_bool(value):
    return str(value).strip().lower() in ["true", "1", "yes", "t"]

def load_and_partition_graph(csv_path, num_partitions=3, strategy="hash"):
    """
    Phân mảnh đồ thị từ file CSV thành K mảnh (shards).

    Chiến lược phân mảnh:
    - Node_nhà(V) = V % num_partitions
    - Một cạnh (U -> V) được lưu tại Node_nhà(U)
    - Điều này đảm bảo tất cả các cạnh xuất phát từ một đỉnh được lưu cùng một chỗ,
      giúp tăng tốc độ tra cứu danh sách kề (adjacency list) cục bộ khi duyệt đồ thị.

    Tham số:
        csv_path: Đường dẫn đến file financial_transactions.csv.
        num_partitions: Số lượng mảnh/máy trạm (shards/sites) cần chia.

    Trả về:
        dict: Thống kê phân mảnh và các chỉ số phân tích cấu trúc đồ thị.
    """
    start_time = time.time()

    partitions = {i: [] for i in range(num_partitions)}
    all_vertices = set()
    vertices_per_partition = {i: set() for i in range(num_partitions)}

    total_edges = 0
    cut_edges = 0
    fraud_edges = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = int(row["FromAccount"])
            v = int(row["ToAccount"])
            amount = float(row["Amount"])
            is_fraud = parse_bool(row["IsFraud"])
            is_cycle_fraud = parse_bool(row.get("IsCycleFraud", "0"))

            all_vertices.add(u)
            all_vertices.add(v)

            # Phân mảnh theo hàm băm - Hash-based Partitioning
            p_id = u % num_partitions

            partitions[p_id].append({
                "from": u,
                "to": v,
                "amount": amount,
                "is_fraud": is_fraud,
                "is_cycle_fraud": is_cycle_fraud,
            })

            vertices_per_partition[p_id].add(u)
            vertices_per_partition[p_id].add(v)

            total_edges += 1
            if is_fraud:
                fraud_edges += 1
                
            # Cạnh bị "cắt" (cut) nếu đỉnh nguồn và đỉnh đích nằm ở 2 máy chủ khác nhau
            dest_p_id = v % num_partitions
            if p_id != dest_p_id:
                cut_edges += 1

    # --- Các chỉ số Phân tích Cấu trúc (Yêu cầu của Category 14) ---
    edge_cut_ratio = (cut_edges / total_edges * 100) if total_edges > 0 else 0

    # Hệ số nhân bản đỉnh (Vertex Replication Factor): trung bình mỗi đỉnh xuất hiện ở bao nhiêu mảnh
    total_vertex_appearances = 0
    for v in all_vertices:
        appearances = sum(
            1 for p in range(num_partitions) if v in vertices_per_partition[p]
        )
        total_vertex_appearances += appearances

    replication_factor = (
        total_vertex_appearances / len(all_vertices) if all_vertices else 0
    )

    # Thống kê trên từng mảnh (Per-partition statistics)
    partition_stats = []
    for p_id in range(num_partitions):
        partition_stats.append({
            "partition_id": p_id,
            "num_edges": len(partitions[p_id]),
            "num_vertices": len(vertices_per_partition[p_id]),
        })

    # Ghi ra các file phân mảnh (Write partition files)
    data_dir = os.path.dirname(csv_path)
    os.makedirs(data_dir, exist_ok=True)
    for p_id in range(num_partitions):
        partition_file = os.path.join(data_dir, f"partition_{p_id}.json")
        partition_data = {
            "partition_id": p_id,
            "strategy": strategy,
            "vertices": sorted(list(vertices_per_partition[p_id])),
            "edges": partitions[p_id],
        }
        with open(partition_file, "w", encoding="utf-8") as f:
            json.dump(partition_data, f)

    elapsed = time.time() - start_time

    result = {
        "num_partitions": num_partitions,
        "total_vertices": len(all_vertices),
        "total_edges": total_edges,
        "fraud_edges": fraud_edges,
        "cut_edges": cut_edges,
        "edge_cut_ratio": round(edge_cut_ratio, 2),
        "vertex_replication_factor": round(replication_factor, 4),
        "partition_stats": partition_stats,
        "partition_time_ms": round(elapsed * 1000, 2),
    }

    return result


if __name__ == "__main__":
    result = load_and_partition_graph("data/financial_transactions.csv")
    print(json.dumps(result, indent=2))
