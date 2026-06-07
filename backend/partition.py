"""
partition.py - Graph Partitioning Engine
========================================
Implements Hash-based Vertex-Home Edge-Cut Partitioning for distributed graph storage.

Textbook Reference:
- Chapter 2 (Distributed Database Design):
    - Section on Horizontal Fragmentation: Each edge is allocated to a partition based
      on the hash of the source vertex (FromAccount). This is analogous to Primary
      Horizontal Fragmentation using a hash predicate.
    - Allocation Strategy: The "home node" of a vertex V is HomeNode(V) = Hash(V) % K,
      where K is the number of sites. All outgoing edges of V are stored at V's home node.

- Chapter 4 (Query Processing):
    - Cost Model: Total_Cost = I/O_Cost + CPU_Cost + Communication_Cost
    - The Edge-Cut Ratio directly impacts Communication_Cost, as edges crossing partition
      boundaries require inter-node messaging during distributed traversal.

- Category 14 Grading (Topology Analysis):
    - Edge-Cut Ratio: Percentage of edges whose endpoints reside on different partitions.
      Lower ratio = better partitioning = less cross-shard communication.
    - Vertex Replication Factor: Average number of partitions each vertex appears in.
      Factor close to 1.0 = minimal replication overhead.
"""

import csv
import json
import os
import time


def parse_bool(value):
    return str(value).strip().lower() in ["true", "1", "yes", "t"]

def load_and_partition_graph(csv_path, num_partitions=3, strategy="hash"):
    """
    Partition a graph from CSV into K shards.

    The partitioning strategy:
    - HomeNode(V) = V % num_partitions
    - An edge (U -> V) is stored at HomeNode(U)
    - This ensures all outgoing edges of a vertex are co-located,
      enabling efficient local adjacency list lookups during traversal.

    Args:
        csv_path: Path to the financial_transactions.csv file.
        num_partitions: Number of shards/sites to partition into.

    Returns:
        dict: Partition statistics and topology analysis metrics.
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

            all_vertices.add(u)
            all_vertices.add(v)

            # Strategy-based Partitioning
            if strategy == "smart":
                # Block-aware Partitioning
                # Groups nearby numeric AccountIDs into the same shard.
                # This is a lightweight graph-aware baseline used for comparison with hash partitioning.
                p_id = (u // 50) % num_partitions
            else:
                # Hash-based Partitioning (Random-like scattering)
                p_id = u % num_partitions

            partitions[p_id].append({
                "from": u,
                "to": v,
                "amount": amount,
                "is_fraud": is_fraud,
            })

            vertices_per_partition[p_id].add(u)
            vertices_per_partition[p_id].add(v)

            total_edges += 1
            if is_fraud:
                fraud_edges += 1
                
            # Edge is "cut" if source and destination are on different home nodes
            if strategy == "smart":
                dest_p_id = (v // 50) % num_partitions
            else:
                dest_p_id = v % num_partitions
                
            if p_id != dest_p_id:
                cut_edges += 1

    # --- Topology Analysis Metrics (Category 14 requirement) ---
    edge_cut_ratio = (cut_edges / total_edges * 100) if total_edges > 0 else 0

    # Vertex Replication Factor: how many partitions each vertex appears in (on average)
    total_vertex_appearances = 0
    for v in all_vertices:
        appearances = sum(
            1 for p in range(num_partitions) if v in vertices_per_partition[p]
        )
        total_vertex_appearances += appearances

    replication_factor = (
        total_vertex_appearances / len(all_vertices) if all_vertices else 0
    )

    # Per-partition statistics
    partition_stats = []
    for p_id in range(num_partitions):
        partition_stats.append({
            "partition_id": p_id,
            "num_edges": len(partitions[p_id]),
            "num_vertices": len(vertices_per_partition[p_id]),
        })

    # Write partition files
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
