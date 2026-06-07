"""
app.py - Web Dashboard API Server
==================================
Central Flask application that serves the web dashboard and provides REST API
for controlling the entire distributed fraud detection system.

This server manages:
1. Data generation with configurable parameters
2. Graph partitioning and topology analysis
3. Node lifecycle management (start/stop/health)
4. Fraud detection query orchestration
5. Benchmark execution with history tracking
6. Fault tolerance simulation (kill/restart nodes)

Textbook Reference:
- Chapter 1: Demonstrates a complete Shared-Nothing distributed database system
  with independent sites communicating via network messages.
- Chapter 4: Cost analysis with Communication_Cost as the dominant factor.
- Chapter 8: Parallel execution across multiple sites for performance.
"""

import os
import sys
import json
import time
import subprocess
import signal
import requests
import threading
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# Import local modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_data import generate_transaction_dataset
from partition import load_and_partition_graph
from coordinator import detect_fraud_rings

app = Flask(__name__, static_folder="../frontend", static_url_path="")
CORS(app)

# ──────────────────────────────────────────────
# Global State
# ──────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

node_processes = {}  # { node_id: subprocess.Popen }
benchmark_history = []  # List of benchmark run results
BENCHMARK_FILE = os.path.join(DATA_DIR, "benchmark_history.json")

def load_benchmark_history():
    global benchmark_history
    if os.path.exists(BENCHMARK_FILE):
        try:
            with open(BENCHMARK_FILE, "r", encoding="utf-8") as f:
                benchmark_history = json.load(f)
        except Exception:
            benchmark_history = []

def save_benchmark_history():
    try:
        with open(BENCHMARK_FILE, "w", encoding="utf-8") as f:
            json.dump(benchmark_history, f, indent=2)
    except Exception as e:
        print(f"Error saving benchmark history: {e}")

load_benchmark_history()

current_config = {
    "num_accounts": 1000,
    "num_normal_txs": 5000,
    "num_partitions": 3,
    "num_local_cycles": 2,
    "num_cross_cycles": 3,
    "fraud_amount_base": 5000.0,
    "partition_strategy": "hash",
}
last_partition_result = None
last_generation_result = None
last_detection_result = None

NUM_NODES = 3
BASE_PORT = 5001

NODES_CONFIG = {i: f"http://localhost:{BASE_PORT + i}" for i in range(NUM_NODES)}


# ──────────────────────────────────────────────
# Frontend Serving
# ──────────────────────────────────────────────

@app.route("/")
def serve_frontend():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory(app.static_folder, path)


# ──────────────────────────────────────────────
# API: Data Generation
# ──────────────────────────────────────────────

@app.route("/api/generate", methods=["POST"])
def api_generate_data():
    """Generate synthetic transaction data with configurable parameters."""
    global current_config, last_generation_result

    params = request.json or {}
    current_config.update({
        "num_accounts": params.get("num_accounts", current_config["num_accounts"]),
        "num_normal_txs": params.get("num_normal_txs", current_config["num_normal_txs"]),
        "num_partitions": params.get("num_partitions", current_config["num_partitions"]),
        "num_local_cycles": params.get("num_local_cycles", current_config["num_local_cycles"]),
        "num_cross_cycles": params.get("num_cross_cycles", current_config["num_cross_cycles"]),
        "fraud_amount_base": params.get("fraud_amount_base", current_config["fraud_amount_base"]),
    })

    csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
    try:
        result = generate_transaction_dataset(
            csv_path,
            num_accounts=current_config["num_accounts"],
            num_normal_txs=current_config["num_normal_txs"],
            num_partitions=current_config["num_partitions"],
            num_local_cycles=current_config["num_local_cycles"],
            num_cross_cycles=current_config["num_cross_cycles"],
            fraud_amount_base=current_config["fraud_amount_base"],
        )
        last_generation_result = result
        return jsonify({"success": True, "data": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ──────────────────────────────────────────────
# API: Graph Partitioning
# ──────────────────────────────────────────────

@app.route("/api/partition", methods=["POST"])
def api_partition():
    """Partition the transaction graph and compute topology metrics."""
    global last_partition_result, current_config

    params = request.json or {}
    num_partitions = params.get("num_partitions", current_config["num_partitions"])
    strategy = params.get("partition_strategy", current_config.get("partition_strategy", "hash"))
    
    current_config["num_partitions"] = num_partitions
    current_config["partition_strategy"] = strategy

    csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
    if not os.path.exists(csv_path):
        return jsonify({"success": False, "error": "No data file found. Generate data first."}), 400

    try:
        result = load_and_partition_graph(csv_path, num_partitions=num_partitions, strategy=strategy)
        last_partition_result = result
        return jsonify({"success": True, "data": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ──────────────────────────────────────────────
# API: Node Management
# ──────────────────────────────────────────────

@app.route("/api/nodes/start", methods=["POST"])
def api_start_nodes():
    """Start all distributed node servers."""
    global node_processes

    results = []
    node_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node.py")

    for nid in range(NUM_NODES):
        if nid in node_processes and node_processes[nid].poll() is None:
            results.append({"node_id": nid, "status": "already_running", "pid": node_processes[nid].pid})
            continue

        log_file_path = os.path.join(DATA_DIR, f"node_{nid}.log")
        log_file = open(log_file_path, "w", encoding="utf-8")

        p = subprocess.Popen(
            [sys.executable, node_script, str(nid), DATA_DIR],
            stdout=log_file,
            stderr=log_file,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
        node_processes[nid] = p
        results.append({"node_id": nid, "status": "started", "pid": p.pid, "port": BASE_PORT + nid})

    # Wait for nodes to boot
    time.sleep(2.0)

    # Health check
    for r in results:
        nid = r["node_id"]
        try:
            resp = requests.get(f"{NODES_CONFIG[nid]}/health", timeout=2.0)
            if resp.status_code == 200:
                r["health"] = "healthy"
            else:
                r["health"] = "unhealthy"
        except requests.exceptions.RequestException:
            r["health"] = "unreachable"

    return jsonify({"success": True, "nodes": results})


@app.route("/api/nodes/stop", methods=["POST"])
def api_stop_nodes():
    """Stop all distributed node servers."""
    results = []
    for nid in list(node_processes.keys()):
        p = node_processes[nid]
        try:
            if p.poll() is None:
                p.terminate()
                p.wait(timeout=5)
            results.append({"node_id": nid, "status": "stopped"})
        except Exception as e:
            results.append({"node_id": nid, "status": "error", "error": str(e)})

    node_processes.clear()
    return jsonify({"success": True, "nodes": results})


@app.route("/api/nodes/status", methods=["GET"])
def api_nodes_status():
    """Get health status of all nodes."""
    statuses = []
    for nid in range(NUM_NODES):
        status = {
            "node_id": nid,
            "port": BASE_PORT + nid,
            "process_running": nid in node_processes and node_processes[nid].poll() is None,
        }

        try:
            resp = requests.get(f"{NODES_CONFIG[nid]}/health", timeout=1.0)
            if resp.status_code == 200:
                health_data = resp.json()
                status["health"] = "healthy"
                status["num_edges"] = health_data.get("num_edges", 0)
                status["num_vertices"] = health_data.get("num_vertices", 0)
            else:
                status["health"] = "unhealthy"
        except requests.exceptions.RequestException:
            status["health"] = "offline"

        statuses.append(status)

    return jsonify({"nodes": statuses})


@app.route("/api/nodes/kill/<int:nid>", methods=["POST"])
def api_kill_node(nid):
    """
    Kill a specific node to simulate fault/crash scenario.

    Textbook Reference - Chapter 5 (Transaction Management):
    - Demonstrates fault tolerance in distributed systems.
    - The remaining nodes should continue to function correctly,
      only failing for paths that require the downed node.
    """
    if nid not in node_processes:
        return jsonify({"success": False, "error": f"Node {nid} is not managed."}), 404

    p = node_processes[nid]
    if p.poll() is not None:
        return jsonify({"success": False, "error": f"Node {nid} is already stopped."}), 400

    try:
        p.terminate()
        p.wait(timeout=5)
        return jsonify({"success": True, "node_id": nid, "status": "killed"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/nodes/restart/<int:nid>", methods=["POST"])
def api_restart_node(nid):
    """Restart a specific node after it was killed."""
    node_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node.py")
    log_file_path = os.path.join(DATA_DIR, f"node_{nid}.log")

    # Kill if still running
    if nid in node_processes:
        p = node_processes[nid]
        if p.poll() is None:
            p.terminate()
            p.wait(timeout=5)

    log_file = open(log_file_path, "w", encoding="utf-8")
    p = subprocess.Popen(
        [sys.executable, node_script, str(nid), DATA_DIR],
        stdout=log_file,
        stderr=log_file,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
    )
    node_processes[nid] = p

    time.sleep(1.5)

    try:
        resp = requests.get(f"{NODES_CONFIG[nid]}/health", timeout=2.0)
        health = "healthy" if resp.status_code == 200 else "unhealthy"
    except requests.exceptions.RequestException:
        health = "unreachable"

    return jsonify({
        "success": True,
        "node_id": nid,
        "status": "restarted",
        "pid": p.pid,
        "health": health,
    })


# ──────────────────────────────────────────────
# API: Fraud Detection
# ──────────────────────────────────────────────

@app.route("/api/detect", methods=["POST"])
def api_detect_fraud():
    """
    Execute distributed fraud ring detection query.

    This orchestrates the full query:
    1. Coordinator sends parallel requests to all active nodes
    2. Each node performs local DFS + cross-shard path expansion
    3. Results are collected and deduplicated
    """
    try:
        global last_detection_result
        strategy = current_config.get("partition_strategy", "hash")
        result = detect_fraud_rings(NODES_CONFIG, strategy=strategy)
        last_detection_result = result
        return jsonify({"success": True, "data": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/metadata", methods=["GET"])
def api_get_metadata():
    import csv
    meta_path = os.path.join(DATA_DIR, "account_metadata.csv")
    if not os.path.exists(meta_path):
        return jsonify({"success": False, "error": "Metadata file not found"}), 404
        
    metadata = {}
    with open(meta_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            metadata[row["AccountID"]] = row
            
    return jsonify({"success": True, "metadata": metadata})


# ──────────────────────────────────────────────
# API: Benchmarking
# ──────────────────────────────────────────────

@app.route("/api/benchmark", methods=["POST"])
def api_run_benchmark():
    """
    Run a complete benchmark cycle:
    1. Generate data with specified parameters
    2. Partition the graph
    3. Restart all nodes with new data
    4. Execute fraud detection
    5. Record results

    This supports the instructor's requirement for running tests multiple times
    with different configurations and comparing results via charts.
    """
    global benchmark_history

    params = request.json or {}
    label = params.get("label", f"Run #{len(benchmark_history) + 1}")

    try:
        dataset_mode = params.get("dataset_mode", "synthetic")
        csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")

        # Step 1: Generate or Use existing
        if dataset_mode == "synthetic":
            gen_result = generate_transaction_dataset(
                csv_path,
                num_accounts=params.get("num_accounts", 1000),
                num_normal_txs=params.get("num_normal_txs", 5000),
                num_partitions=params.get("num_partitions", 3),
                num_local_cycles=params.get("num_local_cycles", 2),
                num_cross_cycles=params.get("num_cross_cycles", 3),
                fraud_amount_base=params.get("fraud_amount_base", 5000.0),
            )
        else:
            if not os.path.exists(csv_path):
                raise FileNotFoundError("PaySim data file not found.")
            gen_result = {"total_transactions": "PaySim Data", "generation_time_ms": 0}

        strategy = params.get("partition_strategy", current_config.get("partition_strategy", "hash"))
        current_config["partition_strategy"] = strategy

        # Step 2: Partition
        part_result = load_and_partition_graph(
            csv_path, 
            num_partitions=params.get("num_partitions", 3),
            strategy=strategy
        )

        # Step 3: Restart nodes
        # Stop existing nodes
        for nid in list(node_processes.keys()):
            p = node_processes[nid]
            if p.poll() is None:
                p.terminate()
                p.wait(timeout=5)

        node_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node.py")
        for nid in range(NUM_NODES):
            log_file = open(
                os.path.join(DATA_DIR, f"node_{nid}.log"), "w", encoding="utf-8"
            )
            p = subprocess.Popen(
                [sys.executable, node_script, str(nid), DATA_DIR],
                stdout=log_file,
                stderr=log_file,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
            )
            node_processes[nid] = p

        time.sleep(2.5)

        # Step 4: Execute detection
        strategy = params.get("partition_strategy", "hash")
        detection_result = detect_fraud_rings(NODES_CONFIG, strategy=strategy)

        # Step 5: Record benchmark
        benchmark_entry = {
            "id": len(benchmark_history) + 1,
            "label": label,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "config": {
                "dataset_mode": dataset_mode,
                "partition_strategy": strategy,
                "num_accounts": params.get("num_accounts", 1000),
                "num_normal_txs": params.get("num_normal_txs", 5000),
                "num_partitions": params.get("num_partitions", 3),
                "num_local_cycles": params.get("num_local_cycles", 2),
                "num_cross_cycles": params.get("num_cross_cycles", 3),
            },
            "generation": {
                "total_transactions": gen_result["total_transactions"],
                "generation_time_ms": gen_result["generation_time_ms"],
            },
            "partition": {
                "edge_cut_ratio": part_result["edge_cut_ratio"],
                "vertex_replication_factor": part_result["vertex_replication_factor"],
                "partition_time_ms": part_result["partition_time_ms"],
            },
            "detection": {
                "total_cycles": detection_result["total_cycles_detected"],
                "local_cycles": detection_result["local_cycles"],
                "cross_shard_cycles": detection_result["cross_shard_cycles"],
                "total_time_ms": detection_result["total_time_ms"],
                "active_nodes": detection_result["active_nodes"],
                "network_messages": detection_result["aggregate_stats"]["total_network_messages"],
                "local_ops": detection_result["aggregate_stats"]["total_local_ops"],
            },
        }

        benchmark_history.append(benchmark_entry)
        save_benchmark_history()

        return jsonify({"success": True, "data": benchmark_entry})
    except Exception as e:
        import traceback
        return jsonify({"success": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/benchmark/history", methods=["GET"])
def api_benchmark_history():
    """Return all benchmark run history for chart visualization."""
    return jsonify({"history": benchmark_history})


@app.route("/api/benchmark/compare", methods=["POST"])
def api_benchmark_compare():
    """Run both centralized and distributed detection, and return comparison."""
    from coordinator import detect_centralized
    
    try:
        # Run Distributed
        global last_detection_result
        strategy = current_config.get("partition_strategy", "hash")
        dist_result = detect_fraud_rings(NODES_CONFIG, strategy=strategy)
        last_detection_result = dist_result

        # Run Centralized
        cent_result = detect_centralized(DATA_DIR)

        if "error" in cent_result:
            return jsonify({"success": False, "error": cent_result["error"]}), 500

        comparison = {
            "distributed": {
                "cycles_found": dist_result["total_cycles_detected"],
                "time_ms": dist_result["total_time_ms"],
                "network_messages": dist_result["aggregate_stats"]["total_network_messages"],
                "local_ops": dist_result["aggregate_stats"]["total_local_ops"]
            },
            "centralized": {
                "cycles_found": cent_result["total_cycles_detected"],
                "time_ms": cent_result["total_time_ms"],
                "network_messages": 0,  # No network
                "local_ops": cent_result["aggregate_stats"]["local_ops"]
            },
            "strategy": strategy
        }

        return jsonify({"success": True, "data": comparison})

    except Exception as e:
        import traceback
        return jsonify({"success": False, "error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/benchmark/clear", methods=["POST"])
def api_clear_history():
    """Clear benchmark history."""
    global benchmark_history
    benchmark_history = []
    save_benchmark_history()
    return jsonify({"success": True})


# ──────────────────────────────────────────────
# API: Full Pipeline
# ──────────────────────────────────────────────

@app.route("/api/pipeline", methods=["POST"])
def api_full_pipeline():
    """
    Run the complete pipeline: generate -> partition -> start nodes -> detect.
    This is the one-click demo endpoint.
    """
    params = request.json or {}
    strategy = params.get("partition_strategy", current_config.get("partition_strategy", "hash"))
    current_config["partition_strategy"] = strategy

    steps = []

    try:
        dataset_mode = params.get("dataset_mode", "synthetic")
        csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")

        # Step 1: Generate or Use existing
        if dataset_mode == "synthetic":
            gen_result = generate_transaction_dataset(
                csv_path,
                num_accounts=params.get("num_accounts", current_config["num_accounts"]),
                num_normal_txs=params.get("num_normal_txs", current_config["num_normal_txs"]),
                num_partitions=params.get("num_partitions", current_config["num_partitions"]),
                num_local_cycles=params.get("num_local_cycles", current_config["num_local_cycles"]),
                num_cross_cycles=params.get("num_cross_cycles", current_config["num_cross_cycles"]),
                fraud_amount_base=params.get("fraud_amount_base", current_config["fraud_amount_base"]),
            )
            steps.append({"step": "generate", "success": True, "data": gen_result})
        else:
            if not os.path.exists(csv_path):
                raise FileNotFoundError("PaySim data file not found.")
            steps.append({"step": "generate", "success": True, "data": {"message": "Using existing PaySim dataset", "total_transactions": "PaySim Data", "generation_time_ms": 0}})

        # Step 2: Partition
        part_result = load_and_partition_graph(
            csv_path, 
            num_partitions=params.get("num_partitions", current_config["num_partitions"]),
            strategy=params.get("partition_strategy", current_config.get("partition_strategy", "hash"))
        )
        steps.append({"step": "partition", "success": True, "data": part_result})

        # Step 3: Start nodes
        for nid in list(node_processes.keys()):
            p = node_processes[nid]
            if p.poll() is None:
                p.terminate()
                p.wait(timeout=5)

        node_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node.py")
        for nid in range(NUM_NODES):
            log_file = open(
                os.path.join(DATA_DIR, f"node_{nid}.log"), "w", encoding="utf-8"
            )
            p = subprocess.Popen(
                [sys.executable, node_script, str(nid), DATA_DIR],
                stdout=log_file,
                stderr=log_file,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
            )
            node_processes[nid] = p

        time.sleep(2.5)

        node_statuses = []
        for nid in range(NUM_NODES):
            try:
                resp = requests.get(f"{NODES_CONFIG[nid]}/health", timeout=2.0)
                node_statuses.append({
                    "node_id": nid,
                    "health": "healthy" if resp.status_code == 200 else "unhealthy",
                })
            except requests.exceptions.RequestException:
                node_statuses.append({"node_id": nid, "health": "unreachable"})

        steps.append({"step": "start_nodes", "success": True, "data": {"nodes": node_statuses}})

        # Step 4: Detect
        global last_detection_result
        strategy = params.get("partition_strategy", current_config.get("partition_strategy", "hash"))
        detection_result = detect_fraud_rings(NODES_CONFIG, strategy=strategy)
        last_detection_result = detection_result
        steps.append({"step": "detect", "success": True, "data": detection_result})

        return jsonify({"success": True, "steps": steps})
    except Exception as e:
        import traceback
        steps.append({"step": "error", "success": False, "error": str(e)})
        return jsonify({"success": False, "steps": steps, "error": str(e), "traceback": traceback.format_exc()}), 500


# ──────────────────────────────────────────────
# API: Graph Visualization
# ──────────────────────────────────────────────
@app.route("/api/graph", methods=["GET"])
def api_graph_data():
    """Returns a subset of the graph suitable for D3.js rendering."""
    import csv
    import random
    
    csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
    meta_path = os.path.join(DATA_DIR, "account_metadata.csv")
    
    if not os.path.exists(csv_path):
        return jsonify({"success": False, "error": "No data found"}), 404

    # Lấy danh sách ID thuộc về chu trình gian lận
    fraud_nodes = set()
    fraud_edges_set = set()
    cycles_list = []
    
    if last_detection_result and "cycles" in last_detection_result:
        for c in last_detection_result["cycles"]:
            cycle_nodes = c["cycle"]
            cycles_list.append(cycle_nodes)
            for i in range(len(cycle_nodes)):
                u = cycle_nodes[i]
                v = cycle_nodes[(i + 1) % len(cycle_nodes)]
                fraud_nodes.add(str(u))
                fraud_nodes.add(str(v))
                fraud_edges_set.add(f"{u}-{v}")

    nodes_dict = {}
    edges_list = []
    
    # Read metadata first
    metadata = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                metadata[row["AccountID"]] = row

    normal_edges_pool = []
    
    MAX_FRAUD_EDGES = 150
    MAX_NORMAL_EDGES = 100

    fraud_edges_list = []

    # Parse graph
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = row["FromAccount"]
            v = row["ToAccount"]
            is_fraud_val = str(row["IsFraud"]).strip().lower() in ["true", "1", "yes", "t"]
            edge_key = f"{u}-{v}"
            
            # If edge is part of detected cycles OR marked as fraud
            if edge_key in fraud_edges_set or is_fraud_val:
                fraud_edges_list.append({"source": u, "target": v, "amount": float(row["Amount"]), "is_fraud": True})
            else:
                normal_edges_pool.append({"source": u, "target": v, "amount": float(row["Amount"]), "is_fraud": False})

    # Prioritize detected cycle edges, then random sample the rest of fraud edges
    detected_fraud_edges = [e for e in fraud_edges_list if f"{e['source']}-{e['target']}" in fraud_edges_set]
    other_fraud_edges = [e for e in fraud_edges_list if f"{e['source']}-{e['target']}" not in fraud_edges_set]
    
    # Sample if too many
    if len(other_fraud_edges) > MAX_FRAUD_EDGES - len(detected_fraud_edges):
        other_fraud_edges = random.sample(other_fraud_edges, max(0, MAX_FRAUD_EDGES - len(detected_fraud_edges)))
        
    edges_list.extend(detected_fraud_edges)
    edges_list.extend(other_fraud_edges)

    for e in edges_list:
        fraud_nodes.add(e["source"])
        fraud_nodes.add(e["target"])

    # Sample normal edges to not overload browser
    sample_normal = random.sample(normal_edges_pool, min(MAX_NORMAL_EDGES, len(normal_edges_pool)))
    edges_list.extend(sample_normal)
    
    # Compile nodes
    for e in edges_list:
        nodes_dict[e["source"]] = True
        nodes_dict[e["target"]] = True

    strategy = current_config.get("partition_strategy", "hash")
    final_nodes = []
    for nid in nodes_dict.keys():
        try:
            val = int(nid)
            if strategy == "smart":
                shard = (val // 50) % NUM_NODES
            else:
                shard = val % NUM_NODES
        except:
            shard = 0
            
        node_info = {
            "id": nid,
            "shard": shard,
            "is_fraud": nid in fraud_nodes
        }
        if nid in metadata:
            node_info.update(metadata[nid])
        final_nodes.append(node_info)

    return jsonify({
        "success": True,
        "nodes": final_nodes,
        "edges": edges_list,
        "cycles": cycles_list
    })

# ──────────────────────────────────────────────
# API: System Config
# ──────────────────────────────────────────────

@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(current_config)


@app.route("/api/config", methods=["POST"])
def api_set_config():
    global current_config
    params = request.json or {}
    current_config.update(params)
    return jsonify({"success": True, "config": current_config})


# ──────────────────────────────────────────────
# Cleanup on exit
# ──────────────────────────────────────────────

import atexit

def cleanup():
    for nid, p in node_processes.items():
        try:
            if p.poll() is None:
                p.terminate()
                p.wait(timeout=3)
        except Exception:
            pass

atexit.register(cleanup)


if __name__ == "__main__":
    print("=" * 60)
    print("  DISTRIBUTED FRAUD RING DETECTION - WEB DASHBOARD")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60)
    app.run(host="localhost", port=5000, debug=False, threaded=True)
