"""
app.py - Máy chủ Web API & Quản lý (Web Dashboard API Server)
==================================
Ứng dụng Flask trung tâm phục vụ giao diện web dashboard và cung cấp REST API
để điều khiển toàn bộ hệ thống phát hiện gian lận phân tán.

Máy chủ này quản lý:
1. Chuẩn bị dữ liệu (Data generation/preparation)
2. Phân mảnh đồ thị và phân tích cấu trúc (Graph partitioning and topology analysis)
3. Quản lý vòng đời các máy trạm (Node lifecycle management - start/stop/health)
4. Điều phối truy vấn phát hiện gian lận (Fraud detection query orchestration)
5. Giả lập tính chịu lỗi (Fault tolerance simulation - kill/restart nodes)

Tham chiếu Giáo trình:
- Chương 1: Chứng minh một hệ thống cơ sở dữ liệu phân tán kiến trúc Shared-Nothing hoàn chỉnh,
  với các máy trạm (sites) độc lập giao tiếp qua mạng.
- Chương 4: Phân tích chi phí (Cost analysis) với Communication_Cost là yếu tố chủ đạo.
- Chương 8: Thực thi song song (Parallel execution) trên nhiều máy trạm để tăng hiệu năng.
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

# Đảm bảo stdout/stderr dùng UTF-8 trên Windows (tránh UnicodeEncodeError với cp1252)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Import local modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
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

current_config = {
    "num_partitions": 3,
}
last_partition_result = None
last_generation_result = None
last_detection_result = None

NUM_NODES = 3
BASE_PORT = 5001

NODES_CONFIG = {i: f"http://localhost:{BASE_PORT + i}" for i in range(NUM_NODES)}


# ──────────────────────────────────────────────
# Cung cấp file giao diện (Frontend Serving)
# ──────────────────────────────────────────────

@app.route("/")
def serve_frontend():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory(app.static_folder, path)


# ──────────────────────────────────────────────
# Helper: Poll node until healthy
# ──────────────────────────────────────────────

def wait_for_node(url, retries=15, delay=0.5):
    """
    Gửi liên tục yêu cầu kiểm tra tới /health để đợi node sẵn sàng hoặc hết giờ.
    Khắc phục tình trạng sleep cứng 2 giây không đủ thời gian cho các node 
    tải các file phân mảnh lớn (hơn 16.000 cạnh mỗi file).
    Thời gian chờ tối đa: retries * delay = 7.5 giây.
    """
    for _ in range(retries):
        try:
            resp = requests.get(f"{url}/health", timeout=1.0)
            if resp.status_code == 200:
                return "healthy"
        except requests.exceptions.RequestException:
            pass
        time.sleep(delay)
    return "unreachable"


def enrich_cycle_amounts(result):
    """Gắn số tiền giao dịch vào mỗi chu trình phát hiện được để hiển thị trên giao diện."""
    if not result or not result.get("cycles"):
        return result

    csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
    if not os.path.exists(csv_path):
        return result

    needed_edges = set()
    for item in result["cycles"]:
        cycle = [str(v) for v in item.get("cycle", [])]
        for i in range(len(cycle) - 1):
            needed_edges.add((cycle[i], cycle[i + 1]))

    amounts = {}
    import csv as csvlib
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csvlib.DictReader(f)
        for row in reader:
            edge = (row["FromAccount"], row["ToAccount"])
            if edge in needed_edges:
                amounts[edge] = float(row["Amount"])

    for item in result["cycles"]:
        cycle = [str(v) for v in item.get("cycle", [])]
        edge_amounts = [
            amounts.get((cycle[i], cycle[i + 1]))
            for i in range(len(cycle) - 1)
        ]
        item["amount"] = next((amt for amt in edge_amounts if amt is not None), None)

    return result


# ──────────────────────────────────────────────
# API: Chuẩn bị dữ liệu (Data Preparation)
# ──────────────────────────────────────────────

@app.route("/api/prepare", methods=["POST"])
def api_prepare():
    """Kiểm tra và chuẩn bị dữ liệu PaySim."""
    csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
    mapping_path = os.path.join(DATA_DIR, "account_mapping.csv")
    
    # Nếu đã có dữ liệu, bỏ qua bước import (tránh ghi đè trong lúc demo)
    if os.path.exists(csv_path) and os.path.exists(mapping_path):
        try:
            import csv as csvlib
            with open(csv_path, "r", encoding="utf-8") as f:
                total = sum(1 for _ in csvlib.DictReader(f))
            with open(mapping_path, "r", encoding="utf-8") as f:
                mapping_total = sum(1 for _ in csvlib.DictReader(f))
            return jsonify({
                "success": True,
                "data": {
                    "message": f"Dữ liệu đã sẵn sàng ({total:,} giao dịch). Bỏ qua bước import.",
                    "total_transactions": total,
                    "mapping_rows": mapping_total,
                    "partition_rule": "HomeNode(FromAccount) = FromAccount % 3",
                    "num_nodes": NUM_NODES,
                    "skipped": True,
                }
            })
        except Exception as e:
            pass  # Tiếp tục thực hiện chạy lại các script nếu bị lỗi
    
    # Nếu chưa có dữ liệu, chạy script import
    try:
        script_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
        import_script = os.path.join(script_dir, "import_paysim.py")
        inject_script = os.path.join(script_dir, "inject_fraud_cycles.py")
        
        env = dict(os.environ, PYTHONIOENCODING="utf-8")
        subprocess.run([sys.executable, import_script], check=True, env=env)
        subprocess.run([sys.executable, inject_script], check=True, env=env)
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError("Không tìm thấy file dữ liệu PaySim sau khi chuẩn bị.")
        if not os.path.exists(mapping_path):
            raise FileNotFoundError("Không tìm thấy file ánh xạ tài khoản sau khi chuẩn bị.")
            
        import csv as csvlib
        with open(csv_path, "r", encoding="utf-8") as f:
            total = sum(1 for _ in csvlib.DictReader(f))
        with open(mapping_path, "r", encoding="utf-8") as f:
            mapping_total = sum(1 for _ in csvlib.DictReader(f))

        return jsonify({
            "success": True, 
            "data": {
                "message": "Dataset PaySim đã được import và cấy fraud rings thành công.",
                "total_transactions": total,
                "mapping_rows": mapping_total,
                "partition_rule": "HomeNode(FromAccount) = FromAccount % 3",
                "num_nodes": NUM_NODES,
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ──────────────────────────────────────────────
# API: Phân mảnh Đồ thị (Graph Partitioning)
# ──────────────────────────────────────────────

@app.route("/api/partition", methods=["POST"])
def api_partition():
    """Phân mảnh đồ thị giao dịch và tính toán các chỉ số cấu trúc (topology metrics)."""
    global last_partition_result, current_config

    params = request.json or {}
    num_partitions = params.get("num_partitions", current_config["num_partitions"])
    current_config["num_partitions"] = num_partitions

    csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
    if not os.path.exists(csv_path):
        return jsonify({"success": False, "error": "Không tìm thấy file dữ liệu. Hãy đảm bảo dữ liệu PaySim đã được chuẩn bị."}), 400

    try:
        result = load_and_partition_graph(csv_path, num_partitions=num_partitions)
        last_partition_result = result
        return jsonify({"success": True, "data": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ──────────────────────────────────────────────
# API: Quản lý Máy trạm (Node Management)
# ──────────────────────────────────────────────

@app.route("/api/nodes/start", methods=["POST"])
def api_start_nodes():
    """Khởi động tất cả các máy chủ node phân tán."""
    global node_processes

    results = []
    node_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node.py")

    for nid in range(NUM_NODES):
        if nid in node_processes and node_processes[nid].poll() is None:
            results.append({"node_id": nid, "status": "already_running", "pid": node_processes[nid].pid})
            continue

        log_file_path = os.path.join(DATA_DIR, f"node_{nid}.log")
        log_file = open(log_file_path, "w", encoding="utf-8")

        env = dict(os.environ, PYTHONIOENCODING="utf-8")
        p = subprocess.Popen(
            [sys.executable, node_script, str(nid), DATA_DIR],
            stdout=log_file,
            stderr=log_file,
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
        node_processes[nid] = p
        results.append({"node_id": nid, "status": "started", "pid": p.pid, "port": BASE_PORT + nid})

    # Kiểm tra sức khỏe bằng cách poll nhiều lần thay vì sleep cố định
    # Lý do: node phải tải file partition (16000+ cạnh/node), có thể mất 2-4 giây
    for r in results:
        nid = r["node_id"]
        r["health"] = wait_for_node(NODES_CONFIG[nid])

    return jsonify({"success": True, "nodes": results})


@app.route("/api/nodes/stop", methods=["POST"])
def api_stop_nodes():
    """Dừng tất cả các máy chủ node phân tán."""
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
    """Lấy trạng thái sức khỏe của tất cả các node."""
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
                cycle_candidates = health_data.get("num_cycle_candidate_edges", 0)
                status["num_cycle_candidate_edges"] = cycle_candidates
            else:
                status["health"] = "unhealthy"
        except requests.exceptions.RequestException:
            status["health"] = "offline"

        statuses.append(status)

    return jsonify({"nodes": statuses})


@app.route("/api/nodes/info/<int:nid>", methods=["GET"])
def api_node_info(nid):
    """Lấy mẫu dữ liệu đỉnh đang được lưu tại node (proxy tới /info của node)."""
    if nid not in node_processes or node_processes[nid].poll() is not None:
        return jsonify({"success": False, "error": f"Node {nid} đang ngoại tuyến."}), 400
    
    try:
        resp = requests.get(f"{NODES_CONFIG[nid]}/info", timeout=2.0)
        if resp.status_code == 200:
            return jsonify({"success": True, "data": resp.json()})
        else:
            return jsonify({"success": False, "error": f"Node {nid} phản hồi mã trạng thái {resp.status_code}"}), 500
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": str(e)}), 500



@app.route("/api/nodes/kill/<int:nid>", methods=["POST"])
def api_kill_node(nid):
    """
    Dừng một node cụ thể để giả lập sự cố mất mát/hỏng hóc node.

    Tham chiếu Giáo trình - Chương 5 (Tính tin cậy / Xử lý lỗi):
    - Chứng minh khả năng chịu lỗi trong hệ thống phân tán.
    - Các node còn lại vẫn hoạt động bình thường,
      chỉ các đường đi yêu cầu đi qua node bị dừng mới bị thất bại (partial results).
    """
    if nid not in node_processes:
        return jsonify({"success": False, "error": f"Node {nid} không được quản lý."}), 404

    p = node_processes[nid]
    if p.poll() is not None:
        return jsonify({"success": False, "error": f"Node {nid} đã được dừng trước đó."}), 400

    try:
        p.terminate()
        p.wait(timeout=5)
        return jsonify({"success": True, "node_id": nid, "status": "killed"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/nodes/restart/<int:nid>", methods=["POST"])
def api_restart_node(nid):
    """Khởi động lại một node cụ thể sau khi đã bị dừng."""
    node_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node.py")
    log_file_path = os.path.join(DATA_DIR, f"node_{nid}.log")

    # Dừng node nếu vẫn còn đang chạy
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
        result = detect_fraud_rings(NODES_CONFIG)
        result = enrich_cycle_amounts(result)
        last_detection_result = result
        return jsonify({"success": True, "data": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/metadata", methods=["GET"])
def api_get_metadata():
    import csv
    mapping_path = os.path.join(DATA_DIR, "account_mapping.csv")
    if not os.path.exists(mapping_path):
        return jsonify({"success": False, "error": "Account mapping file not found"}), 404
        
    metadata = {}
    with open(mapping_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            metadata[row["AccountID"]] = row
            
    return jsonify({"success": True, "metadata": metadata})



# ──────────────────────────────────────────────
# API: Full Pipeline
# ──────────────────────────────────────────────

@app.route("/api/pipeline", methods=["POST"])
def api_full_pipeline():
    """
    Chạy toàn bộ pipeline: kiểm tra dữ liệu -> phân mảnh -> khởi động node -> dò tìm.
    Đây là endpoint dùng cho việc chạy thử nghiệm nhanh chỉ với một cú click chuột.
    """
    params = request.json or {}

    steps = []

    try:
        csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
        mapping_path = os.path.join(DATA_DIR, "account_mapping.csv")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                "Thiếu file data/financial_transactions.csv. Vui lòng chạy: "
                "python scripts/import_paysim.py, tiếp theo là python scripts/inject_fraud_cycles.py"
            )
        if not os.path.exists(mapping_path):
            raise FileNotFoundError(
                "Thiếu file data/account_mapping.csv. Vui lòng chạy: python scripts/import_paysim.py"
            )
        steps.append({
            "step": "check_data",
            "success": True,
            "data": {
                "message": "Đã tìm thấy bộ dữ liệu PaySim đã được chuẩn bị sẵn.",
                "transactions_file": csv_path,
                "mapping_file": mapping_path,
            },
        })

        # Step 2: Partition
        part_result = load_and_partition_graph(
            csv_path, 
            num_partitions=params.get("num_partitions", current_config["num_partitions"])
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
            env = dict(os.environ, PYTHONIOENCODING="utf-8")
            p = subprocess.Popen(
                [sys.executable, node_script, str(nid), DATA_DIR],
                stdout=log_file,
                stderr=log_file,
                env=env,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
            )
            node_processes[nid] = p

        node_statuses = []
        for nid in range(NUM_NODES):
            health = wait_for_node(NODES_CONFIG[nid])
            node_statuses.append({
                "node_id": nid,
                "health": health,
            })

        steps.append({"step": "start_nodes", "success": True, "data": {"nodes": node_statuses}})

        # Step 4: Detect
        global last_detection_result
        detection_result = detect_fraud_rings(NODES_CONFIG)
        detection_result = enrich_cycle_amounts(detection_result)
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
    """Trả về một phần nhỏ của đồ thị phù hợp cho việc hiển thị bằng D3.js."""
    import csv
    import random
    
    csv_path = os.path.join(DATA_DIR, "financial_transactions.csv")
    mapping_path = os.path.join(DATA_DIR, "account_mapping.csv")
    
    if not os.path.exists(csv_path):
        return jsonify({"success": False, "error": "Không tìm thấy dữ liệu"}), 404

    if not last_detection_result or not last_detection_result.get("cycles"):
        return jsonify({
            "success": True,
            "nodes": [],
            "edges": [],
            "cycles": [],
            "message": "Hãy thực hiện bước dò tìm trước khi mở xem đồ thị.",
        })

    metadata = {}
    if os.path.exists(mapping_path):
        with open(mapping_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                metadata[row["AccountID"]] = row

    fraud_nodes = set()
    fraud_edges_set = set()
    cycles_list = []

    for c in last_detection_result["cycles"]:
        cycle_nodes = [str(v) for v in c["cycle"]]
        cycles_list.append(cycle_nodes)
        for i in range(len(cycle_nodes)):
            u = cycle_nodes[i]
            v = cycle_nodes[(i + 1) % len(cycle_nodes)]
            fraud_nodes.add(u)
            fraud_nodes.add(v)
            fraud_edges_set.add((u, v))

    fraud_edges_list = []
    normal_edges_pool = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = str(row["FromAccount"])
            v = str(row["ToAccount"])
            edge_key = (u, v)

            if edge_key in fraud_edges_set:
                fraud_edges_list.append({
                    "source": u,
                    "target": v,
                    "amount": float(row["Amount"]),
                    "is_fraud": True
                })
            else:
                normal_edges_pool.append({
                    "source": u,
                    "target": v,
                    "amount": float(row["Amount"]),
                    "is_fraud": False
                })

    edges_list = []
    # Đưa tất cả cạnh của các chu trình được phát hiện vào (bắt buộc phải hiển thị)
    edges_list.extend(fraud_edges_list)

    # Lấy mẫu các cạnh giao dịch bình thường xung quanh làm nền (cả liên node và nội bộ)
    MAX_NORMAL_EDGES = 100
    sampled_normal = random.sample(normal_edges_pool, min(MAX_NORMAL_EDGES, len(normal_edges_pool)))
    edges_list.extend(sampled_normal)

    # Thu thập các đỉnh từ danh sách cạnh
    unique_node_ids = set()
    for e in edges_list:
        unique_node_ids.add(e["source"])
        unique_node_ids.add(e["target"])
        # Đảm bảo các đỉnh thuộc chu trình gian lận được đánh dấu
        if e["is_fraud"]:
            fraud_nodes.add(e["source"])
            fraud_nodes.add(e["target"])

    nodes = []
    num_partitions = current_config["num_partitions"]
    for vertex_id in sorted(unique_node_ids, key=lambda val: int(val) if val.isdigit() else 0):
        meta = metadata.get(vertex_id, {})
        is_f = vertex_id in fraud_nodes
        node_data = {
            "id": vertex_id,
            "label": vertex_id,
            "shard": int(vertex_id) % num_partitions if vertex_id.isdigit() else 0,
            "is_fraud": is_f,
            "metadata": meta,
        }
        node_data.update(meta)
        nodes.append(node_data)

    return jsonify({
        "success": True,
        "nodes": nodes,
        "edges": edges_list,
        "cycles": cycles_list,
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
    print("  PHÁT HIỆN CHU TRÌNH GIAN LẬN PHÂN TÁN - WEB DASHBOARD")
    print("  Mở địa chỉ http://localhost:5000 trên trình duyệt của bạn")
    print("=" * 60)
    app.run(host="localhost", port=5000, debug=False, threaded=True)
