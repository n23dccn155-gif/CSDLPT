# 🔍 Distributed Fraud Ring Detection

> **INT1414 - Distributed Database Systems | Project #136**  
> Category 14: Graph & Multi-Model Distributed DBs  
> Semester 2, 2025-2026

## 📋 Overview

This project implements a **Distributed Graph Pattern Matching** system to detect **Fraud Rings** in financial transaction data. A fraud ring is a cycle of 4 accounts transferring the same amount of money in a loop (A→B→C→D→A), indicative of money laundering or circular debt schemes.

The system distributes the transaction graph across **3 independent nodes** (Shared-Nothing architecture) and performs **Distributed DFS (Depth-First Search)** with cross-shard path expansion to detect fraud cycles that span multiple nodes.

### 🌟 Key Features & Updates
- **Real-world Kaggle Dataset**: Uses the PaySim Synthetic Financial Dataset, mapping string accounts to integers for optimized graph partitioning.
- **Multi-Model Integration**: Combines distributed graph structure (`financial_transactions.csv`) with relational metadata (`account_metadata.csv`) which is joined dynamically during visualization.
- **Centralized vs Distributed Benchmark**: Proves the performance characteristics and network cost models of distributed databases directly against a single-node baseline.
- **Interactive Graph Visualization**: D3.js-powered visualizer for fraud rings and shards.
- **Smart Partitioning**: Compare Hash-based Partitioning with Block-aware/Graph-aware Partitioning to observe the reduction in edge-cuts and network traffic.

## 🏗️ Architecture

```text
┌─────────────────────────────────────────────┐
│         Web Dashboard (Port 5000)           │
│         Coordinator / Master Node           │
└──────────┬──────────┬──────────┬────────────┘
           │          │          │  (Parallel Query)
    ┌──────┴───┐ ┌────┴─────┐ ┌─┴──────────┐
    │  Node 0  │ │  Node 1  │ │   Node 2   │
    │ Port 5001│ │ Port 5002│ │  Port 5003 │
    └────┬─────┘ └────┬─────┘ └─────┬──────┘
         │            │             │
         └── HTTP POST /expand_path ┘
              (Cross-Shard DFS)
```

## 📚 Textbook References

Based on **Principles of Distributed Database Systems** (Özsu & Valduriez, 4th Edition, 2020):

| Chapter | Concept Applied |
|---------|----------------|
| Ch.1 Introduction | Shared-Nothing Architecture, Site/Node independence |
| Ch.2 Distributed Database Design | Hash-based vs Block-aware Partitioning, Horizontal Fragmentation |
| Ch.4 Query Processing | Cost Model (I/O + CPU + Communication), Distributed query optimization |
| Ch.5 Transaction Management | Fault tolerance, graceful degradation under node failure |
| Ch.6 Data Replication | Vertex Replication Factor analysis |
| Ch.8 Parallel Database Systems | Parallel query dispatch via ThreadPoolExecutor |

## ⚡ Quick Start

### Prerequisites
- Python 3.8+
- pip

### Installation

```bash
cd fraud-ring-detection
pip install -r requirements.txt
```

### Dataset Preparation (PaySim)
*Note: The system supports both synthetic data and real PaySim data.*
To prepare the real PaySim dataset (which is required for the full experience):
```bash
# 1. Convert PaySim string accounts to integers and extract metadata
python scripts/import_paysim.py

# 2. Inject controlled fraud cycles for verification
python scripts/inject_fraud_cycles.py
```

### Run the System

```bash
python backend/app.py
```
Open **http://localhost:5000** in your browser.

Select **Dataset Mode: Real PaySim Dataset** and run the pipeline!

## 📂 Project Structure

```text
fraud-ring-detection/
├── README.md
├── huong_dan_bao_ve_do_an.md    # Defense guide (Vietnamese)
├── requirements.txt
├── backend/
│   ├── app.py              # Web API server & dashboard
│   ├── generate_data.py     # Synthetic transaction data generator
│   ├── partition.py         # Graph partitioning engine (Hash & Block-aware)
│   ├── node.py              # Distributed node server (Flask)
│   └── coordinator.py       # Query coordinator with parallel dispatch (Distributed & Centralized)
├── scripts/
│   ├── import_paysim.py    # Preprocesses Kaggle PaySim data
│   └── inject_fraud_cycles.py
├── frontend/
│   ├── index.html           # Dashboard UI
│   ├── css/style.css        # Premium dark theme
│   └── js/
│       ├── app.js           # Application logic
│       └── charts.js        # Chart.js visualizations
└── data/                    # Generated data & benchmark JSON (gitignored)
    ├── financial_transactions.csv        # Main graph dataset
    └── account_metadata.csv              # Relational metadata
```

## 🔬 Key Algorithms

### Partitioning Strategies (Ch.2)
- **Hash Partitioning (Baseline)**: `HomeNode(V) = V % K`. Scatters data randomly, good load balance, high network cost.
- **Block-aware Partitioning (Smart)**: `HomeNode(V) = (V // 50) % K`. Groups temporally close nodes into the same shard, minimizing edge-cuts and network traffic.

### Distributed DFS (Path Expansion)
1. Each node scans its local edges for potential cycle starts
2. When a path reaches a vertex on another node, an HTTP POST forwards the partial path
3. The receiving node continues the DFS locally
4. **Minimum-ID Rule**: Only starts cycles from the smallest vertex ID to avoid duplicates

### Cost Model
```
Total_Cost = I/O_Cost + CPU_Cost + Communication_Cost
```
Communication_Cost (network messages) is the dominant factor, tracked in real-time.

## 📊 Grading Criteria (Category 14)

| Criteria | Implementation | Status |
|----------|---------------|--------|
| Graph Partitioning | Hash-based vs Block-aware with Edge-Cut analysis | ✅ Excellent |
| Traversal Logic | Distributed DFS with cross-shard path expansion | ✅ Excellent |
| Multi-Model Integration| Graph edges (`financial_transactions`) + Relational (`metadata`) | ✅ Excellent |
| Topology Analysis | Edge-Cut Ratio, Centralized vs Distributed benchmark | ✅ Excellent |

## 👤 Author

- **Student ID**: N23DCCN155
- **Name**: Đặng Văn Hiệp
- **Course**: INT1414 - Distributed Database Systems
- **Instructor**: TS. Hà Thanh Lê
