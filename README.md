# Distributed Fraud Ring Detection (Phát hiện đường dây gian lận phân tán)

> **INT1414 - Distributed Database Systems (Hệ quản trị CSDL phân tán) | Đồ án #136**
> Chuyên đề 14: Graph & Multi-Model Distributed DBs (CSDL Đồ thị & Đa mô hình phân tán)
> Học kỳ 2, 2025-2026

## Tổng quan

Dự án này triển khai một hệ thống **Distributed Graph Pattern Matching (Khớp mẫu đồ thị phân tán)** để phát hiện các **Fraud Rings (Đường dây gian lận)** trong dữ liệu giao dịch tài chính. Một đường dây gian lận là một chu trình (cycle) gồm các tài khoản chuyển tiền cho nhau tạo thành một vòng khép kín (Ví dụ: A→B→C→D→A), dấu hiệu đặc trưng của hành vi rửa tiền hoặc tín dụng đen.

Hệ thống phân tán đồ thị giao dịch lên **3 máy chủ độc lập (independent nodes)** hoạt động theo kiến trúc **Shared-Nothing (Không chia sẻ tài nguyên)** và thực hiện thuật toán **Distributed DFS (Depth-First Search - Tìm kiếm theo chiều sâu phân tán)** kết hợp với Cross-shard path expansion (Mở rộng đường đi liên mảnh) để phát hiện các chu trình gian lận trải dài trên nhiều máy chủ.

### Tính năng chính (Key Features)

- **Real-world Kaggle Dataset (Dữ liệu thực tế)**: Sử dụng tập dữ liệu giao dịch tài chính PaySim, ánh xạ (mapping) tài khoản chuỗi sang số nguyên để tối ưu hóa quá trình phân mảnh đồ thị (graph partitioning).
- **Lightweight Graph + Relational Mapping (Đồ thị nhẹ + Ánh xạ quan hệ)**: Kết hợp cấu trúc đồ thị phân tán (`financial_transactions.csv`) với bảng ánh xạ quan hệ (`account_mapping.csv`) để nối `AccountID` nội bộ về mã tài khoản gốc của PaySim khi hiển thị.
- **Interactive Graph Visualization (Hiển thị đồ thị tương tác)**: Sử dụng D3.js để vẽ trực quan các đường dây gian lận và các mảnh dữ liệu (shards).
- **Fault Tolerance (Khả năng chịu lỗi)**: Có khả năng tiếp tục truy vấn dò tìm ngay cả khi một trong các máy chủ phân mảnh bị tắt (offline), hệ thống vẫn trả về các kết quả cục bộ (partial results).
- **Streamlined Architecture (Kiến trúc tinh gọn)**: Sử dụng thuần túy chiến lược Hash-based Partitioning (Phân mảnh theo hàm băm) để chứng minh một hệ thống phân tán cơ bản, rõ ràng và có độ ổn định cao.

## Kiến trúc Hệ thống (Architecture)

```text
┌─────────────────────────────────────────────┐
│         Web Dashboard (Cổng 5000)           │
│         Coordinator / Master Node           │
│         (Máy chủ Điều phối)                 │
└──────────┬──────────┬──────────┬────────────┘
           │          │          │  (Parallel Query - Truy vấn song song)
    ┌──────┴───┐ ┌────┴─────┐ ┌─┴──────────┐
    │  Node 0  │ │  Node 1  │ │   Node 2   │
    │ Cổng 5001│ │ Cổng 5002│ │ Cổng 5003  │
    └────┬─────┘ └────┬─────┘ └─────┬──────┘
         │            │             │
         └── HTTP POST /expand_path ┘
              (Cross-Shard DFS - Duyệt liên mảnh)
```

## Tham chiếu Giáo trình (Textbook References)

Dựa trên giáo trình **Principles of Distributed Database Systems** (Özsu & Valduriez, 4th Edition, 2020):

| Chương                                                            | Khái niệm áp dụng                                                                                                                |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Ch.1 Introduction (Giới thiệu)                                    | Shared-Nothing Architecture (Kiến trúc không chia sẻ), Site/Node independence (Tính độc lập dữ liệu trên các máy trạm) |
| Ch.2 Distributed Database Design (Thiết kế CSDL phân tán)       | Hash-based Partitioning (Phân mảnh theo hàm băm), Horizontal Fragmentation (Phân mảnh ngang)                                   |
| Ch.4 Query Processing (Xử lý truy vấn)                           | Cost Model (Mô hình chi phí: Communication vs Local Ops), Distributed query optimization (Tối ưu hóa truy vấn phân tán)     |
| Ch.5 Reliability / Failure Handling (Tính tin cậy / Xử lý lỗi) | Mô phỏng node failure, kill/restart node, xử lý partial results khi node offline                                                 |
| Ch.10 Graph Processing (Xử lý đồ thị)                          | Graph partitioning, edge-cut ratio, boundary vertices (Phân mảnh đồ thị, tỷ lệ cạnh cắt, đỉnh biên)                      |
| Ch.8 Parallel Database Systems (Hệ thống CSDL song song)          | Parallel query dispatch (Gửi truy vấn song song) thông qua ThreadPoolExecutor                                                     |

## Hướng dẫn chạy nhanh (Quick Start)

### Yêu cầu hệ thống (Prerequisites)

- Python 3.8+
- pip

### Cài đặt (Installation)

```bash
cd fraud-ring-detection
pip install -r requirements.txt
```

### Chuẩn bị dữ liệu (Dataset Preparation - PaySim)

*Ghi chú: Repository đã bao gồm sẵn một tập dữ liệu mẫu đã được xử lý (processed sample dataset) trong thư mục `data/` để có thể chạy demo nhanh mà không cần chạy lại các script này.*

Chạy các kịch bản sau một lần duy nhất để chuẩn bị dữ liệu PaySim thật và cấy (inject) các chu trình lừa đảo vào để demo:

```bash
# 1. Chuyển đổi mã tài khoản PaySim sang số nguyên và trích xuất siêu dữ liệu
python scripts/import_paysim.py

# 2. Cấy các chu trình lừa đảo mẫu vào dữ liệu để kiểm chứng thuật toán
python scripts/inject_fraud_cycles.py
```

### Chạy Hệ thống (Run the System)

```bash
python backend/app.py
```

Mở trình duyệt và truy cập **http://localhost:5000/**.

Trên giao diện web, chạy lần lượt **4 bước** trong tab **Pipeline**:

1. **Kiểm tra dữ liệu** — Xác nhận file CSV đã sẵn sàng
2. **Phân mảnh đồ thị** — Phân chia đồ thị thành 3 mảnh theo Hash Partitioning
3. **Khởi động Node** — Khởi động 3 máy chủ phân tán (cổng 5001–5003)
4. **Dò tìm chu trình** — Thực thi Distributed DFS để phát hiện fraud rings

## Cấu trúc Dự án (Project Structure)

```text
fraud-ring-detection/
├── README.md
├── project_explanation.md       # Giải thích chi tiết logic từng file
├── huong_dan_bao_ve_do_an.md    # Hướng dẫn bảo vệ đồ án
├── requirements.txt
├── backend/
│   ├── app.py              # Web API server & Quản lý tiến trình (Process manager)
│   ├── partition.py         # Bộ máy phân mảnh đồ thị (Graph partitioning engine)
│   ├── node.py              # Máy chủ con phân tán (Distributed node server)
│   └── coordinator.py       # Máy chủ điều phối với cơ chế gửi lệnh song song
├── scripts/
│   ├── import_paysim.py    # Xử lý trước (Preprocess) dữ liệu Kaggle PaySim
│   └── inject_fraud_cycles.py # Thêm các chu trình gian lận mẫu vào dataset
├── frontend/
│   ├── index.html           # Giao diện hiển thị (Dashboard UI)
│   ├── css/style.css        # Giao diện làm đẹp CSS sáng màu
│   └── js/
│       ├── app.js           # Logic điều khiển ứng dụng
│       └── charts.js        # Vẽ biểu đồ trực quan (Chart.js)
└── data/                    # Dữ liệu
    ├── financial_transactions.csv        # Sample data đã xử lý (có sẵn trong repo)
    ├── account_mapping.csv               # Bảng ánh xạ AccountID -> OriginalAccount (có sẵn trong repo)
    └── partition_*.json                  # Tạo tự động khi chạy bước "Phân mảnh đồ thị"
```

## Thuật toán Cốt lõi (Key Algorithms)

### Chiến lược Phân mảnh (Partitioning Strategy - Ch.2)

- **Hash Partitioning (Phân mảnh theo hàm băm)**: `HomeNode(V) = V % K`. Phân tán dữ liệu ngẫu nhiên, tạo sự cân bằng tải (load balance) tốt, xử lý phân phối ID một cách đồng đều.

### Distributed DFS (Tìm kiếm theo chiều sâu phân tán)

1. Mỗi node quét các cạnh cục bộ (local edges) của nó để tìm điểm khởi đầu tiềm năng của chu trình.
2. Khi đường đi (path) chạm đến một đỉnh (vertex) nằm trên Node khác, hệ thống sẽ gửi HTTP POST để chuyển tiếp một phần của đường đi (partial path) sang máy đó (Cross-shard messaging).
3. Máy nhận được tin nhắn sẽ tiếp tục quá trình quét DFS cục bộ trên máy đó.
4. **Minimum-ID Rule (Quy tắc ID nhỏ nhất)**: Chỉ bắt đầu chu trình từ đỉnh có số ID nhỏ nhất để tránh việc ghi nhận trùng lặp cùng một chu trình.

## Tác giả

- **Mã sinh viên**: N23DCCN155
- **Họ và tên**: Đặng Văn Hiệp
- **Học phần**: INT1414 - Distributed Database Systems (Hệ quản trị CSDL phân tán)
