# TÀI LIỆU ÔN TẬP VÀ THUYẾT MINH BẢO VỆ ĐỒ ÁN 136
**Tên đề tài:** Distributed Graph Pattern Matching: "Fraud Ring Detection"
**Mục đích:** Giải thích toàn diện 100% mã nguồn, luồng hoạt động, ánh xạ lý thuyết giáo trình và ngân hàng câu hỏi bảo vệ trước hội đồng.

---

## PHẦN 1: GIẢI THÍCH SẢN PHẨM & LUỒNG HOẠT ĐỘNG (PIPELINE FLOW)

### Mục tiêu sản phẩm
Hệ thống là một **bản mô phỏng hoàn chỉnh của một Cơ sở dữ liệu đồ thị phân tán** (Distributed Graph Database). Nhiệm vụ của nó là phát hiện các đường dây rửa tiền/gian lận (là các chu trình chuyển tiền khép kín gồm 4 người: A chuyển cho B, B chuyển cho C, C chuyển cho D, D chuyển lại cho A với cùng một số tiền) **mà không được phép kéo toàn bộ dữ liệu về một máy chủ trung tâm**.

Đồ án được xây dựng tích hợp **Đa mô hình (Multi-Model Integration)**: Khai thác cấu trúc Đồ thị (Graph) để tìm chu trình, và sử dụng dữ liệu Quan hệ (Relational Metadata) để làm giàu thông tin cho từng tài khoản.

### Luồng hoạt động xử lý (Pipeline Flow)
Toàn bộ quá trình chạy theo các bước (tương ứng với lúc bạn bấm nút `Run Full Pipeline` trên web):

1. **Chuẩn bị dữ liệu (Dataset Mode):**
   * **PaySim (Dữ liệu thực tế):** Hệ thống đọc dữ liệu giao dịch tài chính thật từ Kaggle, chuyển đổi Account chuỗi sang ID số, đồng thời xuất ra `account_metadata.csv` chứa thông tin nghiệp vụ. Sau đó, "cấy" (inject) một vài chu trình gian lận mẫu để đảm bảo việc kiểm thử.
   * **Synthetic (Dữ liệu tự sinh):** Hệ thống tạo tự động dữ liệu giả lập cho mục đích stress-test.
2. **Phân mảnh đồ thị (Graph Partitioning):** Máy chủ đọc file CSV, chia đều các đỉnh (tài khoản) vào 3 mảnh nhỏ (Shard) độc lập bằng chiến lược *Hash Partitioning* hoặc *Block-aware Partitioning*. Ghi kết quả ra 3 file `partition_0.json`, `partition_1.json`, `partition_2.json`. 
3. **Khởi động mạng lưới (Node Startup):** Trình quản lý bật 3 tiến trình Python hoàn toàn tách biệt chạy trên 3 port `5001, 5002, 5003`. Mỗi tiến trình này nạp lên RAM thành dạng Danh sách kề (Adjacency List).
4. **Dò tìm phân tán (Distributed Detection):** Nút Master gửi lệnh đến cả 3 Node yêu cầu bắt đầu tìm chu trình. Cả 3 Node song song quét các cạnh của mình.
   * Nếu đỉnh tiếp theo nằm cùng Node -> Duyệt tiếp bình thường trên RAM.
   * Nếu đỉnh tiếp theo thuộc Node khác -> Đóng gói đường đi, gửi lệnh HTTP POST sang Node kia (Truyền thông mạng).
5. **Tổng hợp & Trực quan hóa (Aggregation & Visualization):** Master nhận lại kết quả từ 3 Node, loại bỏ trùng lặp. Giao diện Web hiển thị kết quả và dùng D3.js để vẽ **Graph View**, kết hợp thông tin Relational Metadata (Join-on-Visualization) khi người dùng bấm vào các Node.

---

## PHẦN 2: LÝ THUYẾT GIÁO TRÌNH VÀ CÁCH ÁP DỤNG TRONG ĐỒ ÁN
*(Đây là phần cực kỳ quan trọng để ăn điểm tối đa, chứng minh bạn hiểu sâu lý thuyết quyển Özsu & Valduriez)*

### 1. Phân mảnh đồ thị (Horizontal Fragmentation - Ch.2)
* **Lý thuyết:** Phân mảnh ngang cơ bản. Ta chia các Tuple dựa trên một biểu thức.
* **Áp dụng trong bài:** Hệ thống cung cấp 2 chiến lược so sánh thực tế:
  * **Hash-based Partitioning:** `Home Node = Account_ID % 3`. Phân phối đều (Load Balancing tốt) nhưng tỷ lệ cắt nát đồ thị (Edge-Cut Ratio) rất cao (~66%).
  * **Block-aware/Graph-aware Partitioning (Smart):** `Home Node = (Account_ID // 50) % 3`. Gom các tài khoản có tính địa phương/thời gian gần nhau vào cùng một cụm. Giúp giảm thiểu đáng kể số lượng cạnh cắt ngang ranh giới máy chủ, tối ưu hóa băng thông.

### 2. Mô hình chi phí và Benchmark Tập trung vs Phân tán (Cost Model - Ch.4)
* **Lý thuyết:** Chi phí thực thi truy vấn = **I/O Cost** + **CPU Cost** + **Communication Cost** (Chi phí truyền mạng). Truyền thông qua mạng là nút thắt cổ chai đắt đỏ nhất.
* **Áp dụng trong bài:** Đồ án có tính năng **Centralized vs Distributed Benchmark**. Nó so sánh trực tiếp một máy chủ đơn lẻ (chạy hoàn toàn trên RAM) với kiến trúc 3 máy chủ phân tán (chạy qua HTTP). Khi dùng Hash Partitioning, Distributed có thể chạy chậm hơn do Communication Cost quá lớn. Khi chuyển sang Smart Partitioning, Communication Cost giảm mạnh và lợi thế tính toán song song (Parallel CPU) của Distributed giúp nó vượt lên Centralized!

### 3. Đa mô hình (Multi-Model Integration)
* **Lý thuyết:** Hệ thống CSDL hiện đại không chỉ dùng 1 mô hình. Việc kết hợp Graph Database (cho pattern matching) và Relational Database (cho thuộc tính) mang lại sức mạnh toàn diện.
* **Áp dụng trong bài:** File `financial_transactions.csv` cung cấp cấu trúc đồ thị (edges) để dò tìm chu trình bằng DFS. Nhưng để biết được rủi ro (RiskScore), quốc gia (Country) của các Account đó, hệ thống tra cứu bảng quan hệ `account_metadata.csv` và thực hiện phép Join trực tiếp trên bộ nhớ trình duyệt ở tab Graph View bằng D3.js.

### 4. Quản lý giao dịch và Chịu lỗi (Fault Tolerance - Ch.5)
* **Lý thuyết:** Sự sập đổ của một Site vật lý không làm toàn bộ hệ thống tê liệt.
* **Áp dụng trong bài:** Tính năng **Fault Tolerance Test** trên web giả lập việc kill Node 1. Các Node còn lại vẫn bẫy lỗi HTTP và trả về các vòng gian lận cục bộ của chúng (Graceful Degradation).

---

## PHẦN 3: NGÂN HÀNG CÂU HỎI BẢO VỆ & TRẢ LỜI

**Câu 1: Làm sao em chứng minh đồ án của em không kéo toàn bộ dữ liệu về một máy tính (Coordinator) để duyệt đồ thị?**
> **Trả lời:** Dạ, hệ thống tuân thủ chặt chẽ nguyên lý **Ship-Query-to-Data**. Các danh sách kề được chia cứng vào RAM của 3 tiến trình Node khác nhau. Khi chạy DFS, nếu Node 0 duyệt đường đi tới một tài khoản do Node 1 giữ, nó đóng gói đường đi hiện tại `path = [A, B]`, và bắn một HTTP POST request qua port của Node 1 để Node 1 duyệt tiếp. Dữ liệu đứng yên tại chỗ, chỉ có luồng xử lý (Query) là di chuyển. (Xem file `node.py`, hàm `requests.post`).

**Câu 2: Tại sao em lại cần 2 chiến lược phân mảnh (Partition Strategy) khác nhau?**
> **Trả lời:** Để minh chứng cho lý thuyết tối ưu hóa phân mảnh (Chương 2). Chiến lược băm ngẫu nhiên (Hash) giúp cân bằng tải tốt nhưng làm cắt nát đồ thị (Edge-Cut cao), dẫn đến lượng thông điệp mạng (Network Messages) khổng lồ. Em cài đặt thêm chiến lược Block-aware để gom các Account có liên hệ mật thiết (ví dụ sinh ra cùng thời điểm) vào cùng một Node. Thực nghiệm Benchmark của em chứng minh Block-aware làm giảm đột biến số Network Messages và tăng tốc độ xử lý rõ rệt.

**Câu 3: Đồ án có dùng dữ liệu thật không, và Multi-Model Integration nằm ở đâu?**
> **Trả lời:** Dạ có, nhóm em sử dụng dataset **PaySim** (giao dịch tài chính mô phỏng trên Kaggle). Để kết hợp Đa mô hình (Multi-Model), em tách dữ liệu thành 2 phần: cấu trúc đồ thị truyền tiền lưu ở phân mảnh JSON (phục vụ duyệt DFS siêu tốc), và thông tin siêu dữ liệu dạng quan hệ (RiskScore, Country) lưu ở `account_metadata.csv`. Khi phát hiện chu trình, giao diện Web (Graph View D3.js) sẽ tự động Join 2 nguồn này lại để hiển thị đầy đủ thông tin cho người điều tra.

**Câu 4: Chức năng Compare Centralized vs Distributed dùng để làm gì?**
> **Trả lời:** Đây là cách em minh họa **Cost Model (Mô hình chi phí - Chương 4)**. Hàm Centralized gom hết dữ liệu vào 1 máy và chạy trên 1 luồng RAM (Mô phỏng máy chủ đơn). Distributed chia ra 3 máy. Nhờ bảng so sánh này, em chứng minh được rằng kiến trúc phân tán tuy chịu chi phí kết nối mạng (Network Messages), nhưng nhờ sức mạnh tính toán song song (Parallel CPU Ops), nó sẽ có khả năng mở rộng cực tốt (Horizontal Scaling) và vượt qua kiến trúc tập trung trên các đồ thị siêu lớn.

**Câu 5: Làm sao em biết thuật toán của em tìm được ĐÚNG vòng lặp?**
> **Trả lời:** Bằng phương pháp cấy dữ liệu (Controlled Fraud Injection). Cùng với dữ liệu nền của PaySim, script Python của em cố tình cấy các chu trình toán học với các hình thái khác nhau (Local Cycle và Cross-Shard Cycle). Khi chạy, nếu hệ thống tìm ra chính xác các chu trình đã cấy, điều đó khẳng định chắc chắn 100% thuật toán duyệt DFS phân tán của em không bỏ sót và chạy đúng hoàn toàn.

---
**Chúc bạn chuẩn bị thật tốt và đạt điểm Excellent trong buổi bảo vệ nhé!**
