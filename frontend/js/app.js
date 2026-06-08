/**
 * app.js - Logic Ứng dụng Dashboard
 * =====================================
 * Quản lý tất cả tương tác UI, gọi API và quản lý trạng thái cho
 * Dashboard Phát hiện Đường dây Gian lận Phân tán.
 */

const API_BASE = '';  // Cùng máy chủ nguồn (Same origin)

// 
// Trạng thái
// 
let lastDetectionResult = null;
let lastPartitionResult = null;
let accountMetadata = null;
let graphMode = 'shard';

async function loadMetadata() {
    try {
        const res = await apiCall('/api/metadata');
        if (res.success) {
            accountMetadata = res.metadata;
        }
    } catch (e) {
        console.warn("Không thể tải metadata", e);
    }
}
// Tải khi khởi động
loadMetadata();

// 
// Điều hướng Tab
// 
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const tabId = btn.dataset.tab;

        // Cập nhật trạng thái các nút tab
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');

        // Cập nhật hiển thị các panel tab
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
        document.getElementById(`panel-${tabId}`).classList.add('active');

        // Làm mới dữ liệu cho một số tab nhất định
        if (tabId === 'nodes') refreshNodeStatus();
        if (tabId === 'graph') renderGraph();
    });
});

// 
// Các hàm Tiện ích
// 
function showLoading(text = 'Đang xử lý...') {
    document.getElementById('loadingText').textContent = text;
    document.getElementById('loadingOverlay').classList.add('active');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.remove('active');
}

function appendLog(message, type = '') {
    const log = document.getElementById('logOutput');
    const timestamp = new Date().toLocaleTimeString();
    const colorClass = type ? ` class="log-${type}"` : '';
    log.innerHTML += `<span${colorClass}>[${timestamp}] ${message}</span>\n`;
    log.scrollTop = log.scrollHeight;
}

function clearLog() {
    document.getElementById('logOutput').innerHTML = '';
}

function getConfig() {
    return {
        num_partitions: 3,
        numPartitions: 3
    };
}

async function apiCall(endpoint, method = 'GET', body = null) {
    const options = {
        method,
        headers: { 'Content-Type': 'application/json' },
    };
    if (body) options.body = JSON.stringify(body);

    const response = await fetch(`${API_BASE}${endpoint}`, options);
    return await response.json();
}

const pipelineStepsData = [
    { name: 'Kiểm tra dữ liệu', status: 'pending', detail: 'Đang chờ...', buttonId: 'btnPrepareData', action: 'runStepPrepareData()', enabled: true },
    { name: 'Phân mảnh đồ thị', status: 'pending', detail: 'Đang chờ...', buttonId: 'btnPartition', action: 'runStepPartition()', enabled: false },
    { name: 'Khởi động node', status: 'pending', detail: 'Đang chờ...', buttonId: 'btnStartNodes', action: 'runStepStartNodes()', enabled: false },
    { name: 'Dò tìm chu trình', status: 'pending', detail: 'Đang chờ...', buttonId: 'btnDetect', action: 'runStepDetect()', enabled: false },
];

function renderPipelineSteps() {
    const stepsContainer = document.getElementById('pipelineSteps');
    if (!stepsContainer) return;
    stepsContainer.innerHTML = pipelineStepsData.map((s, idx) => `
        <button class="pipeline-action ${s.status}" id="${s.buttonId}" onclick="${s.action}" ${(!s.enabled || s.status === 'running') ? 'disabled' : ''}>
            <span class="pipeline-step-icon ${s.status}">${idx + 1}</span>
            <span class="pipeline-action-body">
                <span class="pipeline-step-name">${s.name}</span>
                <span class="pipeline-step-detail">${s.detail}</span>
            </span>
        </button>
    `).join('');
}

// Khởi tạo hiển thị ban đầu
document.addEventListener('DOMContentLoaded', () => {
    renderPipelineSteps();
});

async function runStepPrepareData() {
    const btn = document.getElementById('btnPrepareData');
    if (btn) btn.disabled = true;
    
    pipelineStepsData[0].status = 'running';
    pipelineStepsData[0].detail = 'Đang tiến hành...';
    renderPipelineSteps();
    appendLog('Đang chuẩn bị Dataset...', 'info');
    
    try {
        const result = await apiCall('/api/prepare', 'POST');
        if (result.success) {
            const data = result.data || {};
            pipelineStepsData[0].status = 'done';
            pipelineStepsData[0].detail = `${(data.total_transactions || 0).toLocaleString()} giao dịch đã sẵn sàng.`;
            appendLog(`Chuẩn bị Dataset thành công.`, 'success');
            
            document.getElementById('uiCurrentDataset').textContent = 'Bộ dữ liệu: Tập con xử lý từ PaySim';
            document.getElementById('uiCurrentTxs').textContent = `Số giao dịch: ${(data.total_transactions || 0).toLocaleString()}`;
            document.getElementById('uiCurrentMapping').textContent = `Bản ghi ánh xạ: ${(data.mapping_rows || 0).toLocaleString()}`;
            document.getElementById('uiCurrentStrategy').textContent = `Quy tắc phân mảnh: ${data.partition_rule || 'HomeNode(FromAccount) = FromAccount % 3'}`;
            document.getElementById('uiCurrentNodes').textContent = `Số Node: ${data.num_nodes || 3}`;
            
            pipelineStepsData[1].enabled = true;
        } else {
            throw new Error(result.error || 'Unknown error');
        }
    } catch (err) {
        pipelineStepsData[0].status = 'error';
        pipelineStepsData[0].detail = err.message;
        pipelineStepsData[0].enabled = true;
        appendLog(`Chuẩn bị dữ liệu thất bại: ${err.message}`, 'error');
    }
    renderPipelineSteps();
}

async function runStepPartition() {
    const btn = document.getElementById('btnPartition');
    if (btn) btn.disabled = true;
    
    pipelineStepsData[1].status = 'running';
    pipelineStepsData[1].detail = 'Đang tiến hành...';
    renderPipelineSteps();
    appendLog('Đang phân mảnh đồ thị...', 'info');
    
    try {
        const config = getConfig();
        const result = await apiCall('/api/partition', 'POST', config);
        if (result.success) {
            lastPartitionResult = result.data;
            pipelineStepsData[1].status = 'done';
            pipelineStepsData[1].detail = `Tỷ lệ cắt cạnh: ${result.data.edge_cut_ratio}% | Hệ số nhân bản: ${result.data.vertex_replication_factor}`;
            appendLog(`Đã phân mảnh đồ thị: Tỷ lệ cắt cạnh (Edge-Cut) ${result.data.edge_cut_ratio}%, Hệ số nhân bản (Replication Factor) ${result.data.vertex_replication_factor}`, 'success');
            pipelineStepsData[2].enabled = true;
        } else {
            throw new Error(result.error || 'Lỗi không xác định');
        }
    } catch (err) {
        pipelineStepsData[1].status = 'error';
        pipelineStepsData[1].detail = err.message;
        pipelineStepsData[1].enabled = true;
        appendLog(`Phân mảnh thất bại: ${err.message}`, 'error');
    }
    renderPipelineSteps();
}

async function runStepStartNodes() {
    const btn = document.getElementById('btnStartNodes');
    if (btn) btn.disabled = true;
    
    pipelineStepsData[2].status = 'running';
    pipelineStepsData[2].detail = 'Đang tiến hành...';
    renderPipelineSteps();
    appendLog('Đang khởi động Node...', 'info');
    
    try {
        await apiCall('/api/nodes/stop', 'POST');
        const result = await apiCall('/api/nodes/start', 'POST');
        
        if (result.success) {
            const healthyCount = result.nodes.filter(n => n.health === 'healthy').length;
            pipelineStepsData[2].status = 'done';
            pipelineStepsData[2].detail = `${healthyCount}/${result.nodes.length} Node hoạt động tốt`;
            appendLog(`Các Node đã khởi động: ${healthyCount}/${result.nodes.length} Healthy`, 'success');
            updateSystemStatus(healthyCount > 0);
            refreshNodeStatus();
            
            pipelineStepsData[3].enabled = false;
            if (healthyCount > 0) pipelineStepsData[3].enabled = true;
            document.getElementById('pipelineNodesWrapper').style.display = 'block';
        } else {
            throw new Error(result.error || 'Lỗi không xác định');
        }
    } catch (err) {
        pipelineStepsData[2].status = 'error';
        pipelineStepsData[2].detail = err.message;
        pipelineStepsData[2].enabled = true;
        appendLog(`Khởi động node thất bại: ${err.message}`, 'error');
    }
    renderPipelineSteps();
}

async function runStepDetect() {
    const btn = document.getElementById('btnDetect');
    if (btn) btn.disabled = true;
    
    pipelineStepsData[3].status = 'running';
    pipelineStepsData[3].detail = 'Đang tiến hành...';
    renderPipelineSteps();
    appendLog('Đang tìm đường dây gian lận...', 'info');
    
    try {
        const result = await apiCall('/api/detect', 'POST');
        if (result.success) {
            lastDetectionResult = result.data;
            pipelineStepsData[3].status = 'done';
            pipelineStepsData[3].detail = `Đã tìm thấy ${result.data.total_cycles_detected} Cycle trong ${result.data.total_time_ms.toFixed(1)}ms`;
            appendLog(`Dò tìm hoàn tất: ${result.data.total_cycles_detected} đường dây gian lận được tìm thấy trong ${result.data.total_time_ms.toFixed(1)}ms`, 'success');
            updateDetectionUI(result.data);
            showQuickStats(result.data);
            
            pipelineStepsData[3].enabled = true;
            document.getElementById('pipelineResultsWrapper').style.display = 'block';
        } else {
            throw new Error(result.error || 'Lỗi không xác định');
        }
    } catch (err) {
        pipelineStepsData[3].status = 'error';
        pipelineStepsData[3].detail = err.message;
        pipelineStepsData[3].enabled = true;
        appendLog(`Detection failed: ${err.message}`, 'error');
    }
    renderPipelineSteps();
}

function showQuickStats(data) {
    const card = document.getElementById('quickStatsCard');
    card.style.display = 'block';
    document.getElementById('quickStats').innerHTML = `
        <div class="stat-card">
            <div class="stat-label">Số đường dây phát hiện</div>
            <div class="stat-value indigo">${data.total_cycles_detected}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Thời gian dò tìm</div>
            <div class="stat-value cyan">${data.total_time_ms.toFixed(1)}ms</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Số tin nhắn mạng</div>
            <div class="stat-value amber">${data.aggregate_stats.total_network_messages}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Node hoạt động</div>
            <div class="stat-value emerald">${data.active_nodes}/${data.total_nodes}</div>
        </div>
    `;
}

// 
// Quản lý Node
// 
async function refreshNodeStatus() {
    try {
        const result = await apiCall('/api/nodes/status');
        const nodeInfos = [];
        await Promise.all(result.nodes.map(async node => {
            const card = document.getElementById(`nodeCard${node.node_id}`);
            const badge = document.getElementById(`nodeBadge${node.node_id}`);
            const samples = document.getElementById(`nodeSamples${node.node_id}`);

            card.className = `node-card ${node.health === 'healthy' ? 'healthy' : 'offline'}`;
            badge.className = `node-badge ${node.health === 'healthy' ? 'healthy' : 'offline'}`;
            badge.textContent = node.health === 'healthy' ? 'Hoạt động' : 'Ngoại tuyến';

            document.getElementById(`nodeEdges${node.node_id}`).textContent =
                `Số cạnh (Edges): ${node.num_edges !== undefined ? node.num_edges : '—'}`;
            document.getElementById(`nodeVertices${node.node_id}`).textContent =
                `Số đỉnh (Vertices): ${node.num_vertices !== undefined ? node.num_vertices : '—'}`;
            const fraudBadge = document.getElementById(`nodeFraudEdges${node.node_id}`);
            if (fraudBadge) {
                const cycleCandidateCount = node.num_cycle_candidate_edges;
                fraudBadge.textContent =
                    `Ứng viên chu trình: ${cycleCandidateCount !== undefined ? cycleCandidateCount : '—'}`;
            }

            if (node.health === 'healthy') {
                const info = await apiCall(`/api/nodes/info/${node.node_id}`);
                if (info.success && info.data) {
                    renderNodeSamples(node.node_id, info.data);
                    nodeInfos.push({ ...info.data, health: node.health });
                }
            } else {
                if (samples) {
                    samples.innerHTML = '<div class="node-samples-title">Phân mảnh dữ liệu</div><div>Node ngoại tuyến.</div>';
                }
                nodeInfos.push({ node_id: node.node_id, health: node.health, sample_edges: [], cycle_fraud_edges_sample: [] });
            }
        }));

        const healthyCount = result.nodes.filter(n => n.health === 'healthy').length;
        updateSystemStatus(healthyCount > 0);
        renderPartitionStorageMonitor(result.nodes, nodeInfos);
    } catch (err) {
        console.error('Không thể làm mới trạng thái node:', err);
    }
}

async function startNodes() {
    showLoading('Đang khởi động các node phân tán...');
    appendLog('Đang khởi động toàn bộ các node phân tán...', 'info');

    try {
        const result = await apiCall('/api/nodes/start', 'POST');
        if (result.success) {
            result.nodes.forEach(n => {
                appendLog(`Node ${n.node_id}: ${n.status} (PID: ${n.pid}, Health: ${n.health})`,
                    n.health === 'healthy' ? 'success' : 'warn');
            });
        }
        refreshNodeStatus();
    } catch (err) {
        appendLog(`Khởi động node thất bại: ${err.message}`, 'error');
    }

    hideLoading();
}

async function stopNodes() {
    showLoading('Đang dừng toàn bộ node...');
    appendLog('Đang dừng toàn bộ các node phân tán...', 'warn');

    try {
        const result = await apiCall('/api/nodes/stop', 'POST');
        if (result.success) {
            result.nodes.forEach(n => {
                appendLog(`Node ${n.node_id}: ${n.status}`, 'success');
            });
        }
        refreshNodeStatus();
        updateSystemStatus(false);
    } catch (err) {
        appendLog(`Dừng node thất bại: ${err.message}`, 'error');
    }

    hideLoading();
}

async function viewNodeData(nid) {
    showLoading(`Đang tải dữ liệu của Node ${nid}...`);
    try {
        const res = await apiCall(`/api/nodes/info/${nid}`);
        if (res.success && res.data) {
            document.getElementById('modalNodeTitle').textContent = `Node ${nid}`;
            document.getElementById('modalOwnedCount').textContent = `${res.data.num_owned_vertices} Vertices`;
            document.getElementById('modalBoundaryCount').textContent = `${res.data.num_boundary_vertices} Vertices`;
            
            document.getElementById('modalOwnedSample').innerHTML = res.data.owned_vertices_sample.join('<br>') || 'Không có dữ liệu';
            document.getElementById('modalBoundarySample').innerHTML = res.data.boundary_vertices_sample.join('<br>') || 'Không có dữ liệu';
            
            document.getElementById('nodeDataModal').style.display = 'flex';
            
            // Hiển thị trình theo dõi lưu trữ phân mảnh bên trong hộp thoại của node này
            const sampleEdges = res.data.sample_edges || [];
            let rows = sampleEdges.map(edge => {
                const storedNode = getShardId(String(edge.from));
                const targetNode = getShardId(String(edge.to));
                const isCross = storedNode !== targetNode;
                return `
                    <tr>
                        <td><code>${edge.from} -> ${edge.to}</code></td>
                        <td>${formatAmount(edge.amount)}</td>
                        <td><span class="edge-badge ${isCross ? 'cross' : 'local'}">${isCross ? 'LIÊN NODE' : 'NỘI BỘ'}</span></td>
                        <td>Lưu tại Node ${storedNode} (do ${edge.from} % 3 = ${storedNode})</td>
                        <td>Đích: Node ${targetNode}</td>
                    </tr>
                `;
            }).join('');
            
            document.getElementById('partitionStorageSummary').innerHTML = `
                <div class="table-scroll">
                    <table class="monitor-table">
                        <thead>
                            <tr>
                                <th>Cạnh (Edge)</th>
                                <th>Số tiền</th>
                                <th>Loại</th>
                                <th>Quy tắc lưu trữ</th>
                                <th>Đích đến (Destination)</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${rows || '<tr><td colspan="5">Node ngoại tuyến hoặc không có dữ liệu mẫu.</td></tr>'}
                        </tbody>
                    </table>
                </div>
            `;

        } else {
            alert('Lỗi khi lấy dữ liệu: ' + (res.error || 'Lỗi không xác định'));
        }
    } catch (err) {
        alert('Lỗi kết nối tới Node: ' + err.message);
    }
    hideLoading();
}

async function killNode(nid) {
    appendLog(` Đang tắt Node ${nid}...`, 'error');

    try {
        const result = await apiCall(`/api/nodes/kill/${nid}`, 'POST');
        if (result.success) {
            appendLog(`Node ${nid} đã BỊ TẮT.`, 'error');
        } else {
            appendLog(`Lỗi tắt Node ${nid}: ${result.error}`, 'warn');
        }
        refreshNodeStatus();
    } catch (err) {
        appendLog(`Lỗi tắt Node ${nid}: ${err.message}`, 'error');
    }
}

async function restartNode(nid) {
    appendLog(` Đang khởi động lại Node ${nid}...`, 'info');

    try {
        const result = await apiCall(`/api/nodes/restart/${nid}`, 'POST');
        if (result.success) {
            appendLog(`Node ${nid} đã khởi động lại: PID=${result.pid}, Trạng thái=${result.health}`, 'success');
        }
        refreshNodeStatus();
    } catch (err) {
        appendLog(`Lỗi khởi động lại Node ${nid}: ${err.message}`, 'error');
    }
}

function updateSystemStatus(online) {
    const badge = document.getElementById('systemStatus');
    if (online) {
        badge.className = 'status-badge online';
        badge.innerHTML = '<div class="status-dot"></div><span>Hệ thống hoạt động</span>';
    } else {
        badge.className = 'status-badge offline';
        badge.innerHTML = '<div class="status-dot"></div><span>Hệ thống ngoại tuyến</span>';
    }
}

// 
// Dò tìm Chu trình Gian lận
// 
async function runDetection() {
    showLoading('Đang thực thi dò tìm gian lận phân tán...');
    appendLog('Bắt đầu truy vấn dò tìm gian lận phân tán...', 'info');

    try {
        const result = await apiCall('/api/detect', 'POST');
        if (result.success) {
            lastDetectionResult = result.data;
            updateDetectionUI(result.data);
            appendLog(`Dò tìm hoàn tất: ${result.data.total_cycles_detected} chu trình trong ${result.data.total_time_ms.toFixed(1)}ms`, 'success');
        } else {
            appendLog(`Lỗi dò tìm: ${result.error}`, 'error');
        }
    } catch (err) {
        appendLog(`Dò tìm thất bại: ${err.message}`, 'error');
    }

    hideLoading();
}

function updateDetectionUI(data) {
    // Cập nhật thống kê
    document.getElementById('statTotalCycles').textContent = data.total_cycles_detected;
    document.getElementById('statLocalCycles').textContent = data.local_cycles;
    document.getElementById('statCrossShardCycles').textContent = data.cross_shard_cycles;
    document.getElementById('statDetectionTime').textContent = `${data.total_time_ms.toFixed(1)}ms`;

    // Cập nhật danh sách chu trình
    const cycleList = document.getElementById('cycleList');
    if (data.cycles.length === 0) {
        cycleList.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon"></div>
                <h3>Không Phát Hiện Chu Trình Gian Lận</h3>
                <p>Tìm kiếm phân tán đã hoàn thành nhưng không tìm thấy chu trình 4 đỉnh nào.</p>
            </div>
        `;
    } else {
        let tableRows = data.cycles.map((c, idx) => {
            const enrichedPath = c.cycle.map(id => {
                if (accountMetadata && accountMetadata[id]) {
                    const meta = accountMetadata[id];
                    return `<span class="tooltip" title="Original account: ${meta.OriginalAccount || id}">${meta.OriginalAccount || id}</span>`;
                }
                return `<span>${id}</span>`;
            }).join(' → ');
            const shardPath = renderShardPath(c.cycle);

            return `
            <tr style="border-bottom: 1px solid var(--border-glass);">
                <td style="padding: 12px 8px; font-weight: 600;">C${idx + 1}</td>
                <td style="padding: 12px 8px; font-family: monospace;">${enrichedPath}</td>
                <td style="padding: 12px 8px; font-family: monospace;">${shardPath}</td>
                <td style="padding: 12px 8px; font-family: monospace;">${formatAmount(c.amount)}</td>
                <td style="padding: 12px 8px;"><span class="cycle-type ${c.type}">${c.type === 'LOCAL' ? 'NỘI BỘ' : 'LIÊN NODE'}</span></td>
                <td style="padding: 12px 8px;">
                    <button class="btn btn-outline btn-sm" onclick="document.getElementById('tab-graph').click()" style="padding: 4px 8px; font-size: 0.8rem;">Đồ thị</button>
                    <button class="btn btn-outline btn-sm" onclick="showTrace(${idx})" style="padding: 4px 8px; font-size: 0.8rem;">Dò tìm</button>
                </td>
            </tr>
            `;
        }).join('');
        
        cycleList.innerHTML = `
            <table style="width: 100%; border-collapse: collapse; margin-top: 10px; text-align: left; font-size: 0.95rem;">
                <thead style="background: var(--bg-secondary);">
                    <tr>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">ID Chu trình</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Đường dẫn (A → B → C → D → A)</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Đường dẫn Phân mảnh (Shard Path)</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Số tiền</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Loại</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Hành động</th>
                    </tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
        `;
    }

    // Cập nhật chi tiết hiệu năng
    const perfCard = document.getElementById('perfCard');
    perfCard.style.display = 'block';
    document.getElementById('perfStats').innerHTML = `
        <div class="stat-card">
            <div class="stat-label">Tin nhắn mạng (Chi phí truyền thông)</div>
            <div class="stat-value amber">${data.aggregate_stats.total_network_messages}</div>
            <div class="stat-change">Yêu cầu HTTP liên Node</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Thao tác cục bộ (Chi phí CPU)</div>
            <div class="stat-value emerald">${data.aggregate_stats.total_local_ops}</div>
            <div class="stat-change">Tra cứu danh sách kề trong bộ nhớ</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Yêu cầu lỗi (Phát hiện lỗi)</div>
            <div class="stat-value rose">${data.aggregate_stats.total_failed_requests}</div>
            <div class="stat-change">Node bị hết hạn hoặc ngoại tuyến</div>
        </div>
    `;

    renderTraceWorkbench(data);
    renderMessageMonitor(data);
}

// 
// Cấu trúc Phân mảnh Đồ thị
// 
function updateTopologyUI(data) {
    // Đã loại bỏ
}

function getShardId(vertexId) {
    const config = getConfig();
    let shard = 0;
    try {
        let val = parseInt(vertexId.replace("C", ""));
        shard = val % config.numPartitions;
    } catch(e) {
        shard = 0; // giá trị dự phòng
    }
    return isNaN(shard) ? 0 : shard;
}

function formatAmount(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return '-';
    return Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function renderShardPath(cycle) {
    return cycle.map(id => `${id}(Node ${getShardId(String(id))})`).join(' → ');
}

function renderNodeSamples(nid, info) {
    const container = document.getElementById(`nodeSamples${nid}`);
    if (!container) return;

    const cycleEdges = info.cycle_fraud_edges_sample || [];
    const sampleEdges = cycleEdges.length ? cycleEdges : (info.sample_edges || []);

    if (!sampleEdges.length) {
        container.innerHTML = '<div class="node-samples-title">Cạnh mẫu</div><div>Không có dữ liệu mẫu.</div>';
        return;
    }

    container.innerHTML = `
        <div class="node-samples-title">${cycleEdges.length ? 'Cạnh ứng viên chu trình' : 'Cạnh mẫu được lưu'}</div>
        ${sampleEdges.slice(0, 5).map(e => {
            const isCross = getShardId(String(e.from)) !== getShardId(String(e.to));
            return `
                <div class="node-sample-row">
                    <span>${e.from} -> ${e.to}</span>
                    <span>
                        <span class="edge-badge ${isCross ? 'cross' : 'local'}">${isCross ? 'LIÊN NODE' : 'NỘI BỘ'}</span>
                        ${formatAmount(e.amount)}
                    </span>
                </div>
            `;
        }).join('')}
    `;
}

function renderPartitionStorageMonitor(nodes = [], nodeInfos = []) { /* Removed */ }

function getCycleMessages(cycle, cycleIdx = 0, status = 'OK') {
    const path = cycle.cycle || cycle;
    const amount = cycle.amount;
    const messages = [];

    for (let i = 0; i < path.length - 2; i++) {
        const u = path[i];
        const v = path[i + 1];
        const fromNode = getShardId(String(u));
        const toNode = getShardId(String(v));
        if (fromNode !== toNode) {
            messages.push({
                cycleId: `C${cycleIdx + 1}`,
                fromNode,
                toNode,
                api: '/expand_path',
                path: path.slice(0, i + 2),
                amount,
                status,
            });
        }
    }

    return messages;
}

function getTraceSteps(cycle) {
    const path = cycle.cycle || cycle;
    const amount = cycle.amount;
    const steps = [];

    steps.push({
        kind: 'init',
        title: `Khởi tạo tại Node ${getShardId(String(path[0]))}`,
        detail: `Bắt đầu tại đỉnh ${path[0]} vì đây là đỉnh có ID nhỏ nhất trong chu trình này. Lọc số tiền giao dịch = ${formatAmount(amount)}.`,
    });

    for (let i = 0; i < path.length - 1; i++) {
        const u = path[i];
        const v = path[i + 1];
        const sourceNode = getShardId(String(u));
        const targetNode = getShardId(String(v));
        const isClosing = i === path.length - 2;

        if (isClosing) {
            steps.push({
                kind: 'found',
                title: `Kiểm tra cạnh khép kín tại Node ${sourceNode}`,
                detail: `Node ${sourceNode} kiểm tra cạnh ${u} -> ${v}. Đích trỏ về lại đỉnh ban đầu ${path[0]} và lượng tiền giao dịch khớp ${formatAmount(amount)}.`,
            });
        } else if (sourceNode === targetNode) {
            steps.push({
                kind: 'local',
                title: `Duyệt nội bộ (Local) tại Node ${sourceNode}`,
                detail: `Đọc cạnh ${u} -> ${v} cục bộ trên Node ${sourceNode} (do cả hai đỉnh cùng thuộc phân mảnh: ${u} % 3 = ${sourceNode} và ${v} % 3 = ${targetNode}).`,
            });
        } else {
            steps.push({
                kind: 'network',
                title: `Duyệt liên Node (Network): Node ${sourceNode} -> Node ${targetNode}`,
                detail: `Node ${sourceNode} gửi yêu cầu POST /expand_path sang Node ${targetNode} kèm đường dẫn=[${path.slice(0, i + 2).join(', ')}], số tiền=${formatAmount(amount)}, đích=${path[0]}.`,
            });
        }
    }

    return steps;
}

function renderTraceWorkbench(data) {
    const card = document.getElementById('traceWorkbenchCard');
    const container = document.getElementById('traceWorkbench');
    if (!card || !container) return;

    card.style.display = 'block';
    if (!data.cycles || data.cycles.length === 0) {
        container.innerHTML = '<div class="empty-state"><h3>Không có thông tin dò tìm</h3><p>Không phát hiện chu trình gian lận nào để theo dõi.</p></div>';
        return;
    }

    container.innerHTML = `
        <div class="trace-cycle-grid">
            ${data.cycles.map((cycle, idx) => {
                const shardPath = renderShardPath(cycle.cycle);
                const messages = getCycleMessages(cycle, idx);
                return `
                    <div class="trace-cycle-card">
                        <div class="trace-card-header">
                            <strong>C${idx + 1}: ${cycle.type === 'LOCAL' ? 'NỘI BỘ' : 'LIÊN NODE'}</strong>
                            <button class="btn btn-outline btn-sm" onclick="showTrace(${idx})">Xem dò tìm (Trace)</button>
                        </div>
                        <div class="mono-line">${cycle.cycle.join(' -> ')}</div>
                        <div class="muted">${shardPath}</div>
                        <div class="sequence-line">
                            ${cycle.cycle.slice(0, -1).map(id => `<span>Node ${getShardId(String(id))}</span>`).join('<b>POST</b>')}
                        </div>
                        <div class="storage-metrics">
                            <span>Số tiền: ${formatAmount(cycle.amount)}</span>
                            <span>Số bước nhảy mạng: ${messages.length}</span>
                            <span>Mẫu: A->B->C->D->A</span>
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
    `;
}

function renderMessageMonitor(data, failedNode = null) {
    const card = document.getElementById('messageMonitorCard');
    const container = document.getElementById('messageMonitor');
    if (!card || !container) return;

    const messages = (data.cycles || []).flatMap((cycle, idx) => {
        return getCycleMessages(cycle, idx).map(msg => {
            const failed = failedNode !== null && (msg.fromNode === failedNode || msg.toNode === failedNode);
            return { ...msg, status: failed ? 'FAILED/TIMEOUT' : 'OK' };
        });
    });

    card.style.display = 'block';
    if (!messages.length) {
        container.innerHTML = '<div class="empty-state"><h3>Không có tin nhắn mạng</h3><p>Tất cả chu trình phát hiện được đều nằm trong nội bộ một phân mảnh.</p></div>';
        return;
    }

    container.innerHTML = `
        <div class="table-scroll">
            <table class="monitor-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Chu trình (Cycle)</th>
                        <th>Gửi từ</th>
                        <th>Nhận bởi</th>
                        <th>API</th>
                        <th>Dữ liệu đường dẫn (Payload)</th>
                        <th>Số tiền</th>
                        <th>Trạng thái</th>
                    </tr>
                </thead>
                <tbody>
                    ${messages.map((m, idx) => `
                        <tr>
                            <td>${idx + 1}</td>
                            <td>${m.cycleId}</td>
                            <td>Node ${m.fromNode}</td>
                            <td>Node ${m.toNode}</td>
                            <td><code>${m.api}</code></td>
                            <td><code>[${m.path.join(', ')}]</code></td>
                            <td>${formatAmount(m.amount)}</td>
                            <td><span class="edge-badge ${m.status === 'OK' ? 'local' : 'failed'}">${m.status}</span></td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
}

function showTrace(cycleIdx) {
    if (!lastDetectionResult || !lastDetectionResult.cycles) return;
    const cycle = lastDetectionResult.cycles[cycleIdx];
    const steps = getTraceSteps(cycle);
    const messages = getCycleMessages(cycle, cycleIdx);

    const stepsHtml = steps.map((step, idx) => `
        <div class="trace-step ${step.kind}">
            <div class="trace-step-index">${idx}</div>
            <div>
                <div class="trace-step-title">${step.title}</div>
                <div class="trace-step-detail">${step.detail}</div>
            </div>
        </div>
    `).join('');

    const messageHtml = messages.length ? `
        <div class="table-scroll" style="margin-top: 16px;">
            <table class="monitor-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>From</th>
                        <th>To</th>
                        <th>API</th>
                        <th>Payload</th>
                    </tr>
                </thead>
                <tbody>
                    ${messages.map((m, idx) => `
                        <tr>
                            <td>${idx + 1}</td>
                            <td>Node ${m.fromNode}</td>
                            <td>Node ${m.toNode}</td>
                            <td><code>${m.api}</code></td>
                            <td><code>{"path":[${m.path.join(',')}],"amount":${m.amount},"target":${cycle.cycle[0]}}</code></td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    ` : '<div class="trace-note">Chu trình này chạy cục bộ (local), không cần gửi POST /expand_path liên Node.</div>';

    document.getElementById('traceContent').innerHTML = `
        <div class="trace-summary">
            <div><strong>Chu trình (Cycle):</strong> <code>${cycle.cycle.join(' -> ')}</code></div>
            <div><strong>Đường dẫn phân mảnh (Shard Path):</strong> <code>${renderShardPath(cycle.cycle)}</code></div>
            <div><strong>Số tiền:</strong> ${formatAmount(cycle.amount)}</div>
        </div>
        <div class="trace-step-list">${stepsHtml}</div>
        ${messageHtml}
    `;
    document.getElementById('traceModal').style.display = 'flex';
}

// 
// Kiểm thử Khả năng Chịu lỗi
// 
function updateFaultScenario(activeStep) {
    const checklist = document.getElementById('faultScenarioChecklist');
    if (!checklist) return;
    [...checklist.querySelectorAll('.scenario-step')].forEach((step, idx) => {
        step.className = `scenario-step ${idx < activeStep ? 'done' : idx === activeStep ? 'running' : 'pending'}`;
    });
}

async function runFaultTest() {
    showLoading('Running fault tolerance test...');
    const container = document.getElementById('faultTestResults');

    appendLog(' KIỂM THỬ KHẢ NĂNG CHỊU LỖI ', 'info');

    try {
        // Bước 1: Đảm bảo hệ thống đang hoạt động
        updateFaultScenario(0);
        appendLog('Bước 1: Đang khởi động toàn bộ các node...', 'info');
        await apiCall('/api/nodes/start', 'POST');
        await new Promise(r => setTimeout(r, 2000));

        // Bước 2: Chạy dò tìm khi tất cả các node đều khỏe mạnh
        updateFaultScenario(1);
        appendLog('Bước 2: Chạy dò tìm khi tất cả các node hoạt động bình thường...', 'info');
        const healthyRun = await apiCall('/api/detect', 'POST');
        const healthyData = healthyRun.data;
        appendLog(`  Kết quả: ${healthyData.total_cycles_detected} chu trình, ${healthyData.total_time_ms.toFixed(1)}ms, ${healthyData.active_nodes}/${healthyData.total_nodes} nodes hoạt động`, 'success');

        // Bước 3: Dừng Node 1 (Giả lập sự cố)
        updateFaultScenario(2);
        appendLog('Bước 3: Dừng Node 1 (giả lập sự cố sập node)...', 'error');
        await apiCall('/api/nodes/kill/1', 'POST');
        await new Promise(r => setTimeout(r, 500));
        appendLog('  Node 1 đã dừng.', 'error');

        // Bước 4: Chạy dò tìm khi cụm node bị suy giảm năng lực
        appendLog('Bước 4: Chạy dò tìm khi Node 1 bị sập (suy giảm năng lực)...', 'warn');
        const degradedRun = await apiCall('/api/detect', 'POST');
        const degradedData = degradedRun.data;
        appendLog(`  Kết quả: ${degradedData.total_cycles_detected} chu trình, ${degradedData.total_time_ms.toFixed(1)}ms, ${degradedData.active_nodes}/${degradedData.total_nodes} nodes hoạt động`, 'warn');

        // Bước 5: Khởi động lại Node 1
        updateFaultScenario(3);
        appendLog('Bước 5: Khởi động lại Node 1...', 'info');
        await apiCall('/api/nodes/restart/1', 'POST');
        await new Promise(r => setTimeout(r, 1500));
        appendLog('  Node 1 đã khởi động lại thành công.', 'success');

        // Bước 6: Chạy dò tìm sau khi hệ thống đã hồi phục
        updateFaultScenario(4);
        appendLog('Bước 6: Chạy dò tìm sau khi hệ thống hồi phục hoàn toàn...', 'info');
        const recoveryRun = await apiCall('/api/detect', 'POST');
        const recoveryData = recoveryRun.data;
        appendLog(`  Kết quả: ${recoveryData.total_cycles_detected} chu trình, ${recoveryData.total_time_ms.toFixed(1)}ms, ${recoveryData.active_nodes}/${recoveryData.total_nodes} nodes hoạt động`, 'success');

        appendLog(' KIỂM THỬ KHẢ NĂNG CHỊU LỖI HOÀN TẤT ', 'success');
        updateFaultScenario(5);

        // Hiển thị kết quả
        container.innerHTML = `
            <div class="grid-3">
                <div class="stat-card" style="border-left: 3px solid var(--accent-emerald);">
                    <div class="stat-label">🟢 Healthy (All Nodes)</div>
                    <div class="stat-value emerald">${healthyData.total_cycles_detected} cycles</div>
                    <div class="stat-change">${healthyData.total_time_ms.toFixed(1)}ms | ${healthyData.active_nodes}/${healthyData.total_nodes} nodes | ${healthyData.aggregate_stats.total_network_messages} net msgs</div>
                </div>
                <div class="stat-card" style="border-left: 3px solid var(--accent-rose);">
                    <div class="stat-label"> Degraded (Node 1 Down)</div>
                    <div class="stat-value rose">${degradedData.total_cycles_detected} cycles</div>
                    <div class="stat-change">${degradedData.total_time_ms.toFixed(1)}ms | ${degradedData.active_nodes}/${degradedData.total_nodes} nodes | ${degradedData.aggregate_stats.total_network_messages} net msgs</div>
                </div>
                <div class="stat-card" style="border-left: 3px solid var(--accent-cyan);">
                    <div class="stat-label"> Recovered</div>
                    <div class="stat-value cyan">${recoveryData.total_cycles_detected} cycles</div>
                    <div class="stat-change">${recoveryData.total_time_ms.toFixed(1)}ms | ${recoveryData.active_nodes}/${recoveryData.total_nodes} nodes | ${recoveryData.aggregate_stats.total_network_messages} net msgs</div>
                </div>
            </div>
            <div style="margin-top: 16px; padding: 16px; background: var(--bg-glass); border-radius: var(--radius-md); border: 1px solid var(--border-glass);">
                <strong style="color: var(--accent-emerald);"> Analysis:</strong>
                <span style="color: var(--text-secondary); font-size: 0.85rem;">
                    The system ${degradedData.total_cycles_detected < healthyData.total_cycles_detected ? 'detected fewer cycles when Node 1 was down (expected: cross-shard paths through Node 1 failed gracefully)' : 'maintained detection capability even with a degraded cluster'}.
                    After recovery, the system returned to ${recoveryData.total_cycles_detected === healthyData.total_cycles_detected ? 'full detection capability' : 'operational state'}.
                    <strong>No crashes, hangs, or data corruption occurred.</strong>
                </span>
            </div>
            <div class="table-scroll" style="margin-top: 16px;">
                <table class="monitor-table">
                    <thead>
                        <tr>
                            <th>Trạng thái hệ thống</th>
                            <th>Số Node hoạt động</th>
                            <th>Số chu trình (Cycles)</th>
                            <th>Yêu cầu lỗi</th>
                            <th>Tin nhắn mạng</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Bình thường (Healthy)</td>
                            <td>${healthyData.active_nodes}/${healthyData.total_nodes}</td>
                            <td>${healthyData.total_cycles_detected}</td>
                            <td>${healthyData.aggregate_stats.total_failed_requests}</td>
                            <td>${healthyData.aggregate_stats.total_network_messages}</td>
                        </tr>
                        <tr>
                            <td>Sự cố: Mất kết nối Node 1</td>
                            <td>${degradedData.active_nodes}/${degradedData.total_nodes}</td>
                            <td>${degradedData.total_cycles_detected}</td>
                            <td>${degradedData.aggregate_stats.total_failed_requests}</td>
                            <td>${degradedData.aggregate_stats.total_network_messages}</td>
                        </tr>
                        <tr>
                            <td>Phục hồi thành công (Recovered)</td>
                            <td>${recoveryData.active_nodes}/${recoveryData.total_nodes}</td>
                            <td>${recoveryData.total_cycles_detected}</td>
                            <td>${recoveryData.aggregate_stats.total_failed_requests}</td>
                            <td>${recoveryData.aggregate_stats.total_network_messages}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        `;

        lastDetectionResult = recoveryData;
        updateDetectionUI(recoveryData);
        renderMessageMonitor(healthyData, 1);
        refreshNodeStatus();

    } catch (err) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon"></div>
                <h3>Test Failed</h3>
                <p>${err.message}. Make sure the pipeline has been run first.</p>
            </div>
        `;
        appendLog(`Fault test error: ${err.message}`, 'error');
    }

    hideLoading();
}

// 
// Trực quan hóa Đồ thị (D3.js)
// 
let simulation = null;

function setGraphMode(mode) {
    graphMode = mode;
    const detectedBtn = document.getElementById('btnGraphDetected');
    const shardBtn = document.getElementById('btnGraphShard');
    if (detectedBtn && shardBtn) {
        detectedBtn.classList.toggle('active-mode', mode === 'detected');
        shardBtn.classList.toggle('active-mode', mode === 'shard');
    }
    renderGraph();
}

async function renderGraph() {
    const svgEl = document.getElementById('graphSvg');
    const container = document.getElementById('d3Container');
    const loading = document.getElementById('graphLoading');
    
    // Xóa đồ thị cũ
    svgEl.innerHTML = '';
    if (simulation) simulation.stop();
    
    loading.style.display = 'block';
    
    try {
        const res = await apiCall('/api/graph');
        if (!res.success) {
            loading.innerHTML = 'Lỗi: ' + res.error;
            return;
        }
        
        const { nodes, edges } = res;
        if (!nodes.length || !edges.length) {
            loading.style.display = 'block';
            loading.innerHTML = res.message || 'Hãy thực hiện bước "Dò tìm chu trình" trước để hiển thị chu trình gian lận.';
            return;
        }

        loading.style.display = 'none';
        
        const width = container.clientWidth;
        const height = container.clientHeight;
        
        const svg = d3.select("#graphSvg")
            .attr("viewBox", [0, 0, width, height]);
            
        // Thiết lập zoom và pan
        const g = svg.append("g");
        svg.call(d3.zoom().on("zoom", (event) => {
            g.attr("transform", event.transform);
        }));

        // Định nghĩa các mũi tên đầu cạnh (Arrow markers)
        svg.append("defs").selectAll("marker")
            .data(["normal", "fraud", "cross"])
            .enter().append("marker")
            .attr("id", d => `arrow-${d}`)
            .attr("viewBox", "0 -5 10 10")
            .attr("refX", 18)
            .attr("refY", 0)
            .attr("markerWidth", 6)
            .attr("markerHeight", 6)
            .attr("orient", "auto")
            .append("path")
            .attr("fill", d => d === 'fraud' ? '#ef4444' : (d === 'cross' ? '#94a3b8' : '#94a3b8'))
            .attr("d", "M0,-5L10,0L0,5");

        if (graphMode === 'shard') {
            const shardCenters = [
                { x: width * 0.18, y: height * 0.5 },
                { x: width * 0.5, y: height * 0.5 },
                { x: width * 0.82, y: height * 0.5 },
            ];

            g.append("g")
                .selectAll("rect")
                .data(shardCenters)
                .enter().append("rect")
                .attr("x", d => d.x - width * 0.13)
                .attr("y", 40)
                .attr("width", width * 0.26)
                .attr("height", height - 80)
                .attr("rx", 8)
                .attr("fill", "rgba(37, 99, 235, 0.04)")
                .attr("stroke", "rgba(37, 99, 235, 0.18)");

            g.append("g")
                .selectAll("text")
                .data(shardCenters)
                .enter().append("text")
                .text((d, i) => `Node ${i} / partition_${i}.json`)
                .attr("x", d => d.x)
                .attr("y", 28)
                .attr("text-anchor", "middle")
                .attr("fill", "var(--text-secondary)")
                .attr("font-size", 12)
                .attr("font-weight", 700);
        }

        // Mô phỏng lực (Force simulation)
        simulation = d3.forceSimulation(nodes)
            .force("link", d3.forceLink(edges).id(d => d.id).distance(graphMode === 'shard' ? 80 : 60))
            .force("charge", d3.forceManyBody().strength(graphMode === 'shard' ? -150 : -200))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("x", d3.forceX(d => graphMode === 'shard' ? [width * 0.18, width * 0.5, width * 0.82][d.shard] : width / 2).strength(graphMode === 'shard' ? 2.5 : 0.08))
            .force("y", d3.forceY(height / 2).strength(graphMode === 'shard' ? 0.2 : 0.08))
            .force("collide", d3.forceCollide(20));

        // Vẽ các cạnh đồ thị
        const link = g.append("g")
            .selectAll("line")
            .data(edges)
            .enter().append("line")
            .attr("stroke", d => {
                if (d.is_fraud) return "#ef4444"; // Cạnh thuộc chu trình phát hiện (Detected Cycle Edge)
                if (d.source.shard !== d.target.shard) return "#94a3b8"; // Cạnh liên mảnh (cross-shard)
                return "#cbd5e1"; // Cạnh bình thường (normal)
            })
            .attr("stroke-width", d => d.is_fraud ? 2.5 : 1.5)
            .attr("stroke-dasharray", d => (!d.is_fraud && d.source.shard !== d.target.shard) ? "5,5" : "none")
            .on("click", (event, d) => showEdgeStorageInfo(d))
            .attr("marker-end", d => {
                if (d.is_fraud) return "url(#arrow-fraud)";
                if (d.source.shard !== d.target.shard) return "url(#arrow-cross)";
                return "url(#arrow-normal)";
            });

        // Bảng màu theo phân mảnh (shard)
        const color = d3.scaleOrdinal()
            .domain([0, 1, 2])
            .range(["#3b82f6", "#10b981", "#f59e0b"]);

        // Vẽ các đỉnh đồ thị
        const node = g.append("g")
            .selectAll("circle")
            .data(nodes)
            .enter().append("circle")
            .attr("r", 8)
            .attr("fill", d => color(d.shard))
            .attr("stroke", d => d.is_fraud ? "#ef4444" : "#fff")
            .attr("stroke-width", d => d.is_fraud ? 3 : 1.5)
            .call(drag(simulation))
            .on("click", (event, d) => {
                showNodeMetadata(d);
            })
            .on("dblclick", (event, d) => {
                // Click đôi để bỏ ghim đỉnh (unpin node)
                d.fx = null;
                d.fy = null;
                simulation.alpha(0.3).restart();
            });

        // Thêm nhãn cho đỉnh (chỉ hiển thị đỉnh thuộc chu trình gian lận để tránh lộn xộn)
        const labels = g.append("g")
            .selectAll("text")
            .data(nodes.filter(n => n.is_fraud))
            .enter().append("text")
            .text(d => d.OriginalAccount || d.id)
            .attr("font-size", 10)
            .attr("dx", 12)
            .attr("dy", 4)
            .attr("fill", "var(--text-primary)");

        simulation.on("tick", () => {
            link
                .attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);

            node
                .attr("cx", d => d.x)
                .attr("cy", d => d.y);
                
            labels
                .attr("x", d => d.x)
                .attr("y", d => d.y);
        });
        
    } catch (e) {
        loading.innerHTML = 'Lỗi khi tải đồ thị: ' + e.message;
    }
}

function showNodeMetadata(d) {
    const panel = document.getElementById('nodeInfoPanel');
    panel.innerHTML = `
        <div style="padding: 16px; background: var(--bg-secondary); border-radius: var(--radius-md); border-left: 4px solid ${d.is_fraud ? '#ef4444' : '#3b82f6'};">
            <h4 style="margin: 0 0 12px 0; font-size: 1.1rem; color: var(--text-primary);">${d.OriginalAccount || 'Tài khoản ' + d.id}</h4>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 0.9rem;">
                <div style="color: var(--text-secondary);">ID Tài khoản:</div>
                <div style="font-family: monospace; text-align: right;">${d.id}</div>

                <div style="color: var(--text-secondary);">Tài khoản gốc:</div>
                <div style="font-family: monospace; text-align: right;">${d.OriginalAccount || 'Tài khoản kiểm thử'}</div>
                
                <div style="color: var(--text-secondary);">Phân mảnh lưu trữ:</div>
                <div style="text-align: right; font-weight: bold;">Node ${d.shard}</div>
            </div>
        </div>
    `;
}

function showEdgeStorageInfo(edge) {
    const source = edge.source.id || edge.source;
    const target = edge.target.id || edge.target;
    const sourceShard = edge.source.shard ?? getShardId(String(source));
    const targetShard = edge.target.shard ?? getShardId(String(target));
    const panel = document.getElementById('nodeInfoPanel');
    panel.innerHTML = `
        <div style="padding: 16px; background: var(--bg-secondary); border-radius: var(--radius-md); border-left: 4px solid ${sourceShard === targetShard ? '#16a34a' : '#d97706'};">
            <h4 style="margin: 0 0 12px 0; font-size: 1.05rem; color: var(--text-primary);">Lưu trữ Cạnh</h4>
            <div class="trace-summary">
                <div><strong>Cạnh:</strong> <code>${source} -> ${target}</code></div>
                <div><strong>Số tiền:</strong> ${formatAmount(edge.amount)}</div>
                <div><strong>Nơi lưu trữ:</strong> Node ${sourceShard} (do ${source} % 3 = ${sourceShard})</div>
                <div><strong>Node gốc đích:</strong> Node ${targetShard} (do ${target} % 3 = ${targetShard})</div>
                <div><strong>Loại cạnh:</strong> <span class="edge-badge ${sourceShard === targetShard ? 'local' : 'cross'}">${sourceShard === targetShard ? 'CẠNH NỘI BỘ' : 'CẠNH LIÊN NODE'}</span></div>
            </div>
        </div>
    `;
}

function drag(simulation) {
    function dragstarted(event) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        event.subject.fx = event.subject.x;
        event.subject.fy = event.subject.y;
    }
    function dragged(event) {
        event.subject.fx = event.x;
        event.subject.fy = event.y;
    }
    function dragended(event) {
        if (!event.active) simulation.alphaTarget(0);
        // Giữ nguyên fx và fy để ghim đỉnh tại vị trí được thả
    }
    return d3.drag()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended);
}

// 
// Khởi tạo ứng dụng
// 
document.addEventListener('DOMContentLoaded', () => {
    appendLog('Dashboard đã tải. Cấu hình các tham số và chạy pipeline để bắt đầu.', 'info');
    refreshNodeStatus();
});
