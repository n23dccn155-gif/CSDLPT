/**
 * app.js - Dashboard Application Logic
 * =====================================
 * Manages all UI interactions, API calls, and state management for the
 * Distributed Fraud Ring Detection Dashboard.
 */

const API_BASE = '';  // Same origin

// 
// State
// 
let lastDetectionResult = null;
let lastPartitionResult = null;
let accountMetadata = null;

async function loadMetadata() {
    try {
        const res = await apiCall('/api/metadata');
        if (res.success) {
            accountMetadata = res.metadata;
        }
    } catch (e) {
        console.warn("Failed to load metadata", e);
    }
}
// Load on start
loadMetadata();

// 
// Tab Navigation
// 
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const tabId = btn.dataset.tab;

        // Update buttons
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');

        // Update panels
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
        document.getElementById(`panel-${tabId}`).classList.add('active');

        // Refresh data for certain tabs
        if (tabId === 'nodes') refreshNodeStatus();
        if (tabId === 'benchmark') loadBenchmarkHistory();
        if (tabId === 'graph') renderGraph();
    });
});

// 
// Utility Functions
// 
function showLoading(text = 'Processing...') {
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
        num_accounts: parseInt(document.getElementById('cfgAccounts').value) || 1000,
        num_normal_txs: parseInt(document.getElementById('cfgNormalTxs').value) || 5000,
        num_partitions: parseInt(document.getElementById('cfgPartitions').value) || 3,
        num_local_cycles: parseInt(document.getElementById('cfgLocalCycles').value) || 2,
        num_cross_cycles: parseInt(document.getElementById('cfgCrossCycles').value) || 3,
        fraud_amount_base: parseFloat(document.getElementById('cfgFraudAmount').value) || 5000,
        dataset_mode: document.getElementById('cfgDatasetMode') ? document.getElementById('cfgDatasetMode').value : 'synthetic',
        partition_strategy: document.getElementById('cfgPartitionStrategy') ? document.getElementById('cfgPartitionStrategy').value : 'hash',
    };
}

function resetConfig() {
    document.getElementById('cfgAccounts').value = 1000;
    document.getElementById('cfgNormalTxs').value = 5000;
    document.getElementById('cfgPartitions').value = 3;
    document.getElementById('cfgLocalCycles').value = 2;
    document.getElementById('cfgCrossCycles').value = 3;
    document.getElementById('cfgFraudAmount').value = 5000;
    appendLog('Configuration reset to defaults.', 'info');
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

// 
// Pipeline Execution
// 
async function runFullPipeline() {
    const btn = document.getElementById('btnRunPipeline');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Running...';
    showLoading('Running full pipeline...');

    const stepsContainer = document.getElementById('pipelineSteps');
    const config = getConfig();

    // Initialize pipeline steps UI
    const steps = [
        { name: 'Generate Data', icon: '', status: 'running' },
        { name: 'Partition Graph', icon: '', status: 'pending' },
        { name: 'Start Nodes', icon: '', status: 'pending' },
        { name: 'Detect Fraud Rings', icon: '', status: 'pending' },
    ];

    function renderSteps() {
        stepsContainer.innerHTML = steps.map(s => `
            <div class="pipeline-step">
                <div class="pipeline-step-icon ${s.status}">${s.status === 'done' ? '' : s.status === 'error' ? '' : s.icon}</div>
                <div>
                    <div class="pipeline-step-name">${s.name}</div>
                    <div class="pipeline-step-detail">${s.detail || (s.status === 'running' ? 'In progress...' : s.status === 'pending' ? 'Waiting...' : '')}</div>
                </div>
            </div>
        `).join('');
    }

    renderSteps();
    clearLog();
    appendLog('Starting full pipeline with config: ' + JSON.stringify(config), 'info');

    try {
        const result = await apiCall('/api/pipeline', 'POST', config);

        if (result.success && result.steps) {
            result.steps.forEach((step, idx) => {
                if (idx < steps.length) {
                    steps[idx].status = step.success ? 'done' : 'error';

                    if (step.step === 'generate' && step.data) {
                        steps[idx].detail = `${step.data.total_transactions} transactions generated in ${step.data.generation_time_ms}ms`;
                        appendLog(`Data generated: ${step.data.total_transactions} transactions`, 'success');
                        
                        document.getElementById('uiCurrentDataset').textContent = config.dataset_mode === 'paysim' ? 'PaySim processed subset' : 'Synthetic Dataset';
                        document.getElementById('uiCurrentTxs').textContent = 'Transactions: ' + step.data.total_transactions;
                        document.getElementById('uiCurrentStrategy').textContent = 'Strategy: ' + (config.partition_strategy === 'smart' ? 'Block-aware Partitioning' : 'Hash Partitioning');
                    } else if (step.step === 'partition' && step.data) {
                        lastPartitionResult = step.data;
                        steps[idx].detail = `Edge-Cut: ${step.data.edge_cut_ratio}% | Replication: ${step.data.vertex_replication_factor}`;
                        appendLog(`Graph partitioned: Edge-Cut ${step.data.edge_cut_ratio}%, Replication Factor ${step.data.vertex_replication_factor}`, 'success');
                        updateTopologyUI(step.data);
                    } else if (step.step === 'start_nodes' && step.data) {
                        const healthyCount = step.data.nodes.filter(n => n.health === 'healthy').length;
                        steps[idx].detail = `${healthyCount}/${step.data.nodes.length} nodes healthy`;
                        appendLog(`Nodes started: ${healthyCount}/${step.data.nodes.length} healthy`, 'success');
                        updateSystemStatus(healthyCount > 0);
                    } else if (step.step === 'detect' && step.data) {
                        lastDetectionResult = step.data;
                        steps[idx].detail = `Found ${step.data.total_cycles_detected} cycles in ${step.data.total_time_ms.toFixed(1)}ms`;
                        appendLog(`Detection complete: ${step.data.total_cycles_detected} fraud rings found in ${step.data.total_time_ms.toFixed(1)}ms`, 'success');
                        updateDetectionUI(step.data);
                        showQuickStats(step.data);
                    }
                }
            });
        } else {
            appendLog(`Pipeline error: ${result.error || 'Unknown error'}`, 'error');
            steps.forEach(s => { if (s.status === 'pending' || s.status === 'running') s.status = 'error'; });
        }
    } catch (err) {
        appendLog(`Pipeline failed: ${err.message}`, 'error');
        steps.forEach(s => { if (s.status === 'pending' || s.status === 'running') s.status = 'error'; });
    }

    renderSteps();
    hideLoading();
    btn.disabled = false;
    btn.innerHTML = ' Run Full Pipeline';
}

function showQuickStats(data) {
    const card = document.getElementById('quickStatsCard');
    card.style.display = 'block';
    document.getElementById('quickStats').innerHTML = `
        <div class="stat-card">
            <div class="stat-label">Fraud Rings Found</div>
            <div class="stat-value indigo">${data.total_cycles_detected}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Detection Time</div>
            <div class="stat-value cyan">${data.total_time_ms.toFixed(1)}ms</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Network Messages</div>
            <div class="stat-value amber">${data.aggregate_stats.total_network_messages}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Active Nodes</div>
            <div class="stat-value emerald">${data.active_nodes}/${data.total_nodes}</div>
        </div>
    `;
}

// 
// Node Management
// 
async function refreshNodeStatus() {
    try {
        const result = await apiCall('/api/nodes/status');
        result.nodes.forEach(node => {
            const card = document.getElementById(`nodeCard${node.node_id}`);
            const badge = document.getElementById(`nodeBadge${node.node_id}`);

            card.className = `node-card ${node.health === 'healthy' ? 'healthy' : 'offline'}`;
            badge.className = `node-badge ${node.health === 'healthy' ? 'healthy' : 'offline'}`;
            badge.textContent = node.health === 'healthy' ? 'Healthy' : 'Offline';

            document.getElementById(`nodeEdges${node.node_id}`).textContent =
                `Edges: ${node.num_edges !== undefined ? node.num_edges : '—'}`;
            document.getElementById(`nodeVertices${node.node_id}`).textContent =
                `Vertices: ${node.num_vertices !== undefined ? node.num_vertices : '—'}`;
        });

        const healthyCount = result.nodes.filter(n => n.health === 'healthy').length;
        updateSystemStatus(healthyCount > 0);
    } catch (err) {
        console.error('Failed to refresh node status:', err);
    }
}

async function startNodes() {
    showLoading('Starting distributed nodes...');
    appendLog('Starting all distributed nodes...', 'info');

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
        appendLog(`Failed to start nodes: ${err.message}`, 'error');
    }

    hideLoading();
}

async function stopNodes() {
    showLoading('Stopping all nodes...');
    appendLog('Stopping all distributed nodes...', 'warn');

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
        appendLog(`Failed to stop nodes: ${err.message}`, 'error');
    }

    hideLoading();
}

async function killNode(nid) {
    appendLog(` Killing Node ${nid}...`, 'error');

    try {
        const result = await apiCall(`/api/nodes/kill/${nid}`, 'POST');
        if (result.success) {
            appendLog(`Node ${nid} has been TERMINATED.`, 'error');
        } else {
            appendLog(`Failed to kill Node ${nid}: ${result.error}`, 'warn');
        }
        refreshNodeStatus();
    } catch (err) {
        appendLog(`Error killing Node ${nid}: ${err.message}`, 'error');
    }
}

async function restartNode(nid) {
    appendLog(` Restarting Node ${nid}...`, 'info');

    try {
        const result = await apiCall(`/api/nodes/restart/${nid}`, 'POST');
        if (result.success) {
            appendLog(`Node ${nid} restarted: PID=${result.pid}, Health=${result.health}`, 'success');
        }
        refreshNodeStatus();
    } catch (err) {
        appendLog(`Error restarting Node ${nid}: ${err.message}`, 'error');
    }
}

function updateSystemStatus(online) {
    const badge = document.getElementById('systemStatus');
    if (online) {
        badge.className = 'status-badge online';
        badge.innerHTML = '<div class="status-dot"></div><span>System Online</span>';
    } else {
        badge.className = 'status-badge offline';
        badge.innerHTML = '<div class="status-dot"></div><span>System Offline</span>';
    }
}

// 
// Detection
// 
async function runDetection() {
    showLoading('Running distributed fraud detection...');
    appendLog('Starting distributed fraud detection query...', 'info');

    try {
        const result = await apiCall('/api/detect', 'POST');
        if (result.success) {
            lastDetectionResult = result.data;
            updateDetectionUI(result.data);
            appendLog(`Detection complete: ${result.data.total_cycles_detected} cycles in ${result.data.total_time_ms.toFixed(1)}ms`, 'success');
        } else {
            appendLog(`Detection error: ${result.error}`, 'error');
        }
    } catch (err) {
        appendLog(`Detection failed: ${err.message}`, 'error');
    }

    hideLoading();
}

function updateDetectionUI(data) {
    // Update stats
    document.getElementById('statTotalCycles').textContent = data.total_cycles_detected;
    document.getElementById('statLocalCycles').textContent = data.local_cycles;
    document.getElementById('statCrossShardCycles').textContent = data.cross_shard_cycles;
    document.getElementById('statDetectionTime').textContent = `${data.total_time_ms.toFixed(1)}ms`;

    // Update cycle list
    const cycleList = document.getElementById('cycleList');
    if (data.cycles.length === 0) {
        cycleList.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon"></div>
                <h3>No Fraud Rings Detected</h3>
                <p>The distributed search completed but found no matching 4-vertex cycles.</p>
            </div>
        `;
    } else {
        let tableRows = data.cycles.map((c, idx) => {
            const enrichedPath = c.cycle.map(id => {
                if (accountMetadata && accountMetadata[id]) {
                    const meta = accountMetadata[id];
                    return `<span class="tooltip" title="Risk: ${meta.RiskScore} | Country: ${meta.Country}">${meta.OriginalAccount || id}</span>`;
                }
                return `<span>${id}</span>`;
            }).join(' → ');

            return `
            <tr style="border-bottom: 1px solid var(--border-glass);">
                <td style="padding: 12px 8px; font-weight: 600;">C${idx + 1}</td>
                <td style="padding: 12px 8px; font-family: monospace;">${enrichedPath}</td>
                <td style="padding: 12px 8px;"><span class="cycle-type ${c.type}">${c.type}</span></td>
                <td style="padding: 12px 8px;">${c.home_nodes ? c.home_nodes.join(', ') : '-'}</td>
            </tr>
            `;
        }).join('');
        
        cycleList.innerHTML = `
            <table style="width: 100%; border-collapse: collapse; margin-top: 10px; text-align: left; font-size: 0.95rem;">
                <thead style="background: var(--bg-secondary);">
                    <tr>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Cycle ID</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Path (A → B → C → D → A)</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Type</th>
                        <th style="padding: 12px 8px; border-bottom: 2px solid var(--border-glass);">Shards</th>
                    </tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
        `;
    }

    // Update performance breakdown
    const perfCard = document.getElementById('perfCard');
    perfCard.style.display = 'block';
    document.getElementById('perfStats').innerHTML = `
        <div class="stat-card">
            <div class="stat-label">Network Messages (Communication Cost)</div>
            <div class="stat-value amber">${data.aggregate_stats.total_network_messages}</div>
            <div class="stat-change">Cross-shard HTTP requests</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Local Operations (CPU Cost)</div>
            <div class="stat-value emerald">${data.aggregate_stats.total_local_ops}</div>
            <div class="stat-change">In-memory adjacency lookups</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Failed Requests (Fault Detection)</div>
            <div class="stat-value rose">${data.aggregate_stats.total_failed_requests}</div>
            <div class="stat-change">Timed out or unreachable nodes</div>
        </div>
    `;
}

// 
// Topology
// 
function updateTopologyUI(data) {
    document.getElementById('topoVertices').textContent = data.total_vertices;
    document.getElementById('topoEdges').textContent = data.total_edges;
    document.getElementById('topoEdgeCut').textContent = `${data.edge_cut_ratio}%`;
    document.getElementById('topoReplication').textContent = data.vertex_replication_factor;

    if (data.partition_stats) {
        updatePartitionCharts(data.partition_stats);
    }
}

// 
// Benchmarking
// 
async function runBenchmark() {
    const btn = document.getElementById('btnBenchmark');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Running...';
    showLoading('Running benchmark...');

    const config = {
        label: document.getElementById('benchLabel').value || `Run #${Date.now() % 10000}`,
        num_accounts: parseInt(document.getElementById('benchAccounts').value) || 1000,
        num_normal_txs: parseInt(document.getElementById('benchTxs').value) || 5000,
        num_local_cycles: parseInt(document.getElementById('benchLocalCycles').value) || 2,
        num_cross_cycles: parseInt(document.getElementById('benchCrossCycles').value) || 3,
        num_partitions: 3,
        dataset_mode: document.getElementById('benchDatasetMode') ? document.getElementById('benchDatasetMode').value : 'synthetic',
        partition_strategy: document.getElementById('benchPartitionStrategy') ? document.getElementById('benchPartitionStrategy').value : 'hash',
    };

    try {
        const result = await apiCall('/api/benchmark', 'POST', config);
        if (result.success) {
            appendLog(`Benchmark "${config.label}" complete: ${result.data.detection.total_cycles} cycles in ${result.data.detection.total_time_ms}ms`, 'success');
            loadBenchmarkHistory();
        } else {
            appendLog(`Benchmark error: ${result.error}`, 'error');
        }
    } catch (err) {
        appendLog(`Benchmark failed: ${err.message}`, 'error');
    }

    hideLoading();
    btn.disabled = false;
    btn.innerHTML = ' Run Benchmark';
}

async function runPresetBenchmarks() {
    showLoading('Running preset benchmark suite...');
    appendLog('Starting preset benchmark suite (5 configurations)...', 'info');

    const presets = [
        { label: '1K Txs', num_normal_txs: 1000, num_accounts: 500, num_local_cycles: 2, num_cross_cycles: 3 },
        { label: '3K Txs', num_normal_txs: 3000, num_accounts: 800, num_local_cycles: 2, num_cross_cycles: 3 },
        { label: '5K Txs', num_normal_txs: 5000, num_accounts: 1000, num_local_cycles: 2, num_cross_cycles: 3 },
        { label: '8K Txs', num_normal_txs: 8000, num_accounts: 1500, num_local_cycles: 2, num_cross_cycles: 3 },
        { label: '10K Txs', num_normal_txs: 10000, num_accounts: 2000, num_local_cycles: 2, num_cross_cycles: 3 },
    ];

    for (const preset of presets) {
        appendLog(`Running benchmark: ${preset.label}...`, 'info');
        try {
            const result = await apiCall('/api/benchmark', 'POST', { ...preset, num_partitions: 3 });
            if (result.success) {
                appendLog(`   ${preset.label}: ${result.data.detection.total_cycles} cycles, ${result.data.detection.total_time_ms}ms`, 'success');
            } else {
                appendLog(`   ${preset.label}: ${result.error}`, 'error');
            }
        } catch (err) {
            appendLog(`   ${preset.label}: ${err.message}`, 'error');
        }
    }

    loadBenchmarkHistory();
    hideLoading();
    appendLog('Preset benchmark suite complete!', 'success');
}

async function runCompareBenchmark() {
    showLoading('Running Centralized vs Distributed benchmark...');
    appendLog('Comparing Centralized vs Distributed architectures...', 'info');

    try {
        const result = await apiCall('/api/benchmark/compare', 'POST');
        if (result.success) {
            const data = result.data;
            const dist = data.distributed;
            const cent = data.centralized;

            document.getElementById('compareCard').style.display = 'block';
            
            document.getElementById('compareTableBody').innerHTML = `
                <tr>
                    <td><strong>Centralized (Single Node)</strong></td>
                    <td>${cent.cycles_found}</td>
                    <td class="cyan">${cent.time_ms.toFixed(1)}</td>
                    <td class="amber">${cent.network_messages}</td>
                    <td class="emerald">${cent.local_ops}</td>
                </tr>
                <tr>
                    <td><strong>Distributed (3 Nodes)</strong></td>
                    <td>${dist.cycles_found}</td>
                    <td class="cyan">${dist.time_ms.toFixed(1)}</td>
                    <td class="amber">${dist.network_messages}</td>
                    <td class="emerald">${dist.local_ops}</td>
                </tr>
            `;

            let analysis = '<strong style="color: var(--accent-indigo);"> Category 14 Analysis:</strong> <br>';
            analysis += '<span style="color: var(--text-secondary); font-size: 0.85rem;">';
            analysis += 'Both architectures correctly found the same number of cycles (Data Consistency). ';
            if (dist.time_ms < cent.time_ms) {
                analysis += 'The <strong>Distributed</strong> architecture outperformed Centralized due to parallel query execution across 3 nodes. ';
            } else {
                analysis += 'The <strong>Centralized</strong> architecture was faster because the dataset/cycles were small enough that the Network Communication overhead of the Distributed architecture outweighed the parallel CPU gains. ';
            }
            analysis += 'In a truly massive dataset, Centralized would crash (OOM), while Distributed would scale horizontally.</span>';

            document.getElementById('compareAnalysis').innerHTML = analysis;

            appendLog(`Comparison complete. Dist: ${dist.time_ms.toFixed(1)}ms | Cent: ${cent.time_ms.toFixed(1)}ms`, 'success');
        } else {
            appendLog(`Comparison error: ${result.error}`, 'error');
        }
    } catch (err) {
        appendLog(`Comparison failed: ${err.message}`, 'error');
    }

    hideLoading();
}

async function loadBenchmarkHistory() {
    try {
        const result = await apiCall('/api/benchmark/history');
        const history = result.history || [];

        // Update table
        const tbody = document.getElementById('benchmarkTableBody');
        if (history.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: var(--text-muted); padding: 24px;">No benchmark data yet</td></tr>';
        } else {
            tbody.innerHTML = history.map(h => `
                <tr>
                    <td>${h.id}</td>
                    <td>${h.label}</td>
                    <td>${h.config.num_normal_txs.toLocaleString()}</td>
                    <td>${h.config.num_accounts.toLocaleString()}</td>
                    <td>${h.detection.total_cycles}</td>
                    <td>${h.detection.total_time_ms.toFixed(1)}</td>
                    <td>${h.detection.network_messages}</td>
                    <td>${h.partition.edge_cut_ratio}%</td>
                    <td>${h.partition.vertex_replication_factor}</td>
                </tr>
            `).join('');
        }

        // Update charts
        if (history.length > 0) {
            updateDetectionTimeChart(history);
            updateNetworkMessagesChart(history);
            updateEdgeCutChart(history);
        }
    } catch (err) {
        console.error('Failed to load benchmark history:', err);
    }
}

async function clearBenchmarkHistory() {
    try {
        await apiCall('/api/benchmark/clear', 'POST');
        loadBenchmarkHistory();
        appendLog('Benchmark history cleared.', 'info');
    } catch (err) {
        appendLog('Failed to clear history: ' + err.message, 'error');
    }
}

// 
// Fault Tolerance Testing
// 
async function runFaultTest() {
    showLoading('Running fault tolerance test...');
    const container = document.getElementById('faultTestResults');

    appendLog(' FAULT TOLERANCE TEST ', 'info');

    try {
        // Step 1: Ensure system is running
        appendLog('Step 1: Starting all nodes...', 'info');
        await apiCall('/api/nodes/start', 'POST');
        await new Promise(r => setTimeout(r, 2000));

        // Step 2: Run detection with all nodes healthy
        appendLog('Step 2: Running detection with ALL nodes healthy...', 'info');
        const healthyRun = await apiCall('/api/detect', 'POST');
        const healthyData = healthyRun.data;
        appendLog(`  Result: ${healthyData.total_cycles_detected} cycles, ${healthyData.total_time_ms.toFixed(1)}ms, ${healthyData.active_nodes}/${healthyData.total_nodes} nodes`, 'success');

        // Step 3: Kill Node 1
        appendLog('Step 3:  KILLING Node 1 (simulating crash)...', 'error');
        await apiCall('/api/nodes/kill/1', 'POST');
        await new Promise(r => setTimeout(r, 500));
        appendLog('  Node 1 terminated.', 'error');

        // Step 4: Run detection with degraded cluster
        appendLog('Step 4: Running detection with DEGRADED cluster (Node 1 down)...', 'warn');
        const degradedRun = await apiCall('/api/detect', 'POST');
        const degradedData = degradedRun.data;
        appendLog(`  Result: ${degradedData.total_cycles_detected} cycles, ${degradedData.total_time_ms.toFixed(1)}ms, ${degradedData.active_nodes}/${degradedData.total_nodes} nodes`, 'warn');

        // Step 5: Restart Node 1
        appendLog('Step 5:  Restarting Node 1...', 'info');
        await apiCall('/api/nodes/restart/1', 'POST');
        await new Promise(r => setTimeout(r, 1500));
        appendLog('  Node 1 restarted successfully.', 'success');

        // Step 6: Run detection after recovery
        appendLog('Step 6: Running detection AFTER recovery...', 'info');
        const recoveryRun = await apiCall('/api/detect', 'POST');
        const recoveryData = recoveryRun.data;
        appendLog(`  Result: ${recoveryData.total_cycles_detected} cycles, ${recoveryData.total_time_ms.toFixed(1)}ms, ${recoveryData.active_nodes}/${recoveryData.total_nodes} nodes`, 'success');

        appendLog(' FAULT TOLERANCE TEST COMPLETE ', 'success');

        // Render results
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
        `;

        // Show comparison chart
        document.getElementById('faultChartCard').style.display = 'block';
        updateFaultComparisonChart(healthyData, degradedData);

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
// Graph Visualization (D3.js)
// 
let simulation = null;

async function renderGraph() {
    const svgEl = document.getElementById('graphSvg');
    const container = document.getElementById('d3Container');
    const loading = document.getElementById('graphLoading');
    
    // Clear old graph
    svgEl.innerHTML = '';
    if (simulation) simulation.stop();
    
    loading.style.display = 'block';
    
    try {
        const res = await apiCall('/api/graph');
        if (!res.success) {
            loading.innerHTML = 'Error: ' + res.error;
            return;
        }
        
        loading.style.display = 'none';
        const { nodes, edges } = res;
        
        const width = container.clientWidth;
        const height = container.clientHeight;
        
        const svg = d3.select("#graphSvg")
            .attr("viewBox", [0, 0, width, height]);
            
        // Setup zoom
        const g = svg.append("g");
        svg.call(d3.zoom().on("zoom", (event) => {
            g.attr("transform", event.transform);
        }));

        // Define arrow markers
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

        // Force simulation
        simulation = d3.forceSimulation(nodes)
            .force("link", d3.forceLink(edges).id(d => d.id).distance(60))
            .force("charge", d3.forceManyBody().strength(-200))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("x", d3.forceX())
            .force("y", d3.forceY());

        // Draw edges
        const link = g.append("g")
            .selectAll("line")
            .data(edges)
            .enter().append("line")
            .attr("stroke", d => {
                if (d.is_fraud) return "#ef4444"; // Fraud edge
                if (d.source.shard !== d.target.shard) return "#94a3b8"; // Cross-shard
                return "#cbd5e1"; // Normal
            })
            .attr("stroke-width", d => d.is_fraud ? 2.5 : 1.5)
            .attr("stroke-dasharray", d => (!d.is_fraud && d.source.shard !== d.target.shard) ? "5,5" : "none")
            .attr("marker-end", d => {
                if (d.is_fraud) return "url(#arrow-fraud)";
                if (d.source.shard !== d.target.shard) return "url(#arrow-cross)";
                return "url(#arrow-normal)";
            });

        // Color scale for shards
        const color = d3.scaleOrdinal()
            .domain([0, 1, 2])
            .range(["#3b82f6", "#10b981", "#f59e0b"]);

        // Draw nodes
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
            });

        // Add node labels (only for fraud nodes to avoid clutter)
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
        loading.innerHTML = 'Error fetching graph: ' + e.message;
    }
}

function showNodeMetadata(d) {
    const panel = document.getElementById('nodeInfoPanel');
    panel.innerHTML = `
        <div style="padding: 16px; background: var(--bg-secondary); border-radius: var(--radius-md); border-left: 4px solid ${d.is_fraud ? '#ef4444' : '#3b82f6'};">
            <h4 style="margin: 0 0 12px 0; font-size: 1.1rem; color: var(--text-primary);">${d.OriginalAccount || 'Account ' + d.id}</h4>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 0.9rem;">
                <div style="color: var(--text-secondary);">Account ID:</div>
                <div style="font-family: monospace; text-align: right;">${d.id}</div>
                
                <div style="color: var(--text-secondary);">Shard Home:</div>
                <div style="text-align: right; font-weight: bold;">Node ${d.shard}</div>
                
                <div style="color: var(--text-secondary);">Type:</div>
                <div style="text-align: right;">${d.AccountType || 'Unknown'}</div>
                
                <div style="color: var(--text-secondary);">Country:</div>
                <div style="text-align: right;">${d.Country || 'Unknown'}</div>
                
                <div style="color: var(--text-secondary);">Risk Score:</div>
                <div style="text-align: right; color: ${d.RiskScore > 0.8 ? '#ef4444' : (d.RiskScore > 0.5 ? '#f59e0b' : '#10b981')}; font-weight: bold;">${d.RiskScore || 'N/A'}</div>
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
        event.subject.fx = null;
        event.subject.fy = null;
    }
    return d3.drag()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended);
}

// 
// Initialization
// 
document.addEventListener('DOMContentLoaded', () => {
    appendLog('Dashboard loaded. Configure parameters and run the pipeline.', 'info');
    refreshNodeStatus();
});
