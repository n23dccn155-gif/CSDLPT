/**
 * charts.js - Chart.js Configuration & Management
 * =================================================
 * Manages all Chart.js instances for the dashboard.
 * Provides functions to create and update charts for:
 * - Detection time comparison
 * - Network messages vs dataset size
 * - Edge-cut ratio trends
 * - Partition distribution
 * - Fault tolerance comparison
 */

// Chart.js global defaults
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(0, 0, 0, 0.05)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 12;

// Chart instances
const charts = {};

// Color palette
const CHART_COLORS = {
    indigo: 'rgba(99, 102, 241, 1)',
    indigoFaded: 'rgba(99, 102, 241, 0.3)',
    emerald: 'rgba(16, 185, 129, 1)',
    emeraldFaded: 'rgba(16, 185, 129, 0.3)',
    amber: 'rgba(245, 158, 11, 1)',
    amberFaded: 'rgba(245, 158, 11, 0.3)',
    rose: 'rgba(244, 63, 94, 1)',
    roseFaded: 'rgba(244, 63, 94, 0.3)',
    cyan: 'rgba(6, 182, 212, 1)',
    cyanFaded: 'rgba(6, 182, 212, 0.3)',
    violet: 'rgba(139, 92, 246, 1)',
    violetFaded: 'rgba(139, 92, 246, 0.3)',
};

/**
 * Create or update the Detection Time bar chart.
 */
function updateDetectionTimeChart(history) {
    const ctx = document.getElementById('chartDetectionTime');
    if (!ctx) return;

    if (charts.detectionTime) {
        charts.detectionTime.destroy();
    }

    const labels = history.map(h => h.label || `Run #${h.id}`);
    const data = history.map(h => h.detection.total_time_ms);

    charts.detectionTime = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Detection Time (ms)',
                data,
                backgroundColor: data.map((_, i) => {
                    const colors = [CHART_COLORS.indigoFaded, CHART_COLORS.emeraldFaded, CHART_COLORS.amberFaded, CHART_COLORS.cyanFaded, CHART_COLORS.violetFaded, CHART_COLORS.roseFaded];
                    return colors[i % colors.length];
                }),
                borderColor: data.map((_, i) => {
                    const colors = [CHART_COLORS.indigo, CHART_COLORS.emerald, CHART_COLORS.amber, CHART_COLORS.cyan, CHART_COLORS.violet, CHART_COLORS.rose];
                    return colors[i % colors.length];
                }),
                borderWidth: 2,
                borderRadius: 8,
                barPercentage: 0.6,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    borderColor: 'rgba(0, 0, 0, 0.1)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12,
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Time (ms)', color: '#64748b' },
                    grid: { color: 'rgba(0, 0, 0, 0.03)' },
                },
                x: {
                    grid: { display: false },
                }
            }
        }
    });
}

/**
 * Create or update the Network Messages chart.
 */
function updateNetworkMessagesChart(history) {
    const ctx = document.getElementById('chartNetworkMessages');
    if (!ctx) return;

    if (charts.networkMessages) {
        charts.networkMessages.destroy();
    }

    const labels = history.map(h => h.label || `Run #${h.id}`);

    charts.networkMessages = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Network Messages',
                    data: history.map(h => h.detection.network_messages),
                    borderColor: CHART_COLORS.amber,
                    backgroundColor: CHART_COLORS.amberFaded,
                    tension: 0.3,
                    fill: true,
                    pointBackgroundColor: CHART_COLORS.amber,
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    pointRadius: 5,
                },
                {
                    label: 'Local Operations',
                    data: history.map(h => h.detection.local_ops),
                    borderColor: CHART_COLORS.emerald,
                    backgroundColor: CHART_COLORS.emeraldFaded,
                    tension: 0.3,
                    fill: true,
                    pointBackgroundColor: CHART_COLORS.emerald,
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    pointRadius: 5,
                },
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { usePointStyle: true, pointStyle: 'circle', padding: 16 }
                },
                tooltip: {
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    borderColor: 'rgba(0, 0, 0, 0.1)',
                    borderWidth: 1,
                    cornerRadius: 8,
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Count', color: '#64748b' },
                    grid: { color: 'rgba(0, 0, 0, 0.03)' },
                },
                x: { grid: { display: false } }
            }
        }
    });
}

/**
 * Create or update the Edge-Cut Ratio chart.
 */
function updateEdgeCutChart(history) {
    const ctx = document.getElementById('chartEdgeCut');
    if (!ctx) return;

    if (charts.edgeCut) {
        charts.edgeCut.destroy();
    }

    const labels = history.map(h => h.label || `Run #${h.id}`);

    charts.edgeCut = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Edge-Cut Ratio (%)',
                data: history.map(h => h.partition.edge_cut_ratio),
                borderColor: CHART_COLORS.rose,
                backgroundColor: CHART_COLORS.roseFaded,
                tension: 0.3,
                fill: true,
                pointBackgroundColor: CHART_COLORS.rose,
                pointBorderColor: '#ffffff',
                pointBorderWidth: 2,
                pointRadius: 5,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    borderColor: 'rgba(0, 0, 0, 0.1)',
                    borderWidth: 1,
                    cornerRadius: 8,
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    title: { display: true, text: 'Edge-Cut Ratio (%)', color: '#64748b' },
                    grid: { color: 'rgba(0, 0, 0, 0.03)' },
                },
                x: { grid: { display: false } }
            }
        }
    });
}

/**
 * Create or update the Partition Distribution charts.
 */
function updatePartitionCharts(partitionStats) {
    if (!partitionStats || !partitionStats.length) return;

    // Edge distribution
    const ctxEdge = document.getElementById('chartPartitionDist');
    if (ctxEdge) {
        if (charts.partitionDist) charts.partitionDist.destroy();

        charts.partitionDist = new Chart(ctxEdge, {
            type: 'doughnut',
            data: {
                labels: partitionStats.map(p => `Partition ${p.partition_id}`),
                datasets: [{
                    data: partitionStats.map(p => p.num_edges),
                    backgroundColor: [CHART_COLORS.indigoFaded, CHART_COLORS.emeraldFaded, CHART_COLORS.amberFaded, CHART_COLORS.roseFaded, CHART_COLORS.cyanFaded],
                    borderColor: [CHART_COLORS.indigo, CHART_COLORS.emerald, CHART_COLORS.amber, CHART_COLORS.rose, CHART_COLORS.cyan],
                    borderWidth: 2,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '55%',
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { usePointStyle: true, padding: 16 }
                    }
                }
            }
        });
    }

    // Vertex distribution
    const ctxVertex = document.getElementById('chartVertexDist');
    if (ctxVertex) {
        if (charts.vertexDist) charts.vertexDist.destroy();

        charts.vertexDist = new Chart(ctxVertex, {
            type: 'doughnut',
            data: {
                labels: partitionStats.map(p => `Partition ${p.partition_id}`),
                datasets: [{
                    data: partitionStats.map(p => p.num_vertices),
                    backgroundColor: [CHART_COLORS.violetFaded, CHART_COLORS.cyanFaded, CHART_COLORS.roseFaded, CHART_COLORS.emeraldFaded, CHART_COLORS.amberFaded],
                    borderColor: [CHART_COLORS.violet, CHART_COLORS.cyan, CHART_COLORS.rose, CHART_COLORS.emerald, CHART_COLORS.amber],
                    borderWidth: 2,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '55%',
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { usePointStyle: true, padding: 16 }
                    }
                }
            }
        });
    }
}

/**
 * Create the Fault Tolerance comparison chart.
 */
function updateFaultComparisonChart(healthyResult, degradedResult) {
    const ctx = document.getElementById('chartFaultComparison');
    if (!ctx) return;

    if (charts.faultComparison) {
        charts.faultComparison.destroy();
    }

    charts.faultComparison = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Cycles Detected', 'Detection Time (ms)', 'Network Messages', 'Active Nodes'],
            datasets: [
                {
                    label: 'All Nodes Healthy',
                    data: [
                        healthyResult.total_cycles_detected,
                        healthyResult.total_time_ms,
                        healthyResult.aggregate_stats.total_network_messages,
                        healthyResult.active_nodes,
                    ],
                    backgroundColor: CHART_COLORS.emeraldFaded,
                    borderColor: CHART_COLORS.emerald,
                    borderWidth: 2,
                    borderRadius: 6,
                },
                {
                    label: 'Node 1 Down (Degraded)',
                    data: [
                        degradedResult.total_cycles_detected,
                        degradedResult.total_time_ms,
                        degradedResult.aggregate_stats.total_network_messages,
                        degradedResult.active_nodes,
                    ],
                    backgroundColor: CHART_COLORS.roseFaded,
                    borderColor: CHART_COLORS.rose,
                    borderWidth: 2,
                    borderRadius: 6,
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { usePointStyle: true, pointStyle: 'rect', padding: 16 }
                },
                tooltip: {
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    borderColor: 'rgba(0, 0, 0, 0.1)',
                    borderWidth: 1,
                    cornerRadius: 8,
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(0, 0, 0, 0.03)' },
                },
                x: { grid: { display: false } }
            }
        }
    });
}
