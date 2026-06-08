/**
 * charts.js - Cấu hình & Quản lý Chart.js
 * =================================================
 * Quản lý toàn bộ các biểu đồ Chart.js trong dashboard.
 * Cung cấp hàm khởi tạo và cập nhật biểu đồ cho:
 * - Phân phối cạnh và đỉnh trên từng phân mảnh
 */

// Các cấu hình mặc định toàn cục của Chart.js
Chart.defaults.color = '#64748b';
Chart.defaults.borderColor = 'rgba(0, 0, 0, 0.05)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 12;

// Đối tượng biểu đồ
const charts = {};

// Bảng mã màu
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
 * Khởi tạo hoặc cập nhật biểu đồ phân phối phân mảnh.
 */
function updatePartitionCharts(partitionStats) {
    if (!partitionStats || !partitionStats.length) return;

    // Phân phối cạnh
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

    // Phân phối đỉnh
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

