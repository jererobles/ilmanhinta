// Dashboard JavaScript for Ilmanhinta Energy System

// Global chart instances
let consumptionChart = null;
let priceChart = null;
let comparisonChart = null;

// Initialize dashboard on page load
document.addEventListener('DOMContentLoaded', function() {
    initializeCharts();
    loadDashboardData();
    setInterval(loadDashboardData, 60000); // Refresh every minute

    // Add event listeners for time range selectors
    document.getElementById('consumptionTimeRange').addEventListener('change', updateConsumptionChart);
    document.getElementById('priceTimeRange').addEventListener('change', updatePriceChart);
});

// Initialize all charts
function initializeCharts() {
    // Consumption Forecast Chart
    const consumptionCtx = document.getElementById('consumptionChart').getContext('2d');
    consumptionChart = new Chart(consumptionCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Predicted Consumption',
                data: [],
                borderColor: 'rgb(59, 130, 246)',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                tension: 0.3,
                fill: true
            }, {
                label: 'Confidence Range',
                data: [],
                borderColor: 'rgba(59, 130, 246, 0.3)',
                backgroundColor: 'rgba(59, 130, 246, 0.05)',
                borderDash: [5, 5],
                fill: '+1'
            }, {
                label: '',
                data: [],
                borderColor: 'rgba(59, 130, 246, 0.3)',
                backgroundColor: 'rgba(59, 130, 246, 0.05)',
                borderDash: [5, 5],
                fill: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        filter: function(item) {
                            return item.text !== '';
                        }
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (context.parsed.y !== null) {
                                label += context.parsed.y.toFixed(1) + ' MW';
                            }
                            return label;
                        }
                    }
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Consumption (MW)'
                    }
                }
            }
        }
    });

    // Price Prediction Chart
    const priceCtx = document.getElementById('priceChart').getContext('2d');
    priceChart = new Chart(priceCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'ML Prediction',
                data: [],
                borderColor: 'rgb(34, 197, 94)',
                backgroundColor: 'rgba(34, 197, 94, 0.1)',
                tension: 0.3,
                fill: true
            }, {
                label: 'Fingrid Forecast',
                data: [],
                borderColor: 'rgb(251, 146, 60)',
                backgroundColor: 'rgba(251, 146, 60, 0.1)',
                tension: 0.3,
                fill: false,
                borderDash: [5, 5]
            }, {
                label: 'Actual Price',
                data: [],
                borderColor: 'rgb(147, 51, 234)',
                backgroundColor: 'rgba(147, 51, 234, 0.1)',
                tension: 0,
                fill: false,
                pointRadius: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (context.parsed.y !== null) {
                                label += '€' + context.parsed.y.toFixed(2) + '/MWh';
                            }
                            return label;
                        }
                    }
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Price (€/MWh)'
                    }
                }
            }
        }
    });

    // Model Comparison Chart
    const comparisonCtx = document.getElementById('comparisonChart').getContext('2d');
    comparisonChart = new Chart(comparisonCtx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'ML Model Error',
                data: [],
                backgroundColor: 'rgba(34, 197, 94, 0.7)',
                borderColor: 'rgb(34, 197, 94)',
                borderWidth: 2
            }, {
                label: 'Fingrid Error',
                data: [],
                backgroundColor: 'rgba(251, 146, 60, 0.7)',
                borderColor: 'rgb(251, 146, 60)',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Hour of Day'
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Average Error (€/MWh)'
                    },
                    beginAtZero: true
                }
            }
        }
    });
}

// Load all dashboard data
async function loadDashboardData() {
    try {
        // Load peak consumption
        await loadPeakConsumption();

        // Load current prices
        await loadPrices();

        // Load model performance
        await loadModelPerformance();

        // Load system health
        await loadSystemHealth();

        // Update charts
        await updateConsumptionChart();
        await updatePriceChart();
        await updateComparisonChart();

        // Update last refresh time
        updateLastRefreshTime();

    } catch (error) {
        console.error('Error loading dashboard data:', error);
        showToast('Error loading data', 'error');
    }
}

// Load peak consumption data
async function loadPeakConsumption() {
    try {
        const response = await fetch('/predict/peak');
        const data = await response.json();

        document.getElementById('peakConsumption').textContent = data.peak_consumption_mw.toFixed(1);
        document.getElementById('peakTime').textContent =
            new Date(data.peak_timestamp).toLocaleTimeString('fi-FI', { hour: '2-digit', minute: '2-digit' });

    } catch (error) {
        console.error('Error loading peak consumption:', error);
        document.getElementById('peakConsumption').textContent = '--';
    }
}

// Load price data
async function loadPrices() {
    try {
        const response = await fetch('/prices/latest_predictions?hours_ahead=1');
        const data = await response.json();

        if (data && data.length > 0) {
            const currentPrice = data[0].predicted_price_eur_mwh;
            document.getElementById('currentPrice').textContent = currentPrice.toFixed(2);
            document.getElementById('mlPrediction').textContent = currentPrice.toFixed(2);

            // Fetch comparison to get actual price
            const compResponse = await fetch('/prices/comparison?hours_back=1&only_with_actuals=true');
            if (compResponse.ok) {
                const compData = await compResponse.json();
                if (compData && compData.length > 0) {
                    const actualPrice = compData[0].actual_price;
                    if (actualPrice) {
                        document.getElementById('currentPrice').textContent = actualPrice.toFixed(2);

                        // Calculate price change
                        const change = ((currentPrice - actualPrice) / actualPrice * 100).toFixed(1);
                        const changeElement = document.getElementById('priceChange');
                        changeElement.textContent = change > 0 ? `+${change}%` : `${change}%`;
                        changeElement.className = change > 0 ? 'text-red-500 text-sm font-semibold' : 'text-green-500 text-sm font-semibold';
                    }
                }
            }
        }
    } catch (error) {
        console.error('Error loading prices:', error);
        document.getElementById('currentPrice').textContent = '--';
    }
}

// Load model performance metrics
async function loadModelPerformance() {
    try {
        const response = await fetch('/prices/performance/summary?hours_back=24');
        const data = await response.json();

        // Update accuracy card
        const maeMetric = data.metrics.find(m => m.metric_name === 'Mean Absolute Error (MAE)');
        if (maeMetric) {
            document.getElementById('modelAccuracy').textContent = maeMetric.ml_value?.toFixed(2) || '--';
        }

        // Update win rate
        document.getElementById('winRate').textContent = data.ml_win_rate?.toFixed(1) || '--';

        // Update performance table
        updatePerformanceTable(data.metrics);

    } catch (error) {
        console.error('Error loading model performance:', error);
    }
}

// Load system health
async function loadSystemHealth() {
    try {
        const response = await fetch('/health');
        const data = await response.json();

        if (data.predictions_available) {
            document.getElementById('systemStatus').innerHTML = '<i class="fas fa-circle text-xs mr-1"></i>Healthy';
            document.getElementById('systemStatus').className = 'text-green-500 text-sm font-semibold';

            // Get prediction count
            const predsResponse = await fetch('/predict/forecast');
            const predsData = await predsResponse.json();
            document.getElementById('predictionCount').textContent = predsData.length || '--';
        } else {
            document.getElementById('systemStatus').innerHTML = '<i class="fas fa-circle text-xs mr-1"></i>No Data';
            document.getElementById('systemStatus').className = 'text-yellow-500 text-sm font-semibold';
            document.getElementById('predictionCount').textContent = '0';
        }

    } catch (error) {
        console.error('Error loading system health:', error);
        document.getElementById('systemStatus').innerHTML = '<i class="fas fa-circle text-xs mr-1"></i>Error';
        document.getElementById('systemStatus').className = 'text-red-500 text-sm font-semibold';
    }
}

// Update consumption chart
async function updateConsumptionChart() {
    try {
        const hours = document.getElementById('consumptionTimeRange').value;
        const response = await fetch('/predict/forecast');
        const data = await response.json();

        // Limit data based on selected time range
        const limitedData = data.slice(0, parseInt(hours));

        // Prepare chart data
        const labels = limitedData.map(d =>
            new Date(d.timestamp).toLocaleString('fi-FI', {
                day: '2-digit',
                month: '2-digit',
                hour: '2-digit',
                minute: '2-digit'
            })
        );

        const predictions = limitedData.map(d => d.predicted_consumption_mw);
        const lowerBounds = limitedData.map(d => d.confidence_lower);
        const upperBounds = limitedData.map(d => d.confidence_upper);

        // Update chart
        consumptionChart.data.labels = labels;
        consumptionChart.data.datasets[0].data = predictions;
        consumptionChart.data.datasets[1].data = upperBounds;
        consumptionChart.data.datasets[2].data = lowerBounds;
        consumptionChart.update();

    } catch (error) {
        console.error('Error updating consumption chart:', error);
    }
}

// Update price chart
async function updatePriceChart() {
    try {
        const hours = document.getElementById('priceTimeRange').value;
        const response = await fetch(`/prices/comparison?hours_back=${hours}&limit=${hours}`);
        const data = await response.json();

        // Sort data by time
        data.sort((a, b) => new Date(a.time) - new Date(b.time));

        // Prepare chart data
        const labels = data.map(d =>
            new Date(d.time).toLocaleString('fi-FI', {
                day: '2-digit',
                month: '2-digit',
                hour: '2-digit',
                minute: '2-digit'
            })
        );

        const mlPredictions = data.map(d => d.ml_prediction);
        const fingridForecasts = data.map(d => d.fingrid_forecast);
        const actualPrices = data.map(d => d.actual_price);

        // Update chart
        priceChart.data.labels = labels;
        priceChart.data.datasets[0].data = mlPredictions;
        priceChart.data.datasets[1].data = fingridForecasts;
        priceChart.data.datasets[2].data = actualPrices;
        priceChart.update();

    } catch (error) {
        console.error('Error updating price chart:', error);
    }
}

// Update comparison chart
async function updateComparisonChart() {
    try {
        const response = await fetch('/prices/performance/hourly?hours_back=24');
        const data = await response.json();

        // Sort by hour
        data.sort((a, b) => new Date(a.hour) - new Date(b.hour));

        // Prepare chart data
        const labels = data.map(d =>
            new Date(d.hour).toLocaleTimeString('fi-FI', { hour: '2-digit' })
        );

        const mlErrors = data.map(d => d.ml_avg_error || 0);
        const fingridErrors = data.map(d => d.fingrid_avg_error || 0);

        // Update chart
        comparisonChart.data.labels = labels;
        comparisonChart.data.datasets[0].data = mlErrors;
        comparisonChart.data.datasets[1].data = fingridErrors;
        comparisonChart.update();

    } catch (error) {
        console.error('Error updating comparison chart:', error);
    }
}

// Update performance table
function updatePerformanceTable(metrics) {
    const tableBody = document.getElementById('performanceTable');
    tableBody.innerHTML = '';

    metrics.forEach(metric => {
        const row = document.createElement('tr');
        row.className = 'border-b hover:bg-gray-50';

        const improvement = metric.improvement_pct;
        const improvementClass = improvement && improvement > 0 ? 'text-green-600' :
                                improvement && improvement < 0 ? 'text-red-600' : 'text-gray-600';
        const improvementText = improvement ?
            (improvement > 0 ? `+${improvement.toFixed(1)}%` : `${improvement.toFixed(1)}%`) : '--';

        row.innerHTML = `
            <td class="py-2 px-4 font-medium">${metric.metric_name}</td>
            <td class="text-center py-2 px-4">${metric.ml_value?.toFixed(2) || '--'}</td>
            <td class="text-center py-2 px-4">${metric.fingrid_value?.toFixed(2) || '--'}</td>
            <td class="text-center py-2 px-4 font-semibold ${improvementClass}">${improvementText}</td>
        `;

        tableBody.appendChild(row);
    });
}

// Trigger Dagster job
async function triggerJob(jobName) {
    try {
        // Show loading state
        showJobStatus(`Triggering ${jobName}...`, 'info');

        // Call the API endpoint to trigger the job
        const response = await fetch(`/trigger-job/${jobName}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });

        const result = await response.json();

        if (response.ok) {
            showJobStatus(`Job ${jobName} triggered successfully! Run ID: ${result.run_id || 'N/A'}`, 'success');
            showToast(`${jobName} started successfully`, 'success');

            // Refresh data after a delay
            setTimeout(() => {
                loadDashboardData();
            }, 5000);
        } else {
            showJobStatus(`Failed to trigger ${jobName}: ${result.detail || 'Unknown error'}`, 'error');
            showToast(`Failed to trigger ${jobName}`, 'error');
        }

    } catch (error) {
        console.error('Error triggering job:', error);
        showJobStatus(`Error triggering ${jobName}: ${error.message}`, 'error');
        showToast('Error triggering job', 'error');
    }
}

// Show job status message
function showJobStatus(message, type) {
    const statusDiv = document.getElementById('jobStatus');
    const statusMessage = document.getElementById('jobStatusMessage');

    statusDiv.classList.remove('hidden');
    statusMessage.textContent = message;

    // Update styling based on type
    const container = statusDiv.querySelector('div');
    container.className = type === 'success' ? 'bg-green-50 border-l-4 border-green-400 p-4 rounded' :
                         type === 'error' ? 'bg-red-50 border-l-4 border-red-400 p-4 rounded' :
                         'bg-blue-50 border-l-4 border-blue-400 p-4 rounded';

    const icon = statusDiv.querySelector('i');
    icon.className = type === 'success' ? 'fas fa-check-circle text-green-400' :
                    type === 'error' ? 'fas fa-exclamation-circle text-red-400' :
                    'fas fa-info-circle text-blue-400';

    const text = statusDiv.querySelector('p');
    text.className = type === 'success' ? 'text-sm text-green-700' :
                     type === 'error' ? 'text-sm text-red-700' :
                     'text-sm text-blue-700';

    // Hide after 10 seconds
    setTimeout(() => {
        statusDiv.classList.add('hidden');
    }, 10000);
}

// Show toast notification
function showToast(message, type = 'success') {
    const toast = document.getElementById('toast');
    const toastMessage = document.getElementById('toastMessage');

    toastMessage.textContent = message;

    // Update styling based on type
    const toastDiv = toast.querySelector('div');
    toastDiv.className = type === 'success' ?
        'bg-green-500 text-white px-6 py-3 rounded-lg shadow-lg flex items-center' :
        'bg-red-500 text-white px-6 py-3 rounded-lg shadow-lg flex items-center';

    const icon = toast.querySelector('i');
    icon.className = type === 'success' ? 'fas fa-check-circle mr-2' : 'fas fa-exclamation-circle mr-2';

    // Show toast
    toast.classList.remove('hidden');
    toast.classList.add('translate-y-0');

    // Hide after 3 seconds
    setTimeout(() => {
        toast.classList.add('translate-y-20');
        setTimeout(() => {
            toast.classList.add('hidden');
            toast.classList.remove('translate-y-0', 'translate-y-20');
        }, 300);
    }, 3000);
}

// Refresh all data
function refreshAll() {
    loadDashboardData();
    showToast('Dashboard refreshed', 'success');
}

// Update last refresh time
function updateLastRefreshTime() {
    const now = new Date();
    const timeString = now.toLocaleTimeString('fi-FI', { hour: '2-digit', minute: '2-digit' });
    document.getElementById('updateTime').textContent = timeString;
}

// Toggle comparison metric
function toggleComparisonMetric(metric) {
    // Update button states
    const buttons = document.querySelectorAll('.comparison-btn');
    buttons.forEach(btn => btn.classList.remove('active', 'bg-purple-100', 'text-purple-700'));

    event.target.classList.add('active', 'bg-purple-100', 'text-purple-700');

    // Update chart based on selected metric
    if (metric === 'accuracy') {
        // Show accuracy metrics instead of errors
        updateComparisonChartAccuracy();
    } else {
        // Show error metrics
        updateComparisonChart();
    }
}

// Update comparison chart with accuracy metrics
async function updateComparisonChartAccuracy() {
    try {
        const response = await fetch('/prices/insights/win-conditions?group_by=hour&min_cases=5');
        const data = await response.json();

        // Extract hour from condition string and sort
        const processedData = data.map(d => ({
            hour: parseInt(d.condition.split(':')[1]),
            winRate: d.win_rate
        })).sort((a, b) => a.hour - b.hour);

        // Prepare chart data
        const labels = processedData.map(d => `${d.hour}:00`);
        const winRates = processedData.map(d => d.winRate);
        const fingridWinRates = processedData.map(d => 100 - d.winRate);

        // Update chart
        comparisonChart.data.labels = labels;
        comparisonChart.data.datasets[0].label = 'ML Win Rate';
        comparisonChart.data.datasets[0].data = winRates;
        comparisonChart.data.datasets[1].label = 'Fingrid Win Rate';
        comparisonChart.data.datasets[1].data = fingridWinRates;
        comparisonChart.options.scales.y.title.text = 'Win Rate (%)';
        comparisonChart.update();

    } catch (error) {
        console.error('Error updating accuracy comparison:', error);
    }
}
