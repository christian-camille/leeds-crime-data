let map;
let heatLayer;
let crimeData = null;

const MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
];

let totalMonths = 0;
let minDateTimestamp = 0;
let intensitySlider;
let maxCrimeCount = 100;
let currentWardData = [];
let maxAvailableDate = { year: 0, month: 0 };

async function init() {
    map = L.map('map', {
        zoomControl: true,
        attributionControl: true
    }).setView([53.8, -1.55], 11);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://carto.com/">CARTO</a> | Data: UK Police API',
        maxZoom: 18
    }).addTo(map);

    try {
        const response = await fetch(`data/crime_data.json?v=${new Date().getTime()}`);
        crimeData = await response.json();

        loadWardBoundaries();

        const locationCounts = {};
        maxCrimeCount = 0;
        maxAvailableDate = { year: 0, month: 0 };

        crimeData.p.forEach(p => {
            // Track max date
            if (p[3] > maxAvailableDate.year || (p[3] === maxAvailableDate.year && p[4] > maxAvailableDate.month)) {
                maxAvailableDate.year = p[3];
                maxAvailableDate.month = p[4];
            }

            const key = `${p[0]},${p[1]}`;
            const newCount = (locationCounts[key] || 0) + p[5];
            locationCounts[key] = newCount;
            if (newCount > maxCrimeCount) maxCrimeCount = newCount;
        });

        if (maxCrimeCount < 100) maxCrimeCount = 100;
        if (maxCrimeCount > 5000) maxCrimeCount = 5000;

        populateFilters();
        applyFilters();

        document.getElementById('loading').classList.add('hidden');
    } catch (error) {
        console.error('Failed to load crime data:', error);
        document.getElementById('loading').innerHTML = `
            <p style="color: var(--danger);">Failed to load data. Please ensure crime_data.json exists.</p>
        `;
    }
}

function populateFilters() {
    const crimeTypeSelect = document.getElementById('crime-type');
    crimeData.t.forEach(type => {
        const option = document.createElement('option');
        option.value = type;
        option.textContent = type;
        crimeTypeSelect.appendChild(option);
    });

    initSlider();
    initIntensitySlider();
}

function initIntensitySlider() {
    const slider = document.getElementById('intensity-slider');

    intensitySlider = noUiSlider.create(slider, {
        start: [0, 90],
        connect: true,
        range: {
            'min': 0,
            'max': 100
        },
        step: 1,
        tooltips: [
            { to: (v) => `Min: ${Math.round(v)}%` },
            { to: (v) => `Sens: ${Math.round(v)}%` }
        ],
        format: {
            to: (v) => Math.round(v),
            from: (v) => Number(v)
        }
    });

    slider.noUiSlider.on('update', function () {
        applyFilters();
    });
}

function initSlider() {
    const slider = document.getElementById('date-slider');

    const startYear = crimeData.y[0];
    totalMonths = (maxAvailableDate.year - startYear) * 12 + maxAvailableDate.month;
    minDateTimestamp = new Date(startYear, 0).getTime();

    noUiSlider.create(slider, {
        start: [0, totalMonths - 1],
        connect: true,
        step: 1,
        behaviour: 'drag',
        range: {
            'min': 0,
            'max': totalMonths - 1
        },
        format: {
            to: function (value) {
                return Math.round(value);
            },
            from: function (value) {
                return Math.round(value);
            }
        },
        tooltips: false
    });

    const tooltipContainer = document.createElement('div');
    tooltipContainer.className = 'merged-tooltip';
    slider.appendChild(tooltipContainer);

    slider.noUiSlider.on('update', function (values) {
        const v0 = parseInt(values[0]);
        const v1 = parseInt(values[1]);

        const percent0 = (v0 / (totalMonths - 1)) * 100;
        const percent1 = (v1 / (totalMonths - 1)) * 100;
        const centerPercent = (percent0 + percent1) / 2;

        tooltipContainer.style.left = `${centerPercent}%`;

        if (centerPercent < 20) {
            tooltipContainer.style.transform = `translateX(-${centerPercent * 2.5}%)`;
        } else if (centerPercent > 80) {
            tooltipContainer.style.transform = `translateX(-${100 - (100 - centerPercent) * 2.5}%)`;
        } else {
            tooltipContainer.style.transform = `translateX(-50%)`;
        }

        tooltipContainer.innerHTML = `${formatMonthYear(v0)} — ${formatMonthYear(v1)}`;
        applyFilters();
    });
}

function formatMonthYear(monthIndex) {
    const startYear = crimeData.y[0];
    const totalMonthIndex = Math.round(monthIndex);

    const yearOffset = Math.floor(totalMonthIndex / 12);
    const month = totalMonthIndex % 12;

    return `${MONTHS[month].substring(0, 3)} ${startYear + yearOffset}`;
}

function getDateFromIndex(index) {
    const startYear = crimeData.y[0];
    const yearOffset = Math.floor(index / 12);
    const month = (index % 12) + 1;
    const year = startYear + yearOffset;
    return { year, month };
}

function getFilterParams() {
    const slider = document.getElementById('date-slider');
    const values = slider.noUiSlider.get();

    const startDate = getDateFromIndex(parseInt(values[0]));
    const endDate = getDateFromIndex(parseInt(values[1]));

    return {
        crimeType: document.getElementById('crime-type').value,
        yearStart: startDate.year,
        yearEnd: endDate.year,
        monthStart: startDate.month,
        monthEnd: endDate.month,
        excludeCityCentre: document.getElementById('exclude-city-centre').checked
    };
}

function filterPoints(params) {
    const typeIndex = params.crimeType === 'all' ? -1 : crimeData.t.indexOf(params.crimeType);
    const hraIndex = crimeData.pd ? crimeData.pd.indexOf('HRA') : -1;

    return crimeData.p.filter(point => {
        const [lat, lon, pType, pYear, pMonth, count, isCityCentre, distIdx] = point;

        if (typeIndex !== -1 && pType !== typeIndex) {
            return false;
        }

        if (params.excludeCityCentre) {
            if (isCityCentre === 1) return false;
            if (hraIndex !== -1 && distIdx === hraIndex) return false;
        }

        if (pYear < params.yearStart || pYear > params.yearEnd) {
            return false;
        }

        if (pYear === params.yearStart && pMonth < params.monthStart) {
            return false;
        }
        if (pYear === params.yearEnd && pMonth > params.monthEnd) {
            return false;
        }

        return true;
    });
}



function applyFilters() {
    const params = getFilterParams();
    const filteredPoints = filterPoints(params);

    const aggregated = {};

    filteredPoints.forEach(point => {
        const [lat, lon, pType, pYear, pMonth, count] = point;
        const key = `${lat},${lon}`;
        if (!aggregated[key]) {
            aggregated[key] = { lat, lon, count: 0 };
        }
        aggregated[key].count += count;
    });

    const heatPoints = [];
    let localMax = 0;

    Object.values(aggregated).forEach(p => {
        if (p.count > localMax) localMax = p.count;
    });

    let minFilterPercent = 0;
    let sensitivityPercent = 100;

    if (intensitySlider) {
        [minFilterPercent, sensitivityPercent] = intensitySlider.get().map(Number);
    }

    const sortedCounts = Object.values(aggregated).map(p => p.count).sort((a, b) => a - b);
    const numPoints = sortedCounts.length;

    const minFilterIndex = Math.floor((minFilterPercent / 100) * numPoints);
    const minFilter = numPoints > 0 ? sortedCounts[Math.min(minFilterIndex, numPoints - 1)] : 0;

    const sensitivityIndex = Math.floor((sensitivityPercent / 100) * numPoints);
    const saturationPoint = numPoints > 0 ? sortedCounts[Math.min(sensitivityIndex, numPoints - 1)] : 1;

    Object.values(aggregated).forEach(p => {
        if (p.count >= minFilter) {
            heatPoints.push([p.lat, p.lon, p.count]);
        }
    });

    if (currentMapMode === 'heatmap') {
        if (geoJsonLayer) map.removeLayer(geoJsonLayer);

        if (heatLayer) {
            map.removeLayer(heatLayer);
        }

        heatLayer = L.heatLayer(heatPoints, {
            radius: 25,
            blur: 35,
            maxZoom: 15,
            max: saturationPoint > 0 ? saturationPoint : 1,
            gradient: {
                0.0: '#0d0887',
                0.2: '#5302a3',
                0.4: '#8b0aa5',
                0.6: '#db5c68',
                0.8: '#febd2a',
                1.0: '#f0f921'
            }
        }).addTo(map);
    } else {
        if (heatLayer) map.removeLayer(heatLayer);
        updateChoropleth(filteredPoints);
    }

    updateStats(filteredPoints, params);
    updateWardChart(filteredPoints);
}

let wardGeoJsonData = null;
let geoJsonLayer = null;

async function loadWardBoundaries() {
    try {
        const response = await fetch('data/leeds_wards.geojson');
        wardGeoJsonData = await response.json();
    } catch (e) {
        console.error("Failed to load ward boundaries", e);
    }
}

function updateChoropleth(points) {
    if (!wardGeoJsonData) return;

    // Track total and type breakdown per ward
    const wardCounts = {};
    let maxCount = 0;

    points.forEach(point => {
        const [, , pType, , , count, , , wardIdx] = point;

        if (wardIdx !== undefined) {
            const wardName = crimeData.w[wardIdx];
            if (!wardCounts[wardName]) {
                wardCounts[wardName] = { total: 0, types: {} };
            }

            wardCounts[wardName].total += count;
            // Track crime type counts
            wardCounts[wardName].types[pType] = (wardCounts[wardName].types[pType] || 0) + count;
        }
    });

    Object.values(wardCounts).forEach(data => {
        if (data.total > maxCount) maxCount = data.total;
    });

    if (geoJsonLayer) map.removeLayer(geoJsonLayer);

    function getColor(d) {
        return d > maxCount * 0.9 ? '#800026' :
            d > maxCount * 0.8 ? '#A00026' :
                d > maxCount * 0.7 ? '#BD0026' :
                    d > maxCount * 0.6 ? '#D50F23' :
                        d > maxCount * 0.5 ? '#E31A1C' :
                            d > maxCount * 0.4 ? '#F03523' :
                                d > maxCount * 0.3 ? '#FC4E2A' :
                                    d > maxCount * 0.2 ? '#FD7534' :
                                        d > maxCount * 0.1 ? '#FD8D3C' :
                                            d > maxCount * 0.05 ? '#FEB24C' :
                                                d > 0 ? '#FFEDA0' :
                                                    '#FFEDA0';
    }

    function style(feature) {
        const data = wardCounts[feature.properties.WARD_NAME];
        const count = data ? data.total : 0;
        return {
            fillColor: getColor(count),
            weight: 2,
            opacity: 1,
            color: 'white',
            dashArray: '3',
            fillOpacity: 0.4
        };
    }

    function highlightFeature(e) {
        const layer = e.target;
        layer.setStyle({
            weight: 4,
            color: '#6366f1',
            dashArray: '',
            fillOpacity: 0.7
        });
        layer.bringToFront();

        const data = wardCounts[layer.feature.properties.WARD_NAME];
        info.update(layer.feature.properties, data);
    }

    function resetHighlight(e) {
        geoJsonLayer.resetStyle(e.target);
        info.update();
    }

    function onEachFeature(feature, layer) {
        layer.on({
            mouseover: highlightFeature,
            mouseout: resetHighlight,
            click: (e) => {
                L.DomEvent.stopPropagation(e);
                showWardDetails(feature.properties.WARD_NAME);
            }
        });
        const data = wardCounts[feature.properties.WARD_NAME];
        const count = data ? data.total : 0;
        layer.bindTooltip(`<strong>${feature.properties.WARD_NAME}</strong><br>${count.toLocaleString()} crimes`);
    }

    geoJsonLayer = L.geoJson(wardGeoJsonData, {
        style: style,
        onEachFeature: onEachFeature
    }).addTo(map);

    if (!window.infoControlAdded && currentMapMode !== 'heatmap') {
        info.addTo(map);
        window.infoControlAdded = true;
    }
}

const info = L.control();

info.onAdd = function (map) {
    this._div = L.DomUtil.create('div', 'info');
    this.update();
    return this._div;
};

info.update = function (props, data) {
    const isAllCrimes = document.getElementById('crime-type').value === 'all';

    let content = '<h4>Ward Crime Stats</h4>';

    if (props) {
        const total = data ? data.total : 0;
        content += `<b>${props.WARD_NAME}</b><br />${total.toLocaleString()} crimes`;

        // Add Full Crime Type Breakdown if "All Crimes" is selected
        if (isAllCrimes && data && data.types) {
            const sortedCrimes = Object.entries(data.types)
                .sort((a, b) => b[1] - a[1]); // Sort desc by count

            if (sortedCrimes.length > 0) {
                content += '<div class="tooltip-header">CRIME BREAKDOWN:</div>';
                content += '<div class="tooltip-crime-list">';

                sortedCrimes.forEach(([typeIdx, count]) => {
                    const typeName = crimeData.t[typeIdx];
                    const percentage = total > 0 ? ((count / total) * 100).toFixed(1) : 0;

                    content += `<div class="tooltip-crime-item">
                        <span>${typeName}</span>
                        <b>${count.toLocaleString()} (${percentage}%)</b>
                    </div>`;
                });
                content += '</div>';
            }
        }
    } else {
        content += 'Hover over a ward';
    }

    this._div.innerHTML = content;
};

function updateStats(points, params) {
    const totalCrimes = points.reduce((sum, p) => sum + p[5], 0);
    document.getElementById('total-crimes').textContent = totalCrimes.toLocaleString();

    const startMonthName = MONTHS[params.monthStart - 1].substring(0, 3);
    const endMonthName = MONTHS[params.monthEnd - 1].substring(0, 3);
    document.getElementById('date-range').textContent =
        `${startMonthName} ${params.yearStart} - ${endMonthName} ${params.yearEnd}`;
}

function updateWardChart(points) {
    const wardTotals = {};

    points.forEach(point => {
        const [lat, lon, pType, pYear, pMonth, count, isCityCentre, distIdx, wardIdx] = point;

        if (wardIdx !== undefined) {
            const wardName = crimeData.w[wardIdx];
            if (!wardTotals[wardName]) {
                wardTotals[wardName] = 0;
            }
            wardTotals[wardName] += count;
        }
    });

    const sortedWards = Object.entries(wardTotals)
        .sort((a, b) => b[1] - a[1]);

    currentWardData = sortedWards;

    const totalVisibleCrimes = sortedWards.reduce((sum, item) => sum + item[1], 0);

    const top5Wards = sortedWards.slice(0, 5);
    const maxCount = sortedWards.length > 0 ? sortedWards[0][1] : 1;

    const chartContainer = document.getElementById('wards-chart');
    chartContainer.innerHTML = '';

    top5Wards.forEach(([ward, count]) => {
        const percentage = (count / maxCount) * 100;

        const percentageOfTotal = (count / totalVisibleCrimes) * 100;

        const barDiv = document.createElement('div');
        barDiv.className = 'ward-bar';
        barDiv.innerHTML = `
            <span class="ward-name" title="${ward}">${ward}</span>
            <div class="ward-bar-container">
                <div class="ward-bar-fill" style="width: ${percentage}%"></div>
            </div>
            <div class="ward-stats">
                <span class="ward-abs">${count.toLocaleString()}</span>
                <span class="ward-percent">${percentageOfTotal.toFixed(1)}%</span>
            </div>
        `;
        chartContainer.appendChild(barDiv);
    });
}

function resetFilters() {
    document.getElementById('crime-type').value = 'all';
    document.getElementById('exclude-city-centre').checked = false;

    const slider = document.getElementById('date-slider');
    slider.noUiSlider.set([0, totalMonths - 1]);

    if (intensitySlider) {
        intensitySlider.set([0, 90]);
    }

    applyFilters();
}

function showAllWards() {
    const modal = document.getElementById('ward-modal');
    const listContainer = document.getElementById('modal-ward-list');
    listContainer.innerHTML = '';

    const maxCount = currentWardData.length > 0 ? currentWardData[0][1] : 1;

    const totalVisibleCrimes = currentWardData.reduce((sum, item) => sum + item[1], 0);

    currentWardData.forEach(([ward, count]) => {
        const percentage = (count / maxCount) * 100;
        const percentageOfTotal = (count / totalVisibleCrimes) * 100;

        const barDiv = document.createElement('div');
        barDiv.className = 'ward-bar';
        barDiv.innerHTML = `
            <span class="ward-name" title="${ward}">${ward}</span>
            <div class="ward-bar-container">
                <div class="ward-bar-fill" style="width: ${percentage}%"></div>
            </div>
            <div class="ward-stats">
                <span class="ward-abs">${count.toLocaleString()}</span>
                <span class="ward-percent">${percentageOfTotal.toFixed(1)}%</span>
            </div>
        `;
        listContainer.appendChild(barDiv);
    });

    modal.classList.remove('hidden');
}

function closeWardModal() {
    document.getElementById('ward-modal').classList.add('hidden');
}

document.getElementById('show-all-wards').addEventListener('click', showAllWards);
document.querySelector('.close-modal').addEventListener('click', closeWardModal);
document.getElementById('ward-modal').addEventListener('click', (e) => {
    if (e.target.id === 'ward-modal') closeWardModal();
});

document.getElementById('reset-filters').addEventListener('click', resetFilters);
document.getElementById('crime-type').addEventListener('change', applyFilters);
document.getElementById('exclude-city-centre').addEventListener('change', applyFilters);
const viewHeatmapBtn = document.getElementById('view-heatmap');
const viewWardsBtn = document.getElementById('view-wards');
let currentMapMode = 'heatmap';

viewHeatmapBtn.addEventListener('click', () => setMapMode('heatmap'));
viewWardsBtn.addEventListener('click', () => setMapMode('wards'));

function setMapMode(mode) {
    if (currentMapMode === mode) return;
    currentMapMode = mode;

    const intensityControl = document.getElementById('intensity-slider').parentElement;

    if (mode === 'heatmap') {
        viewHeatmapBtn.classList.add('active');
        viewWardsBtn.classList.remove('active');
        if (geoJsonLayer) map.removeLayer(geoJsonLayer);
        intensityControl.style.display = 'block';
        if (window.infoControlAdded) {
            info.remove();
            window.infoControlAdded = false;
        }
    } else {
        viewWardsBtn.classList.add('active');
        viewHeatmapBtn.classList.remove('active');
        if (heatLayer) map.removeLayer(heatLayer);
        intensityControl.style.display = 'none';
        if (!window.infoControlAdded) {
            info.addTo(map);
            window.infoControlAdded = true;
        }
    }

    applyFilters();
}


// Ward Details Logic
function showWardDetails(wardName) {
    const wardIdx = crimeData.w.indexOf(wardName);
    if (wardIdx === -1) return;

    // Filter points for this ward only, ignoring current map filters for accurate history
    const typeIndex = document.getElementById('crime-type').value === 'all'
        ? -1
        : crimeData.t.indexOf(document.getElementById('crime-type').value);

    // Get strictly this ward's data, optionally filtered by crime type
    // Ignore date filters to show full history trend
    const wardPoints = crimeData.p.filter(p => {
        const pType = p[2];
        const pWardIdx = p[8];
        return pWardIdx === wardIdx && (typeIndex === -1 || pType === typeIndex);
    });

    // Calculate Monthly Totals
    const monthlyCounts = {};
    wardPoints.forEach(p => {
        const [, , , year, month, count] = p;
        const key = `${year}-${String(month).padStart(2, '0')}`;
        monthlyCounts[key] = (monthlyCounts[key] || 0) + count;
    });

    // Sort by date YYYY-MM
    const sortedMonths = Object.keys(monthlyCounts).sort();

    // Calculate Stats
    // Last 3 Months Sum
    // Find the latest valid month in the dataset to anchor "now"
    const lastMonthKey = sortedMonths[sortedMonths.length - 1];
    if (!lastMonthKey) return; // No data

    const [lastYear, lastMonth] = lastMonthKey.split('-').map(Number);

    // Function to get previous N months count
    function getSumForPeriod(endYear, endMonth, monthsBack) {
        let sum = 0;
        let y = endYear;
        let m = endMonth;

        for (let i = 0; i < monthsBack; i++) {
            const key = `${y}-${String(m).padStart(2, '0')}`;
            sum += monthlyCounts[key] || 0;
            m--;
            if (m < 1) {
                m = 12;
                y--;
            }
        }
        return sum;
    }

    const last3Months = getSumForPeriod(lastYear, lastMonth, 3);

    // Previous 3 months (shift back 3 months)
    let prevY = lastYear;
    let prevM = lastMonth - 3;
    while (prevM < 1) { prevM += 12; prevY--; }

    const prev3Months = getSumForPeriod(prevY, prevM, 3);

    const trendDiff = last3Months - prev3Months;
    const trendPct = prev3Months > 0 ? ((trendDiff / prev3Months) * 100).toFixed(1) : 0;

    const last12Months = getSumForPeriod(lastYear, lastMonth, 12);

    // Render Modal Content
    document.getElementById('ward-details-title').textContent = wardName;

    const statsContainer = document.getElementById('ward-stats-container');

    const trendClass = trendDiff > 0 ? 'trend-negative' : 'trend-positive'; // More crimes = negative result
    const trendIcon = trendDiff > 0 ? '▲' : '▼';
    const trendColor = trendDiff > 0 ? 'trend-up' : 'trend-down';

    statsContainer.innerHTML = `
        <div class="stat-box">
            <h3>Yearly Total</h3>
            <div class="value">${last12Months.toLocaleString()}</div>
            <div class="sub-value">Last 12 Months</div>
        </div>
        <div class="stat-box ${trendClass}">
            <h3>3-Month Trend</h3>
            <div class="value">${last3Months.toLocaleString()}</div>
            <div class="sub-value ${trendColor}">
                ${trendIcon} ${Math.abs(trendPct)}% vs prev.
            </div>
        </div>
    `;

    // Render Sparkline (Last 24 months MAX)
    const sparkContainer = document.getElementById('ward-sparkline');
    sparkContainer.innerHTML = '';

    // Generate last 24 months keys
    const sparkKeys = [];
    let currY = lastYear;
    let currM = lastMonth;

    for (let i = 0; i < 24; i++) {
        sparkKeys.unshift(`${currY}-${String(currM).padStart(2, '0')}`);
        currM--;
        if (currM < 1) { currM = 12; currY--; }
    }

    const sparkCounts = sparkKeys.map(k => monthlyCounts[k] || 0);
    const maxSpark = Math.max(...sparkCounts, 1);

    sparkKeys.forEach((key, idx) => {
        const count = monthlyCounts[key] || 0;
        const barHeight = (count / maxSpark) * 100;

        const bar = document.createElement('div');
        bar.className = 'spark-bar';
        bar.style.height = `${barHeight}%`;
        bar.title = `${key}: ${count} crimes`;
        sparkContainer.appendChild(bar);
    });

    document.getElementById('ward-details-modal').classList.remove('hidden');
}

function closeWardDetails() {
    document.getElementById('ward-details-modal').classList.add('hidden');
}

document.querySelector('.close-modal-details').addEventListener('click', closeWardDetails);
document.getElementById('ward-details-modal').addEventListener('click', (e) => {
    if (e.target.id === 'ward-details-modal') closeWardDetails();
});

document.addEventListener('DOMContentLoaded', init);
