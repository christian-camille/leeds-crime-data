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

        crimeData.p.forEach(p => {
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
    const endYear = crimeData.y[crimeData.y.length - 1];
    totalMonths = (endYear - startYear + 1) * 12;
    minDateTimestamp = new Date(startYear, 0).getTime();

    noUiSlider.create(slider, {
        start: [0, totalMonths - 1],
        connect: true,
        step: 1,
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
        tooltips: {
            to: function (value) {
                return formatMonthYear(value);
            }
        }
    });

    slider.noUiSlider.on('update', function () {
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

    const wardCounts = {};
    let maxCount = 0;

    points.forEach(point => {
        const [, , , , , count, , , wardIdx] = point;

        if (wardIdx !== undefined) {
            const wardName = crimeData.w[wardIdx];
            wardCounts[wardName] = (wardCounts[wardName] || 0) + count;
        }
    });

    Object.values(wardCounts).forEach(c => {
        if (c > maxCount) maxCount = c;
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
        const count = wardCounts[feature.properties.WARD_NAME] || 0;
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
            weight: 5,
            color: '#666',
            dashArray: '',
            fillOpacity: 0.7
        });
        layer.bringToFront();

        info.update(layer.feature.properties, wardCounts[layer.feature.properties.WARD_NAME] || 0);
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
                L.DomEvent.stopPropagation(e); // Prevent map/layer click events from triggering selection boxes
            }
        });
        const count = wardCounts[feature.properties.WARD_NAME] || 0;
        layer.bindTooltip(`<strong>${feature.properties.WARD_NAME}</strong><br>${count.toLocaleString()} crimes`);
    }

    geoJsonLayer = L.geoJson(wardGeoJsonData, {
        style: style,
        onEachFeature: onEachFeature
    }).addTo(map);

    if (!window.infoControlAdded) {
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

info.update = function (props, count) {
    this._div.innerHTML = '<h4>Ward Crime Stats</h4>' + (props ?
        '<b>' + props.WARD_NAME + '</b><br />' + count + ' crimes'
        : 'Hover over a ward');
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
    } else {
        viewWardsBtn.classList.add('active');
        viewHeatmapBtn.classList.remove('active');
        if (heatLayer) map.removeLayer(heatLayer);
        intensityControl.style.display = 'none';
    }

    applyFilters();
}

document.addEventListener('DOMContentLoaded', init);
