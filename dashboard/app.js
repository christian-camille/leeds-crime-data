let map;
let heatLayer;
let crimeData = null;

const MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
];

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
        const response = await fetch('data/crime_data.json');
        crimeData = await response.json();

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

    const yearStartSelect = document.getElementById('year-start');
    const yearEndSelect = document.getElementById('year-end');

    crimeData.y.forEach(year => {
        const optionStart = document.createElement('option');
        optionStart.value = year;
        optionStart.textContent = year;
        yearStartSelect.appendChild(optionStart);

        const optionEnd = document.createElement('option');
        optionEnd.value = year;
        optionEnd.textContent = year;
        yearEndSelect.appendChild(optionEnd);
    });

    yearEndSelect.value = crimeData.y[crimeData.y.length - 1];
    document.getElementById('month-end').value = '12';
}

function applyFilters() {
    const crimeType = document.getElementById('crime-type').value;
    const yearStart = parseInt(document.getElementById('year-start').value);
    const yearEnd = parseInt(document.getElementById('year-end').value);
    const monthStart = parseInt(document.getElementById('month-start').value);
    const monthEnd = parseInt(document.getElementById('month-end').value);

    const typeIndex = crimeType === 'all' ? -1 : crimeData.t.indexOf(crimeType);

    const filteredPoints = crimeData.p.filter(point => {
        const [lat, lon, pType, pYear, pMonth, count] = point;

        if (typeIndex !== -1 && pType !== typeIndex) {
            return false;
        }

        if (pYear < yearStart || pYear > yearEnd) {
            return false;
        }

        if (pYear === yearStart && pMonth < monthStart) {
            return false;
        }
        if (pYear === yearEnd && pMonth > monthEnd) {
            return false;
        }

        return true;
    });

    const aggregated = {};

    filteredPoints.forEach(point => {
        const [lat, lon, pType, pYear, pMonth, count] = point;
        const key = `${lat},${lon}`;
        if (!aggregated[key]) {
            aggregated[key] = { lat, lon, count: 0 };
        }
        aggregated[key].count += count;
    });

    const heatPoints = Object.values(aggregated).map(p => [p.lat, p.lon, p.count]);

    if (heatLayer) {
        map.removeLayer(heatLayer);
    }

    const maxIntensity = Math.max(...heatPoints.map(p => p[2]), 1);

    heatLayer = L.heatLayer(heatPoints, {
        radius: 18,
        blur: 25,
        maxZoom: 15,
        max: maxIntensity * 0.25,
        gradient: {
            0.0: '#0d0887',
            0.2: '#5302a3',
            0.4: '#8b0aa5',
            0.6: '#db5c68',
            0.8: '#febd2a',
            1.0: '#f0f921'
        }
    }).addTo(map);

    updateStats(filteredPoints, yearStart, monthStart, yearEnd, monthEnd);
}

function updateStats(points, yearStart, monthStart, yearEnd, monthEnd) {
    const totalCrimes = points.reduce((sum, p) => sum + p[5], 0);
    document.getElementById('total-crimes').textContent = totalCrimes.toLocaleString();

    const startMonthName = MONTHS[monthStart - 1].substring(0, 3);
    const endMonthName = MONTHS[monthEnd - 1].substring(0, 3);
    document.getElementById('date-range').textContent =
        `${startMonthName} ${yearStart} - ${endMonthName} ${yearEnd}`;
}

function resetFilters() {
    document.getElementById('crime-type').value = 'all';
    document.getElementById('year-start').value = crimeData.y[0];
    document.getElementById('year-end').value = crimeData.y[crimeData.y.length - 1];
    document.getElementById('month-start').value = '01';
    document.getElementById('month-end').value = '12';
    applyFilters();
}

document.getElementById('apply-filters').addEventListener('click', applyFilters);
document.getElementById('reset-filters').addEventListener('click', resetFilters);

document.addEventListener('DOMContentLoaded', init);
