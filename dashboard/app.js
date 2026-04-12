let map;
let heatLayer;
let crimeData = null;
let searchData = null;
let searchCircle = null;
let searchMarker = null;
let activeSearchState = null;

const SEARCH_RADIUS_METERS = 100;
const SEARCH_RADIUS_MIN = 100;
const SEARCH_RADIUS_MAX = 1000;

function getSearchRadius() {
    const slider = document.getElementById('search-radius');
    return slider ? Number(slider.value) : SEARCH_RADIUS_METERS;
}

function formatRadius(meters) {
    return meters >= 1000 ? `${meters / 1000}km` : `${meters}m`;
}

function updateRadiusUI() {
    const radius = getSearchRadius();
    document.getElementById('radius-value').textContent = formatRadius(radius);
    document.getElementById('search-help-text').textContent =
        `Aggregated crimes within a ${formatRadius(radius)} radius of the postcode.`;
    document.getElementById('postcode-radius-label').textContent =
        `Selected filters, ${formatRadius(radius)} radius`;
}

function onRadiusChange() {
    updateRadiusUI();
    if (activeSearchState) {
        activeSearchState.candidates = buildSearchCandidates(activeSearchState.lat, activeSearchState.lon);
        renderSearchOverlay(activeSearchState.lat, activeSearchState.lon);
        updateActiveSearchResults();
        setSearchStatus(`Showing local crimes within ${formatRadius(getSearchRadius())} of ${activeSearchState.postcode}.`, 'success');
    }
}

const LEEDS_BOUNDS = {
    latMin: 53.69,
    latMax: 53.96,
    lonMin: -1.80,
    lonMax: -1.29
};

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
let currentFilteredResults = null;

async function fetchJsonFromCandidates(paths) {
    let lastError = null;

    for (const path of paths) {
        try {
            const response = await fetch(`${path}?v=${new Date().getTime()}`);
            if (!response.ok) {
                lastError = new Error(`HTTP ${response.status} for ${path}`);
                continue;
            }
            return await response.json();
        } catch (error) {
            lastError = error;
        }
    }

    throw lastError || new Error('Could not load crime_data.json from any known path.');
}

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
        if (window.location.protocol === 'file:') {
            throw new Error('This dashboard cannot load JSON over file://. Run a local web server and open http://localhost:8000.');
        }

        crimeData = await fetchJsonFromCandidates([
            'data/crime_data.json',
            './data/crime_data.json',
            '/dashboard/data/crime_data.json'
        ]);

        try {
            searchData = await fetchJsonFromCandidates([
                'data/postcode_search.json',
                './data/postcode_search.json',
                '/dashboard/data/postcode_search.json'
            ]);
        } catch (searchError) {
            console.warn('Failed to load postcode search data:', searchError);
            setSearchControlsEnabled(false);
            setSearchStatus('Postcode search is unavailable until postcode_search.json is generated.', 'warning');
        }

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
            <p style="color: var(--danger);">Failed to load data: ${error.message}</p>
            <p style="margin-top: 8px; color: #b3b3b3;">Tip: run <code>python -m http.server 8000</code> inside the <code>dashboard</code> folder.</p>
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

function setSearchControlsEnabled(enabled) {
    document.getElementById('postcode-search').disabled = !enabled;
    document.getElementById('postcode-search-btn').disabled = !enabled;
    document.getElementById('clear-postcode-search').disabled = !enabled;
}

function setSearchStatus(message, type = 'info') {
    const status = document.getElementById('postcode-search-status');
    if (!message) {
        status.textContent = '';
        status.className = 'search-status hidden';
        return;
    }

    status.textContent = message;
    status.className = `search-status ${type}`;
}

function normalizePostcode(value) {
    return value.trim().toUpperCase().replace(/\s+/g, ' ');
}

function isWithinLeedsBounds(lat, lon) {
    return lat >= LEEDS_BOUNDS.latMin && lat <= LEEDS_BOUNDS.latMax && lon >= LEEDS_BOUNDS.lonMin && lon <= LEEDS_BOUNDS.lonMax;
}

function haversineDistanceMeters(lat1, lon1, lat2, lon2) {
    const toRadians = (degrees) => (degrees * Math.PI) / 180;
    const earthRadius = 6371000;
    const dLat = toRadians(lat2 - lat1);
    const dLon = toRadians(lon2 - lon1);
    const a =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2);

    return 2 * earthRadius * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function buildSearchCandidates(lat, lon) {
    if (!searchData) {
        return [];
    }

    const radius = getSearchRadius();
    const latDelta = radius / 111320;
    const lonDelta = radius / (111320 * Math.cos((lat * Math.PI) / 180));

    return searchData.p.filter((point) => {
        const [pointLat, pointLon] = point;
        if (Math.abs(pointLat - lat) > latDelta || Math.abs(pointLon - lon) > lonDelta) {
            return false;
        }

        return haversineDistanceMeters(lat, lon, pointLat, pointLon) <= radius;
    });
}

function filterSearchCandidates(candidates, params) {
    if (!searchData) {
        return [];
    }

    const typeIndex = params.crimeType === 'all' ? -1 : searchData.t.indexOf(params.crimeType);

    return candidates.filter((point) => {
        const [, , pointType, pointYear, pointMonth] = point;

        if (typeIndex !== -1 && pointType !== typeIndex) {
            return false;
        }

        if (pointYear < params.yearStart || pointYear > params.yearEnd) {
            return false;
        }

        if (pointYear === params.yearStart && pointMonth < params.monthStart) {
            return false;
        }

        if (pointYear === params.yearEnd && pointMonth > params.monthEnd) {
            return false;
        }

        return true;
    });
}

function renderSearchResults(filteredCandidates) {
    if (!activeSearchState) {
        return;
    }

    const resultsPanel = document.getElementById('postcode-results');
    const breakdownContainer = document.getElementById('postcode-breakdown');
    const totalCrimes = filteredCandidates.reduce((sum, point) => sum + point[5], 0);
    const countsByType = {};

    filteredCandidates.forEach((point) => {
        const typeName = searchData.t[point[2]];
        countsByType[typeName] = (countsByType[typeName] || 0) + point[5];
    });

    const sortedTypes = Object.entries(countsByType).sort((a, b) => b[1] - a[1]);
    const topCrime = sortedTypes[0];

    document.getElementById('postcode-results-title').textContent = activeSearchState.postcode;
    document.getElementById('postcode-results-context').textContent = `${activeSearchState.ward} • ${activeSearchState.district}`;
    document.getElementById('postcode-total-crimes').textContent = totalCrimes.toLocaleString();
    document.getElementById('postcode-top-crime').textContent = topCrime ? topCrime[0] : 'No crimes';
    document.getElementById('postcode-top-crime-share').textContent = topCrime
        ? `${((topCrime[1] / Math.max(totalCrimes, 1)) * 100).toFixed(1)}% of local total`
        : 'No matching crimes for current filters';

    breakdownContainer.innerHTML = '';

    if (!sortedTypes.length) {
        const emptyState = document.createElement('p');
        emptyState.className = 'search-empty';
        emptyState.textContent = `No crimes matched the current filters inside this ${formatRadius(getSearchRadius())} radius.`;
        breakdownContainer.appendChild(emptyState);
    } else {
        const maxCount = sortedTypes[0][1];
        sortedTypes.forEach(([typeName, count]) => {
            const row = document.createElement('div');
            row.className = 'search-breakdown-row';
            row.innerHTML = `
                <span class="search-breakdown-label" title="${typeName}">${typeName}</span>
                <div class="search-breakdown-bar"><div class="search-breakdown-fill" style="width: ${(count / maxCount) * 100}%"></div></div>
                <span class="search-breakdown-value">${count.toLocaleString()}</span>
            `;
            breakdownContainer.appendChild(row);
        });
    }

    resultsPanel.classList.remove('hidden');
}

function renderSearchOverlay(lat, lon) {
    if (currentMapMode !== 'search') {
        return;
    }

    if (searchMarker) {
        map.removeLayer(searchMarker);
    }

    if (searchCircle) {
        map.removeLayer(searchCircle);
    }

    searchMarker = L.circleMarker([lat, lon], {
        radius: 7,
        color: '#f0f0f5',
        weight: 2,
        fillColor: '#22c55e',
        fillOpacity: 1
    }).addTo(map);

    searchCircle = L.circle([lat, lon], {
        radius: getSearchRadius(),
        color: '#22c55e',
        weight: 2,
        fillColor: '#22c55e',
        fillOpacity: 0.12
    }).addTo(map);

    const zoomLevel = getSearchRadius() <= 250 ? 16 : getSearchRadius() <= 500 ? 15 : 14;
    map.flyTo([lat, lon], Math.max(map.getZoom(), zoomLevel), { duration: 0.6 });
}

function updateActiveSearchResults(params = getSearchFilterParams()) {
    if (!activeSearchState) {
        return;
    }

    const filteredCandidates = filterSearchCandidates(activeSearchState.candidates, params);

    if (currentMapMode === 'search') {
        renderSearchResults(filteredCandidates);
    }
}

function clearPostcodeSearch() {
    activeSearchState = null;
    document.getElementById('postcode-search').value = '';
    document.getElementById('postcode-results').classList.add('hidden');
    setSearchStatus('', 'info');

    if (searchMarker) {
        map.removeLayer(searchMarker);
        searchMarker = null;
    }

    if (searchCircle) {
        map.removeLayer(searchCircle);
        searchCircle = null;
    }
}

async function runPostcodeSearch() {
    if (!searchData) {
        setSearchStatus('Postcode search data is not available yet.', 'warning');
        return;
    }

    const button = document.getElementById('postcode-search-btn');
    const input = document.getElementById('postcode-search');
    const rawPostcode = normalizePostcode(input.value);

    if (!rawPostcode) {
        setSearchStatus('Enter a Leeds postcode to search.', 'warning');
        return;
    }

    button.disabled = true;
    setSearchStatus('Looking up postcode...', 'info');

    try {
        const response = await fetch(`https://api.postcodes.io/postcodes/${encodeURIComponent(rawPostcode)}`);
        const payload = await response.json();

        if (!response.ok || payload.status !== 200 || !payload.result) {
            throw new Error('Postcode not found.');
        }

        const { latitude, longitude, admin_ward: adminWard, admin_district: adminDistrict, postcode } = payload.result;

        if (!isWithinLeedsBounds(latitude, longitude) || !String(adminDistrict || '').includes('Leeds')) {
            throw new Error('That postcode is outside the Leeds search area.');
        }

        const candidates = buildSearchCandidates(latitude, longitude);
        activeSearchState = {
            postcode,
            ward: adminWard || 'Unknown ward',
            district: adminDistrict || 'Unknown district',
            lat: latitude,
            lon: longitude,
            candidates
        };

        renderSearchOverlay(latitude, longitude);
        updateActiveSearchResults();
        setSearchStatus(`Showing local crimes within ${formatRadius(getSearchRadius())} of ${postcode}.`, 'success');
    } catch (error) {
        activeSearchState = null;
        document.getElementById('postcode-results').classList.add('hidden');
        setSearchStatus(error.message || 'Postcode lookup failed.', 'error');
    } finally {
        button.disabled = false;
    }
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
        monthEnd: endDate.month
    };
}

function getSearchFilterParams() {
    return getFilterParams();
}

function filterPoints(params) {
    const typeIndex = params.crimeType === 'all' ? -1 : crimeData.t.indexOf(params.crimeType);

    return crimeData.p.filter(point => {
        const [, , pType, pYear, pMonth] = point;

        if (typeIndex !== -1 && pType !== typeIndex) {
            return false;
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

function aggregateCounts(points, keyBuilder) {
    return points.reduce((totals, point) => {
        const key = keyBuilder(point);
        if (key === null || key === undefined) {
            return totals;
        }

        totals[key] = (totals[key] || 0) + point[5];
        return totals;
    }, {});
}

function aggregateByLocation(points) {
    return points.reduce((totals, point) => {
        const [lat, lon, , , , count] = point;
        const key = `${lat},${lon}`;

        if (!totals[key]) {
            totals[key] = { lat, lon, count: 0 };
        }

        totals[key].count += count;
        return totals;
    }, {});
}

function aggregateByCrimeType(points) {
    return aggregateCounts(points, (point) => crimeData.t[point[2]]);
}

function aggregateByWard(points) {
    return aggregateCounts(points, (point) => {
        const wardIdx = point[8];
        return wardIdx !== undefined ? crimeData.w[wardIdx] : null;
    });
}

function aggregateByMonth(points) {
    return aggregateCounts(points, (point) => {
        const year = point[3];
        const month = String(point[4]).padStart(2, '0');
        return `${year}-${month}`;
    });
}

function aggregateByCityCentre(points) {
    return aggregateCounts(points, (point) => (point[6] ? 'cityCentre' : 'restOfLeeds'));
}

function aggregateByCrimeTypeMonth(points) {
    return points.reduce((totals, point) => {
        const crimeType = crimeData.t[point[2]];
        const monthKey = String(point[4]).padStart(2, '0');

        if (!totals[crimeType]) {
            totals[crimeType] = {};
        }

        totals[crimeType][monthKey] = (totals[crimeType][monthKey] || 0) + point[5];
        return totals;
    }, {});
}

function aggregateWardDetails(points) {
    return points.reduce((totals, point) => {
        const wardIdx = point[8];
        if (wardIdx === undefined) {
            return totals;
        }

        const wardName = crimeData.w[wardIdx];
        if (!totals[wardName]) {
            totals[wardName] = { total: 0, types: {} };
        }

        totals[wardName].total += point[5];
        totals[wardName].types[point[2]] = (totals[wardName].types[point[2]] || 0) + point[5];
        return totals;
    }, {});
}

function getIntensitySettings(locationTotals) {
    let minFilterPercent = 0;
    let sensitivityPercent = 100;

    if (intensitySlider) {
        [minFilterPercent, sensitivityPercent] = intensitySlider.get().map(Number);
    }

    const sortedCounts = Object.values(locationTotals)
        .map((location) => location.count)
        .sort((a, b) => a - b);
    const numPoints = sortedCounts.length;

    const minFilterIndex = Math.floor((minFilterPercent / 100) * numPoints);
    const minFilter = numPoints > 0 ? sortedCounts[Math.min(minFilterIndex, numPoints - 1)] : 0;

    const sensitivityIndex = Math.floor((sensitivityPercent / 100) * numPoints);
    const saturationPoint = numPoints > 0 ? sortedCounts[Math.min(sensitivityIndex, numPoints - 1)] : 1;

    return {
        minFilterPercent,
        sensitivityPercent,
        minFilter,
        saturationPoint
    };
}

function buildHeatPoints(locationTotals, minFilter) {
    return Object.values(locationTotals).reduce((points, location) => {
        if (location.count >= minFilter) {
            points.push([location.lat, location.lon, location.count]);
        }
        return points;
    }, []);
}

function buildFilteredResults(params) {
    const points = filterPoints(params);
    const locationTotals = aggregateByLocation(points);
    const intensity = getIntensitySettings(locationTotals);
    const heatPoints = buildHeatPoints(locationTotals, intensity.minFilter);

    return {
        params,
        points,
        totalCrimes: points.reduce((sum, point) => sum + point[5], 0),
        locationTotals,
        heatPoints,
        intensity,
        aggregations: {
            byCrimeType: aggregateByCrimeType(points),
            byWard: aggregateByWard(points),
            byMonth: aggregateByMonth(points),
            byCityCentre: aggregateByCityCentre(points),
            byCrimeTypeMonth: aggregateByCrimeTypeMonth(points),
            wardDetails: aggregateWardDetails(points)
        }
    };
}



function applyFilters() {
    const params = getFilterParams();
    const filteredResults = buildFilteredResults(params);
    currentFilteredResults = filteredResults;

    if (currentMapMode === 'heatmap') {
        if (geoJsonLayer) map.removeLayer(geoJsonLayer);

        if (heatLayer) {
            map.removeLayer(heatLayer);
        }

            heatLayer = L.heatLayer(filteredResults.heatPoints, {
            radius: 25,
            blur: 35,
            maxZoom: 15,
                max: filteredResults.intensity.saturationPoint > 0 ? filteredResults.intensity.saturationPoint : 1,
            gradient: {
                0.0: '#0d0887',
                0.2: '#5302a3',
                0.4: '#8b0aa5',
                0.6: '#db5c68',
                0.8: '#febd2a',
                1.0: '#f0f921'
            }
        }).addTo(map);
    } else if (currentMapMode === 'wards') {
        if (heatLayer) map.removeLayer(heatLayer);
        updateChoropleth(filteredResults);
    } else {
        if (heatLayer) map.removeLayer(heatLayer);
        if (geoJsonLayer) map.removeLayer(geoJsonLayer);
    }

    if (currentMapMode !== 'search') {
        updateStats(filteredResults);
        updateWardChart(filteredResults);
    }

    updateActiveSearchResults(getSearchFilterParams());
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

function updateChoropleth(filteredResults) {
    if (!wardGeoJsonData) return;

    const wardCounts = filteredResults.aggregations.wardDetails;
    let maxCount = 0;

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

function updateStats(filteredResults) {
    document.getElementById('total-crimes').textContent = filteredResults.totalCrimes.toLocaleString();

    const startMonthName = MONTHS[filteredResults.params.monthStart - 1].substring(0, 3);
    const endMonthName = MONTHS[filteredResults.params.monthEnd - 1].substring(0, 3);
    document.getElementById('date-range').textContent =
        `${startMonthName} ${filteredResults.params.yearStart} - ${endMonthName} ${filteredResults.params.yearEnd}`;
}

function updateWardChart(filteredResults) {
    const sortedWards = Object.entries(filteredResults.aggregations.byWard)
        .sort((a, b) => b[1] - a[1]);

    currentWardData = sortedWards;

    const totalVisibleCrimes = sortedWards.reduce((sum, item) => sum + item[1], 0);

    const top10Wards = sortedWards.slice(0, 10);
    const maxCount = sortedWards.length > 0 ? sortedWards[0][1] : 1;

    const chartContainer = document.getElementById('wards-chart');
    chartContainer.innerHTML = '';

    top10Wards.forEach(([ward, count]) => {
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
document.getElementById('postcode-search-btn').addEventListener('click', runPostcodeSearch);
document.getElementById('clear-postcode-search').addEventListener('click', clearPostcodeSearch);
document.getElementById('search-radius').addEventListener('input', onRadiusChange);
document.getElementById('postcode-search').addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
        event.preventDefault();
        runPostcodeSearch();
    }
});
const viewHeatmapBtn = document.getElementById('view-heatmap');
const viewWardsBtn = document.getElementById('view-wards');
const viewSearchBtn = document.getElementById('view-search');
const viewAnalyticsBtn = document.getElementById('view-analytics');
let currentMapMode = 'heatmap';

viewHeatmapBtn.addEventListener('click', () => setMapMode('heatmap'));
viewWardsBtn.addEventListener('click', () => setMapMode('wards'));
viewSearchBtn.addEventListener('click', () => setMapMode('search'));
viewAnalyticsBtn.addEventListener('click', () => setMapMode('analytics'));

function setMapMode(mode) {
    if (currentMapMode === mode) return;
    currentMapMode = mode;

    const dateRangeGroup = document.getElementById('date-range-group');
    const intensityGroup = document.getElementById('intensity-group');
    const searchGroup = document.getElementById('search-group');
    const statsPanel = document.getElementById('stats-panel');
    const chartPanel = document.getElementById('chart-panel');
    const mapElement = document.getElementById('map');
    const analyticsView = document.getElementById('analytics-view');

    viewHeatmapBtn.classList.toggle('active', mode === 'heatmap');
    viewWardsBtn.classList.toggle('active', mode === 'wards');
    viewSearchBtn.classList.toggle('active', mode === 'search');
    viewAnalyticsBtn.classList.toggle('active', mode === 'analytics');

    mapElement.classList.toggle('hidden', mode === 'analytics');
    analyticsView.classList.toggle('hidden', mode !== 'analytics');

    if (searchMarker) {
        map.removeLayer(searchMarker);
        searchMarker = null;
    }

    if (searchCircle) {
        map.removeLayer(searchCircle);
        searchCircle = null;
    }

    if (mode === 'heatmap') {
        if (geoJsonLayer) map.removeLayer(geoJsonLayer);
        dateRangeGroup.classList.remove('hidden');
        intensityGroup.classList.remove('hidden');
        searchGroup.classList.add('hidden');
        statsPanel.classList.remove('hidden');
        chartPanel.classList.remove('hidden');
        if (window.infoControlAdded) {
            info.remove();
            window.infoControlAdded = false;
        }
    } else if (mode === 'wards') {
        if (heatLayer) map.removeLayer(heatLayer);
        dateRangeGroup.classList.remove('hidden');
        intensityGroup.classList.add('hidden');
        searchGroup.classList.add('hidden');
        statsPanel.classList.remove('hidden');
        chartPanel.classList.remove('hidden');
        if (!window.infoControlAdded) {
            info.addTo(map);
            window.infoControlAdded = true;
        }
    } else if (mode === 'search') {
        dateRangeGroup.classList.remove('hidden');
        intensityGroup.classList.add('hidden');
        searchGroup.classList.remove('hidden');
        statsPanel.classList.add('hidden');
        chartPanel.classList.add('hidden');
        if (heatLayer) map.removeLayer(heatLayer);
        if (geoJsonLayer) map.removeLayer(geoJsonLayer);
        if (window.infoControlAdded) {
            info.remove();
            window.infoControlAdded = false;
        }
        if (activeSearchState) {
            renderSearchOverlay(activeSearchState.lat, activeSearchState.lon);
            updateActiveSearchResults(getSearchFilterParams());
        }
    } else {
        dateRangeGroup.classList.remove('hidden');
        intensityGroup.classList.add('hidden');
        searchGroup.classList.add('hidden');
        statsPanel.classList.remove('hidden');
        chartPanel.classList.remove('hidden');
        if (heatLayer) map.removeLayer(heatLayer);
        if (geoJsonLayer) map.removeLayer(geoJsonLayer);
        if (window.infoControlAdded) {
            info.remove();
            window.infoControlAdded = false;
        }
    }

    applyFilters();

    if (mode !== 'analytics') {
        requestAnimationFrame(() => map.invalidateSize());
    }
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
    const crimeTypeValue = document.getElementById('crime-type').value;
    const crimeTypeLabel = crimeTypeValue === 'all' ? 'All Crimes' : crimeTypeValue;
    document.getElementById('ward-details-title').textContent = wardName;
    document.getElementById('ward-details-subtitle').textContent = crimeTypeLabel;
    document.getElementById('ward-sparkline-title').textContent =
        `Monthly Trend — ${crimeTypeLabel} (Last 24 Months)`;

    // Ward rank from current filtered data
    const wardRankIdx = currentWardData.findIndex(([w]) => w === wardName);
    const wardRank = wardRankIdx >= 0 ? `#${wardRankIdx + 1}` : 'N/A';
    const totalWards = currentWardData.length;

    // Monthly average over the last 12 months
    const monthlyAvg = Math.round(last12Months / 12);

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
        <div class="stat-box">
            <h3>Monthly Avg</h3>
            <div class="value">${monthlyAvg.toLocaleString()}</div>
            <div class="sub-value">Per Month</div>
        </div>
        <div class="stat-box ${trendClass}">
            <h3>3-Month Trend</h3>
            <div class="value">${last3Months.toLocaleString()}</div>
            <div class="sub-value ${trendColor}">
                ${trendIcon} ${Math.abs(trendPct)}% vs prev.
            </div>
        </div>
        <div class="stat-box">
            <h3>Ward Rank</h3>
            <div class="value">${wardRank}</div>
            <div class="sub-value">of ${totalWards} wards</div>
        </div>
    `;

    // Render Sparkline (Last 24 months MAX)
    const sparkContainer = document.getElementById('ward-sparkline');
    sparkContainer.innerHTML = '';

    // Remove any previously added year row
    const existingYearRow = document.querySelector('.spark-year-row');
    if (existingYearRow) existingYearRow.remove();

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

    // Year labels below sparkline
    const yearRow = document.createElement('div');
    yearRow.className = 'spark-year-row';
    let prevLabelYear = null;
    sparkKeys.forEach((key, idx) => {
        const year = key.split('-')[0];
        if (year !== prevLabelYear) {
            const label = document.createElement('span');
            label.className = 'spark-year-label';
            label.style.left = `${(idx / sparkKeys.length) * 100}%`;
            label.textContent = year;
            yearRow.appendChild(label);
            prevLabelYear = year;
        }
    });
    sparkContainer.insertAdjacentElement('afterend', yearRow);

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
