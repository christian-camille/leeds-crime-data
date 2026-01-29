# Leeds Crime Intelligence Platform

A comprehensive geospatial intelligence platform for the Leeds metropolitan area. This project combines a robust ETL pipeline with an interactive web dashboard to visualise over **900,000 crime records** spanning **2018–2025**. It integrates data from the **UK Police API**, **Leeds City Council**, and **ONS**, providing hyper-local insights through heatmaps, ward-level choropleths, and temporal trend analysis.

<p align=`"center">
  <img src="https://user.fm/files/v2-77083e47adabc7343a4c878338cf8497/Screenshot%202026-01-28%20154135.png" width="900" alt="Leeds Crime Heatmap 2018-2025">
  <br>
  <b>Figure 1:</b> <i>Interactive dashboard showing crime density across Leeds (2018-2025).</i>
</p>

## Project Highlights

- **Interactive Dashboard**: A responsive web application featuring dual-mode visualisation (Heatmap & Ward Choropleth), dynamic filtering, and real-time statistics.
- **Robust ETL Pipeline**: Automated ingestion system that handles incremental updates, rate limiting, and historical data merging.
- **Geospatial Intelligence**: Precise point-in-polygon validation (`Shapely`) and batch geocoding (`postcodes.io`) to enrich every crime record with administrative boundaries.
- **Data Normalisation**: Unified schema across disparate sources (API vs Archive) to ensure consistent categorisation and analysis.
- **Optimised Performance**: Pre-aggregated data structures (`JSON`) to ensure sub-second rendering of nearly a million data points in the browser.

## Dataset Features

| Feature | Description |
|---------|-------------|
| Crime ID | Unique identifier for each crime record |
| Month | Date of the crime (YYYY-MM) |
| Location | Street-level location with coordinates |
| Crime Type | Normalised category (e.g., "Violence and sexual offences") |
| LSOA | Lower Super Output Area code and name |
| Ward Name | Electoral ward (e.g., "Little London & Woodhouse") |
| Postcode District | First part of postcode (e.g., "LS1") |
| Polling District | Voting district code (e.g., "LWE") |
| Outcome | Case outcome where available |

## Tech Stack

### Data Pipeline
- **Python 3.12** - Core ETL logic
- **Pandas & NumPy** - High-performance data manipulation
- **Shapely** - Geospatial operations and polygon validation
- **Requests** - Robust API integration
- **Pytest** - Automated testing suite





## Installation

### Prerequisites
- Python 3.12+
- pip
- Git

### Setup

```bash
# Clone the repository
git clone https://github.com/reed-sh/leeds-crimes.git
cd leeds-crimes

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

```

## Usage

The pipeline is orchestrated via a central script, but individual components can be run independently for debugging or partial updates.

### Quick Run (Full Pipeline)

This is the recommended way to execute the entire workflow from extraction to enrichment.

```bash
python src/main.py

```

### Manual Step-by-Step Execution

If you prefer to run the stages manually:

**0. Download Historical Data** Downloads archived crime data from Police.uk (required for historical analysis).

```bash
python src/download_archives.py --latest

```

**1. Generate Archive Data** Aggregates historical data from local archive files.

```bash
python src/combine_leeds_data.py

```

**2. Fetch API Data** Fetches the most recent "fresh" data from the UK Police API.

```bash
python src/fetch_data.py

```

**3. Process & Filter** Normalises API data and performs geospatial filtering.

```bash
python src/process_api_data.py

```

**4. Merge & Enrich** Consolidates all sources and appends Ward/Postcode/Polling District metadata.

```bash
python src/merge_datasets.py
python src/enrich_data.py

```

**5. Fetch Boundaries** Retrieves and processes official Leeds ward boundaries for the map.

```bash
python src/fetch_wards.py

```

**6. Prepare Dashboard** transforming the processed CSV into optimised JSON for the web interface.

```bash
python src/prepare_dashboard_data.py

```

## Interactive Dashboard

The dashboard is the centrepiece of this project, offering a high-performance interface for exploring 7+ years of crime data. Built with **Leaflet.js** and **noUiSlider**, it leverages optimised GeoJSON layers to deliver smooth transitions between granular heatmaps and administrative ward views, all within the browser.

### Features
* **Heatmap Visualisation**: Dynamic density map of crime hotspots.
* **Temporal Filtering**: Analyse trends over specific years and months.
* **Category Filtering**: Isolate specific crime types (e.g., "Burglary").
* **Choropleth Map**: Toggle between heatmap and ward-level density views.
* **Ward Breakdown**: Top 5 wards by crime count for the selected period.

### Running the Dashboard

To avoid CORS issues when loading the data, serve the dashboard using a local web server:

```bash
# Navigate to the dashboard directory
cd dashboard

# Start a local server (Python 3)
python -m http.server 8000
```

Then open your browser to `http://localhost:8000`.

## Project Structure

```
leeds-crimes/
├── data/
│   ├── archive/          # Historical crime data by month
│   ├── raw/              # Raw API responses
│   └── processed/        # Cleaned and enriched datasets
├── src/
│   ├── main.py                 # Pipeline orchestrator
│   ├── download_archives.py    # Archive data downloader
│   ├── fetch_data.py           # API data collection
│   ├── combine_leeds_data.py   # Archive data aggregation
│   ├── process_api_data.py     # API data normalisation
│   ├── merge_datasets.py       # Data consolidation
│   ├── filter_leeds_locations.py # Geospatial filtering
│   ├── assign_lsoa.py          # LSOA assignment
│   ├── enrich_data.py          # Ward/Postcode enrichment
│   ├── patch_enrichment.py     # Enrichment gap-filling
│   ├── fetch_wards.py          # Ward boundary collection
│   └── prepare_dashboard_data.py # Dashboard data generation
├── tests/
│   ├── test_data_sources.py    # API availability tests
│   ├── test_boundary.py        # Leeds polygon validation
│   ├── test_enrichment.py      # Data quality checks
│   └── test_location.py        # Location validation
├── requirements.txt
└── README.md

```

## Testing

The project includes a comprehensive test suite to ensure data integrity and API stability. Run these before executing the main pipeline:

```bash
# Run all tests
pytest tests/ -v

```

**Test Categories:**

* `test_data_sources`: Verifies external APIs are accessible and responding.
* `test_boundary`: Validates that the Leeds polygon geometry is correctly loaded.
* `test_enrichment`: Checks that data quality thresholds are met (e.g., no null Wards).
* `test_location`: Samples coordinates to ensure they reside within the target area.

## Data Sources

* **UK Police API** - [data.police.uk](https://data.police.uk/docs/)
* **ONS Geoportal** - LSOA boundary data
* **OpenStreetMap Nominatim** - Leeds administrative boundary
* **Postcodes.io** - Postcode and ward lookup
* **Leeds City Council MapServer** - Polling District boundaries

## Output

The pipeline produces two primary artifacts:
1. **`data/processed/leeds_street_combined.csv`**: The master dataset containing **~906,000 records** with 100% Ward/Postcode coverage, ideal for deep analysis (EDA) or ML modelling.
2. **`dashboard/data/crime_data.json`**: An optimised, minified structure containing pre-aggregated indices and spatial coordinates, powering the real-time web dashboard.

## License

This project is open-source and available under the **MIT License**.


- **Crime Data**: Sourced from the [UK Police API](https://data.police.uk/docs/) under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
- **Boundary Data**: Contains OS data © Crown copyright and database right 2024.

