# Leeds Crime Data Pipeline

A complete ETL (Extract, Transform, Load) pipeline for collecting, processing, and enriching street-level crime data for the Leeds metropolitan area. The pipeline aggregates data from the UK Police API and historical archives, producing a comprehensive dataset spanning **January 2018 to October 2025** with over **900,000 crime records**.

<p align="center">
  <img src="https://files.catbox.moe/7a1kyy.png" width="900" alt="Leeds Crime Heatmap 2018-2025">
  <br>
  <b>Figure 1:</b> <i>Geospatial density of 900,000+ crime records across the Leeds metropolitan area.</i>
</p>

## Project Highlights

- **Data Engineering**: Automated month-by-month API fetching with rate limiting, error handling, and incremental processing.
- **Geospatial Processing**: Point-in-polygon validation using `Shapely` to ensure all records fall within Leeds administrative boundaries.
- **Data Enrichment**: Batch geocoding via the `postcodes.io` API to append Ward Names and Postcode Districts.
- **Dataset Normalisation**: Unified format across API and archive sources with consistent crime categorisation.

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
| Outcome | Case outcome where available |

## Tech Stack

- **Python 3.12** - Core language
- **Pandas** - Data manipulation and analysis
- **Shapely** - Geometric operations and spatial filtering
- **Requests** - API interactions
- **tqdm** - Progress visualisation
- **NumPy** - Numerical operations
- **pytest** - Testing framework

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

**4. Merge & Enrich** Consolidates all sources and appends Ward/Postcode metadata.

```bash
python src/merge_datasets.py
python src/enrich_data.py

```

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
│   └── patch_enrichment.py     # Enrichment gap-filling
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

## Output

The final dataset is exported to `data/processed/leeds_street_combined.csv`. It contains **~906,000 records** with **100% coverage** for Ward Names and Postcode Districts, making it ready for immediate exploratory data analysis (EDA).

## License

This project is for educational and portfolio purposes. Crime data is sourced from the UK Police API under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

