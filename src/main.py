"""
Leeds Crime Data Pipeline - Main Orchestrator

Runs the complete ETL pipeline for collecting, processing, and enriching
street-level crime data for the Leeds metropolitan area.

Usage:
    python src/main.py              # Run full pipeline
    python src/main.py --step 1     # Run specific step only
    python src/main.py --from 3     # Start from step 3
    python src/main.py --list       # List all steps
"""

import argparse
import os
import sys
import time
from datetime import datetime

from combine_leeds_data import combine_leeds_data, get_available_archive_months
from fetch_data import fetch_crime_data
from process_api_data import process_api_data
from merge_datasets import merge_datasets
from enrich_data import enrich_data
from patch_enrichment import patch_enrichment
from download_archives import download_covering_since
from fetch_wards import fetch_wards
from prepare_dashboard_data import prepare_dashboard_data


def add_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def previous_month(today: datetime) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def get_api_fetch_range(base_dir: str = "data/archive") -> tuple[str, str] | None:
    """Return incremental API fetch range after latest archive month, or None if up to date."""
    archive_months = get_available_archive_months(base_dir)

    end_year, end_month = previous_month(datetime.now())
    end_label = f"{end_year:04d}-{end_month:02d}"

    if not archive_months:
        return "2022-11", end_label

    latest_archive = archive_months[-1]
    latest_year, latest_month = map(int, latest_archive.split("-"))
    start_year, start_month = add_month(latest_year, latest_month)

    if (start_year, start_month) > (end_year, end_month):
        return None

    start_label = f"{start_year:04d}-{start_month:02d}"
    return start_label, end_label


def fetch_api_incremental_data() -> None:
    """Fetch API data only for months not already covered by archive snapshots."""
    api_range = get_api_fetch_range()
    if api_range is None:
        print("Archive data already covers all months up to last complete month. Skipping API fetch.")
        return

    start_date, end_date = api_range
    print(f"Incremental API fetch range: {start_date} to {end_date}")
    fetch_crime_data(start_date, end_date)


def print_preflight_checks(steps_to_run: list[dict]) -> None:
    """Print quick sanity checks before running selected pipeline steps."""
    step_numbers = {step["num"] for step in steps_to_run}

    print()
    print("Preflight checks")
    print("-" * 60)

    archive_dir = "data/archive"
    if 0 in step_numbers or 1 in step_numbers or 2 in step_numbers:
        if not os.path.exists(archive_dir):
            print(f"[!] Archive directory missing: {archive_dir}")
        else:
            seed_months = get_available_archive_months(archive_dir, include_zip=True)
            extracted_months = get_available_archive_months(archive_dir, include_zip=False)

            if seed_months:
                print(
                    f"[✓] Archive seeds: {len(seed_months)} month label(s), "
                    f"range {seed_months[0]} to {seed_months[-1]}"
                )
            else:
                print("[!] No archive month folders or ZIPs found in data/archive")

            if extracted_months:
                print(
                    f"[✓] Extracted month folders: {len(extracted_months)}, "
                    f"range {extracted_months[0]} to {extracted_months[-1]}"
                )
            else:
                print("[i] No extracted month folders yet (Step 1 will extract from ZIPs if available)")

    if 2 in step_numbers:
        api_range = get_api_fetch_range()
        if api_range is None:
            print("[i] API fetch: skipped (archive coverage already up to last complete month)")
        else:
            print(f"[✓] API fetch range: {api_range[0]} to {api_range[1]}")

    print("-" * 60)


PIPELINE_STEPS = [
    {
        "num": 0,
        "name": "Download Archive Data",
        "desc": "Downloads historical archive snapshots covering data from 2018-01 onward",
        "func": download_covering_since,
        "args": (2018, 1)
    },
    {
        "num": 1,
        "name": "Generate Archive Data",
        "desc": "Aggregates historical data from local archive files",
        "func": combine_leeds_data,
        "args": ()
    },
    {
        "num": 2,
        "name": "Fetch API Data",
        "desc": "Fetches incremental crime data from the UK Police API",
        "func": fetch_api_incremental_data,
        "args": ()
    },
    {
        "num": 3,
        "name": "Process API Data",
        "desc": "Normalizes API data, filters by Leeds boundary, assigns LSOA codes",
        "func": process_api_data,
        "args": ()
    },
    {
        "num": 4,
        "name": "Merge Datasets",
        "desc": "Combines archive and API data, removes duplicates",
        "func": merge_datasets,
        "args": ()
    },
    {
        "num": 5,
        "name": "Enrich Data",
        "desc": "Adds Ward Names, Postcode Districts, and Polling Districts via geocoding",
        "func": enrich_data,
        "args": ()
    },
    {
        "num": 6,
        "name": "Patch Enrichment",
        "desc": "Fills in missing Ward/Postcode data with wider search radius",
        "func": patch_enrichment,
        "args": ()
    },
    {
        "num": 7,
        "name": "Fetch Ward Boundaries",
        "desc": "Fetches and processes official ward boundaries from MapServer",
        "func": fetch_wards,
        "args": ()
    },
    {
        "num": 8,
        "name": "Prepare Dashboard Data",
        "desc": "Aggregates enriched data into optimized JSON for the dashboard and postcode search",
        "func": prepare_dashboard_data,
        "args": ()
    }
]


def print_banner():
    print("=" * 60)
    print("  Leeds Crime Data Pipeline")
    print("=" * 60)
    print()


def print_step_list():
    print_banner()
    print("Pipeline Steps:")
    print("-" * 60)
    for step in PIPELINE_STEPS:
        print(f"  {step['num']}. {step['name']}")
        print(f"     {step['desc']}")
    print()


def run_step(step):
    print()
    print("=" * 60)
    print(f"  Step {step['num']}: {step['name']}")
    print("=" * 60)
    print(f"  {step['desc']}")
    print("-" * 60)
    
    start_time = time.time()
    
    try:
        step["func"](*step["args"])
        elapsed = time.time() - start_time
        print()
        print(f"[✓] Step {step['num']} completed in {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - start_time
        print()
        print(f"[✗] Step {step['num']} failed after {elapsed:.1f}s")
        print(f"    Error: {e}")
        return False


def run_pipeline(start_step=0, end_step=None, single_step=None):
    print_banner()
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    steps_to_run = []
    
    if single_step is not None:
        steps_to_run = [s for s in PIPELINE_STEPS if s["num"] == single_step]
        if not steps_to_run:
            print(f"Error: Step {single_step} not found.")
            return False
    else:
        end = end_step if end_step is not None else max(s["num"] for s in PIPELINE_STEPS)
        steps_to_run = [s for s in PIPELINE_STEPS if start_step <= s["num"] <= end]
    
    print(f"Running {len(steps_to_run)} step(s): {', '.join(str(s['num']) for s in steps_to_run)}")
    print_preflight_checks(steps_to_run)
    
    pipeline_start = time.time()
    failed_step = None
    
    for step in steps_to_run:
        success = run_step(step)
        if not success:
            failed_step = step["num"]
            break
    
    print()
    print("=" * 60)
    total_time = time.time() - pipeline_start
    
    if failed_step:
        print(f"  Pipeline FAILED at step {failed_step}")
        print(f"  Total time: {total_time:.1f}s")
        print("=" * 60)
        return False
    else:
        print(f"  Pipeline COMPLETE")
        print(f"  Total time: {total_time:.1f}s")
        print("=" * 60)
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Leeds Crime Data Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/main.py              Run full pipeline
  python src/main.py --step 3     Run only step 3
  python src/main.py --from 4     Start from step 4
  python src/main.py --list       Show all steps
        """
    )
    
    parser.add_argument("--list", action="store_true", 
                        help="List all pipeline steps")
    parser.add_argument("--step", type=int, metavar="N",
                        help="Run only step N")
    parser.add_argument("--from", dest="from_step", type=int, metavar="N",
                        help="Start from step N")
    parser.add_argument("--to", type=int, metavar="N",
                        help="End at step N (use with --from)")
    
    args = parser.parse_args()
    
    if args.list:
        print_step_list()
        return 0
    
    if args.step is not None and args.from_step is not None:
        print("Error: Cannot use --step and --from together.")
        return 1
    
    success = run_pipeline(
        start_step=args.from_step if args.from_step is not None else 0,
        end_step=args.to,
        single_step=args.step
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
