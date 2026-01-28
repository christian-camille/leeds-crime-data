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
import sys
import time
from datetime import datetime

from combine_leeds_data import combine_leeds_data
from fetch_data import fetch_crime_data
from process_api_data import process_api_data
from merge_datasets import merge_datasets
from enrich_data import enrich_data
from patch_enrichment import patch_enrichment
from download_archives import download_latest


PIPELINE_STEPS = [
    {
        "num": 0,
        "name": "Download Archive Data",
        "desc": "Downloads historical crime data archives from Police.uk",
        "func": download_latest,
        "args": ()
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
        "desc": "Fetches crime data from the UK Police API",
        "func": fetch_crime_data,
        "args": ("2022-11", "2025-12")
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
    
    if args.step and args.from_step:
        print("Error: Cannot use --step and --from together.")
        return 1
    
    success = run_pipeline(
        start_step=args.from_step or 1,
        end_step=args.to,
        single_step=args.step
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
