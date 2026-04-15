import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests


CHECKPOINT_DIRNAME = ".fetch_state"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_TIMEOUT_SECONDS = 10
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 2


def _parse_month_arg(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid month '{value}'. Use YYYY-MM format, for example 2022-11."
        ) from exc

    return parsed.strftime("%Y-%m")


def _get_month_paths(output_dir: str, date: str) -> dict[str, Path]:
    base_name = f"leeds_crime_{date.replace('-', '_')}"
    state_dir = Path(output_dir) / CHECKPOINT_DIRNAME
    state_dir.mkdir(parents=True, exist_ok=True)
    return {
        "output": Path(output_dir) / f"{base_name}.csv",
        "meta": state_dir / f"{base_name}.json",
        "partial": state_dir / f"{base_name}.jsonl",
    }


def _load_checkpoint(meta_path: Path) -> dict:
    if not meta_path.exists():
        return {"next_point_index": 0, "status_counts": {}, "failed_points": []}

    with meta_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    return {
        "next_point_index": int(data.get("next_point_index", 0)),
        "status_counts": {str(key): int(value) for key, value in data.get("status_counts", {}).items()},
        "status": data.get("status", "in_progress"),
        "failed_points": sorted({int(index) for index in data.get("failed_points", [])}),
    }


def _write_checkpoint(
    meta_path: Path,
    *,
    next_point_index: int,
    status_counts: dict[int | str, int],
    status: str,
    failed_points: set[int] | list[int] | tuple[int, ...],
) -> None:
    serializable_counts = {str(key): int(value) for key, value in status_counts.items()}
    payload = {
        "next_point_index": int(next_point_index),
        "status_counts": serializable_counts,
        "status": status,
        "failed_points": sorted({int(index) for index in failed_points}),
    }

    with meta_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _append_partial_results(partial_path: Path, crimes: list[dict]) -> None:
    if not crimes:
        return

    with partial_path.open("a", encoding="utf-8") as handle:
        for crime in crimes:
            handle.write(json.dumps(crime))
            handle.write("\n")


def _load_partial_results(partial_path: Path) -> list[dict]:
    if not partial_path.exists():
        return []

    with partial_path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _clear_month_state(*paths: Path) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def _load_existing_month_records(output_file: Path) -> list[dict]:
    if not output_file.exists():
        return []

    df = pd.read_csv(output_file, low_memory=False)
    return df.to_dict(orient="records")


def _merge_and_save_records(output_file: Path, base_records: list[dict], new_records: list[dict]) -> int:
    combined_records = [*base_records, *new_records]
    if not combined_records:
        return 0

    df = pd.DataFrame(combined_records)
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])
    elif "persistent_id" in df.columns:
        df = df.drop_duplicates(subset=["persistent_id"])
    else:
        df = df.drop_duplicates()

    df.to_csv(output_file, index=False)
    return len(df)


def _fetch_grid_point(date: str, lat: float, lon: float, *, timeout: int, max_retries: int, retry_delay: int) -> tuple[str, list[dict] | str | None, int | None]:
    base_url = "https://data.police.uk/api/crimes-street/all-crime"

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(base_url, params={"lat": lat, "lng": lon, "date": date}, timeout=timeout)

            if response.status_code == 200:
                return "ok", response.json(), 200

            if response.status_code == 404:
                return "unavailable", None, 404

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                wait_seconds = 5 if response.status_code == 429 else retry_delay * (2 ** attempt)
                print(
                    f"Retryable HTTP {response.status_code} for {date} at {lat},{lon}. "
                    f"Retrying in {wait_seconds}s ({attempt + 1}/{max_retries})..."
                )
                time.sleep(wait_seconds)
                continue

            return "failed_status", None, response.status_code

        except requests.RequestException as exc:
            if attempt < max_retries:
                wait_seconds = retry_delay * (2 ** attempt)
                print(
                    f"Transient error for {date} at {lat},{lon}: {exc}. "
                    f"Retrying in {wait_seconds}s ({attempt + 1}/{max_retries})..."
                )
                time.sleep(wait_seconds)
                continue

            return "exception", str(exc), None

    return "exception", "Unexpected retry exhaustion", None


def _month_requires_repair(output_file: Path, checkpoint: dict, repair_existing: bool) -> bool:
    failed_points = checkpoint.get("failed_points", [])
    return (output_file.exists() and bool(failed_points)) or (output_file.exists() and repair_existing)


def _build_point_indexes(total_points: int, checkpoint: dict, output_exists: bool, repair_existing: bool) -> list[int]:
    failed_points = sorted({int(index) for index in checkpoint.get("failed_points", [])})
    next_point_index = int(checkpoint.get("next_point_index", 0))

    if output_exists and repair_existing and not failed_points:
        return list(range(total_points))

    remaining_points = list(range(next_point_index, total_points)) if not output_exists else []
    return remaining_points + failed_points

def fetch_crime_data(
    start_date,
    end_date,
    output_dir="data/raw",
    *,
    repair_existing: bool = False,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    retry_delay: int = DEFAULT_RETRY_DELAY_SECONDS,
):
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    MIN_LAT = 53.69
    MAX_LAT = 53.96
    MIN_LON = -1.80
    MAX_LON = -1.29
    STEP = 0.02
    
    lats = np.arange(MIN_LAT, MAX_LAT, STEP)
    lons = np.arange(MIN_LON, MAX_LON, STEP)
    grid_points = [(lat, lon) for lat in lats for lon in lons]
    
    dates = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()
    
    print(f"Fetching data for {len(dates)} months using grid ({len(grid_points)} points)...")
    
    for date in dates:
        paths = _get_month_paths(output_dir, date)
        output_file = paths["output"]
        checkpoint = _load_checkpoint(paths["meta"])
        start_index = checkpoint.get("next_point_index", 0)
        status_counts = {int(key): value for key, value in checkpoint.get("status_counts", {}).items()}
        known_status = checkpoint.get("status")
        failed_points = set(int(index) for index in checkpoint.get("failed_points", []))
        repair_needed = _month_requires_repair(output_file, checkpoint, repair_existing)
        point_indexes = _build_point_indexes(len(grid_points), checkpoint, output_file.exists(), repair_existing)
        
        if output_file.exists() and not repair_needed:
            print(f"Skipping {date}, already exists.")
            continue

        if known_status == "unavailable":
            print(f"Skipping {date}, known unavailable from Police API.")
            continue
            
        if output_file.exists() and repair_needed:
            if failed_points:
                print(f"Repairing {date}: retrying {len(failed_points)} missing grid point(s)...")
            else:
                print(f"Repairing {date}: revisiting all grid points to recover any missing data...")
        else:
            print(f"Fetching data for {date}...")
        month_unavailable = False
        
        total_points = len(grid_points)

        if start_index > 0 and not output_file.exists() and not repair_existing:
            print(f"Resuming {date} from grid point {start_index + 1}/{total_points}...")

        for index in point_indexes:
            lat, lon = grid_points[index]
            count = index + 1
            if count % 50 == 0:
                print(f"  Processed {count}/{total_points} grid points...")

            result_type, payload, status_code = _fetch_grid_point(
                date,
                lat,
                lon,
                timeout=timeout,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )

            if status_code is not None:
                status_counts[status_code] = status_counts.get(status_code, 0) + 1

            if result_type == "ok":
                failed_points.discard(index)
                _append_partial_results(paths["partial"], payload if isinstance(payload, list) else [])
            elif result_type == "unavailable":
                print(f"Date {date} is not available from Police API (HTTP 404). Skipping this month.")
                month_unavailable = True
                _write_checkpoint(
                    paths["meta"],
                    next_point_index=index,
                    status_counts=status_counts,
                    status="unavailable",
                    failed_points=failed_points,
                )
                _clear_month_state(paths["partial"])
                break
            elif result_type == "failed_status":
                print(f"HTTP {status_code} for {date} at {lat},{lon} after retries. Marking grid point for repair.")
                failed_points.add(index)
            else:
                print(f"Exception for {date} at {lat},{lon}: {payload}")
                failed_points.add(index)

            _write_checkpoint(
                paths["meta"],
                next_point_index=max(start_index, index + 1),
                status_counts=status_counts,
                status="in_progress",
                failed_points=failed_points,
            )

            time.sleep(0.1)

        if month_unavailable:
            continue

        non_success_status = {code: count for code, count in status_counts.items() if code != 200}
        if non_success_status:
            status_summary = ", ".join(f"{code}:{count}" for code, count in sorted(non_success_status.items()))
            print(f"HTTP status summary for {date}: {status_summary}")

        base_records = _load_existing_month_records(output_file)
        all_crimes = _load_partial_results(paths["partial"])
        
        if base_records or all_crimes:
            initial_len = len(base_records) + len(all_crimes)
            final_len = _merge_and_save_records(output_file, base_records, all_crimes)
            print(f"Fetched {initial_len} records. Deduplicated to {final_len} records.")

            if failed_points:
                _write_checkpoint(
                    paths["meta"],
                    next_point_index=total_points,
                    status_counts=status_counts,
                    status="complete_with_gaps",
                    failed_points=failed_points,
                )
                _clear_month_state(paths["partial"])
                print(f"Saved to {output_file} with {len(failed_points)} grid point(s) still pending repair.")
            else:
                _clear_month_state(paths["meta"], paths["partial"])
                print(f"Saved to {output_file}")
        else:
            if failed_points:
                _write_checkpoint(
                    paths["meta"],
                    next_point_index=total_points,
                    status_counts=status_counts,
                    status="complete_with_gaps",
                    failed_points=failed_points,
                )
                _clear_month_state(paths["partial"])
                print(f"No records found for {date}; {len(failed_points)} grid point(s) remain pending repair.")
            else:
                _clear_month_state(paths["meta"], paths["partial"])
                print(f"No records found for {date}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Leeds crime data from the UK Police API")
    parser.add_argument("--start", type=_parse_month_arg, default="2022-11", help="Start month in YYYY-MM format")
    parser.add_argument("--end", type=_parse_month_arg, default="2025-12", help="End month in YYYY-MM format")
    parser.add_argument("--output-dir", default="data/raw", help="Directory for raw monthly CSV output")
    parser.add_argument(
        "--repair-existing",
        action="store_true",
        help="Revisit existing monthly CSVs and merge in any records found for missing or previously failed grid points",
    )
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retries per grid request for transient failures")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="Request timeout in seconds")
    parser.add_argument("--retry-delay", type=int, default=DEFAULT_RETRY_DELAY_SECONDS, help="Base delay in seconds for retry backoff")
    args = parser.parse_args()

    if args.start > args.end:
        parser.error(f"--start must be earlier than or equal to --end. Got {args.start} > {args.end}.")

    return args

if __name__ == "__main__":
    args = parse_args()
    fetch_crime_data(
        args.start,
        args.end,
        output_dir=args.output_dir,
        repair_existing=args.repair_existing,
        max_retries=args.max_retries,
        timeout=args.timeout,
        retry_delay=args.retry_delay,
    )
