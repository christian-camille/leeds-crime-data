"""
Police.uk Archive Data Downloader

Downloads historical crime data archives from https://data.police.uk/data/archive/
Features:
- Progress bar with download speed and ETA
- Resume capability for interrupted downloads
- MD5 checksum verification
- Flexible date range selection
"""

import argparse
import hashlib
import re
import sys
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

BASE_URL = "https://data.police.uk/data/archive"
ARCHIVE_DIR = Path(__file__).parent.parent / "data" / "archive"
CHUNK_SIZE = 8192
ARCHIVE_COVERAGE_MONTHS = 36


def get_archive_url(year: int, month: int) -> str:
    return f"{BASE_URL}/{year:04d}-{month:02d}.zip"


def get_available_archive_months() -> list[tuple[int, int]]:
    """Fetch available monthly archives from the Police.uk archive index."""
    try:
        response = requests.get(f"{BASE_URL}/", timeout=30)
        response.raise_for_status()
        matches = set(re.findall(r"(\d{4})-(\d{2})\.zip", response.text))
        months = [(int(year), int(month)) for year, month in matches if 1 <= int(month) <= 12]
        months.sort()
        return months
    except requests.RequestException as e:
        print(f"✗ Failed to fetch archive index: {e}")
        return []


def month_to_index(year: int, month: int) -> int:
    return year * 12 + (month - 1)


def index_to_month(index: int) -> tuple[int, int]:
    return index // 12, (index % 12) + 1


def plan_compact_archives(
    start_year: int,
    start_month: int,
    available_months: list[tuple[int, int]],
    coverage_months: int = ARCHIVE_COVERAGE_MONTHS,
) -> list[tuple[int, int]]:
    """
    Select the minimum set of archive files needed to cover from start month to latest,
    assuming each archive contains `coverage_months` months ending at that archive month.
    """
    if not available_months:
        return []

    sorted_available = sorted(set(available_months))
    available_indices = [month_to_index(year, month) for year, month in sorted_available]
    latest_end = sorted_available[-1]
    selected: list[tuple[int, int]] = []

    start_index = month_to_index(start_year, start_month)
    current_end = latest_end

    while True:
        selected.append(current_end)

        current_end_index = month_to_index(*current_end)
        covered_start_index = current_end_index - (coverage_months - 1)

        if covered_start_index <= start_index:
            break

        target_end_index = covered_start_index - 1
        current_end_index = month_to_index(*current_end)

        candidate_index = None
        for index in available_indices:
            if index >= current_end_index:
                break
            if target_end_index <= index <= target_end_index + (coverage_months - 1):
                candidate_index = index
                break

        candidate = index_to_month(candidate_index) if candidate_index is not None else None

        if candidate is None:
            break

        current_end = candidate

    return sorted(set(selected))


def get_md5_for_archive(year: int, month: int) -> str | None:
    """Fetch MD5 hash from the archive page for verification."""
    try:
        response = requests.get(f"{BASE_URL}/", timeout=30)
        response.raise_for_status()
        search_text = f"{year:04d}-{month:02d}.zip"
        content = response.text

        if search_text in content:
            idx = content.find(search_text)
            section = content[idx:idx + 500]
            lines = section.split('\n')
            for line in lines[1:5]:
                line = line.strip()
                if len(line) == 32 and all(c in '0123456789abcdef' for c in line):
                    return line
    except requests.RequestException:
        pass
    return None


def calculate_md5(filepath: Path) -> str:
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def download_archive(year: int, month: int, verify: bool = True, force: bool = False) -> bool:
    """
    Download a single archive file.

    Args:
        year: Archive year
        month: Archive month
        verify: Whether to verify MD5 checksum
        force: Force re-download even if file exists

    Returns:
        True if download successful, False otherwise
    """
    url = get_archive_url(year, month)
    filename = f"{year:04d}-{month:02d}.zip"
    filepath = ARCHIVE_DIR / filename
    temp_filepath = ARCHIVE_DIR / f"{filename}.partial"

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    if filepath.exists() and not force:
        print(f"✓ {filename} already exists (use --force to re-download)")
        return True

    print(f"\n📥 Downloading {filename}...")

    try:
        resume_byte = 0
        headers = {}

        if temp_filepath.exists():
            resume_byte = temp_filepath.stat().st_size
            headers["Range"] = f"bytes={resume_byte}-"
            print(f"  Resuming from {resume_byte / 1024 / 1024:.1f} MB...")

        response = requests.get(url, headers=headers, stream=True, timeout=30)

        if response.status_code == 404:
            print(f"✗ Archive {filename} not found (may not exist yet)")
            return False

        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        if resume_byte > 0:
            total_size += resume_byte

        mode = "ab" if resume_byte > 0 else "wb"

        with open(temp_filepath, mode) as f:
            with tqdm(
                total=total_size,
                initial=resume_byte,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"  {filename}",
                bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]"
            ) as pbar:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        temp_filepath.rename(filepath)

        if verify:
            print(f"  🔍 Verifying checksum...")
            expected_md5 = get_md5_for_archive(year, month)
            if expected_md5:
                actual_md5 = calculate_md5(filepath)
                if actual_md5 == expected_md5:
                    print(f"  ✓ Checksum verified")
                else:
                    print(f"  ✗ Checksum mismatch! Expected {expected_md5}, got {actual_md5}")
                    return False
            else:
                print(f"  ⚠ Could not fetch checksum for verification")

        print(f"✓ {filename} downloaded successfully")
        return True

    except requests.RequestException as e:
        print(f"✗ Download failed: {e}")
        return False


def download_latest() -> bool:
    """Download the latest available archive."""
    print("📡 Fetching latest archive...")
    url = f"{BASE_URL}/latest.zip"

    try:
        response = requests.head(url, allow_redirects=True, timeout=30)
        final_url = response.url
        filename = final_url.split("/")[-1]

        if filename.endswith(".zip") and "-" in filename:
            parts = filename.replace(".zip", "").split("-")
            year, month = int(parts[0]), int(parts[1])
            return download_archive(year, month)
        else:
            print(f"✗ Could not determine latest archive date")
            return False
    except requests.RequestException as e:
        print(f"✗ Failed to fetch latest archive info: {e}")
        return False


def download_since(start_year: int, start_month: int, all_months: bool = False, **kwargs) -> int:
    """Download archives from start month through latest (compact by default)."""
    requested_start = (start_year, start_month)
    available = get_available_archive_months()

    if not available:
        print("✗ No available archive months found.")
        return 0

    target_months = [(year, month) for year, month in available if (year, month) >= requested_start]

    if not target_months:
        print(f"✗ No archives available on or after {start_year:04d}-{start_month:02d}.")
        return 0

    if not all_months:
        target_months = plan_compact_archives(start_year, start_month, target_months)
        mode_text = "compact"
    else:
        mode_text = "monthly"

    first_year, first_month = target_months[0]
    last_year, last_month = target_months[-1]
    print(
        f"📦 Found {len(target_months)} archive(s) in {mode_text} mode from "
        f"{first_year:04d}-{first_month:02d} to {last_year:04d}-{last_month:02d}."
    )
    planned_list = ", ".join(f"{year:04d}-{month:02d}" for year, month in target_months)
    print(f"🗂 Planned archives: {planned_list}")

    success_count = 0
    for year, month in target_months:
        if download_archive(year, month, **kwargs):
            success_count += 1

    return success_count


def download_range(start_year: int, start_month: int, end_year: int, end_month: int, **kwargs) -> int:
    """
    Download archives for a date range.

    Returns:
        Number of successfully downloaded archives
    """
    success_count = 0
    current = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)

    while current <= end:
        if download_archive(current.year, current.month, **kwargs):
            success_count += 1

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return success_count


def parse_date(date_str: str) -> tuple[int, int]:
    """Parse YYYY-MM format to (year, month) tuple."""
    try:
        parsed = datetime.strptime(date_str, "%Y-%m")
        return parsed.year, parsed.month
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM format.")


def main():
    parser = argparse.ArgumentParser(
        description="Download Police.uk crime data archives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_archives.py --latest              # Download latest archive
  python download_archives.py --month 2024-01       # Download January 2024
  python download_archives.py --range 2023-01 2023-12  # Download all of 2023
    python download_archives.py --since 2018-01       # Download minimal snapshots from Jan 2018
    python download_archives.py --since 2018-01 --all-months  # Download every month from Jan 2018
  python download_archives.py --month 2024-06 --no-verify  # Skip MD5 check
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--latest", action="store_true", help="Download the latest archive")
    group.add_argument("--month", type=str, help="Download specific month (YYYY-MM)")
    group.add_argument("--range", nargs=2, metavar=("START", "END"),
                       help="Download range of months (YYYY-MM YYYY-MM)")
    group.add_argument("--since", type=str, metavar="START",
                       help="Download all available archives from start month (YYYY-MM) to latest")

    parser.add_argument("--no-verify", action="store_true", help="Skip MD5 checksum verification")
    parser.add_argument("--force", action="store_true", help="Force re-download existing files")
    parser.add_argument("--all-months", action="store_true",
                        help="With --since, download every monthly archive (not compact mode)")

    args = parser.parse_args()

    print("=" * 60)
    print("  Police.uk Archive Downloader")
    print("=" * 60)
    print(f"  Output directory: {ARCHIVE_DIR}")
    print("=" * 60)

    verify = not args.no_verify

    if args.latest:
        success = download_latest()
        sys.exit(0 if success else 1)

    elif args.month:
        year, month = parse_date(args.month)
        success = download_archive(year, month, verify=verify, force=args.force)
        sys.exit(0 if success else 1)

    elif args.range:
        start_year, start_month = parse_date(args.range[0])
        end_year, end_month = parse_date(args.range[1])
        count = download_range(start_year, start_month, end_year, end_month,
                               verify=verify, force=args.force)
        print(f"\n{'=' * 60}")
        print(f"  Downloaded {count} archive(s)")
        print("=" * 60)
        sys.exit(0 if count > 0 else 1)

    elif args.since:
        start_year, start_month = parse_date(args.since)
        count = download_since(
            start_year,
            start_month,
            all_months=args.all_months,
            verify=verify,
            force=args.force,
        )
        print(f"\n{'=' * 60}")
        print(f"  Downloaded {count} archive(s)")
        print("=" * 60)
        sys.exit(0 if count > 0 else 1)


if __name__ == "__main__":
    main()
