import pandas as pd
import os
import re
import shutil
import zipfile
from datetime import datetime


def ensure_month_directory(base_dir: str, date: str) -> str | None:
    """Ensure monthly archive folder exists, extracting from YYYY-MM.zip when needed."""
    month_dir = os.path.join(base_dir, date)
    if os.path.exists(month_dir):
        return month_dir

    zip_path = os.path.join(base_dir, f"{date}.zip")
    if not os.path.exists(zip_path):
        print(f"Warning: Neither {month_dir} nor {zip_path} exists.")
        return None

    print(f"Extracting {zip_path}...")
    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_members:
                print(f"Warning: No CSV files found in {zip_path}.")
                return None

            if any(name.startswith(f"{date}/") for name in csv_members):
                archive.extractall(base_dir)
            else:
                os.makedirs(month_dir, exist_ok=True)
                for member in csv_members:
                    filename = os.path.basename(member)
                    if not filename:
                        continue
                    target_path = os.path.join(month_dir, filename)
                    with archive.open(member) as source, open(target_path, "wb") as target:
                        shutil.copyfileobj(source, target)

    except zipfile.BadZipFile:
        print(f"Warning: {zip_path} is not a valid ZIP file.")
        return None
    except OSError as error:
        print(f"Warning: Failed to extract {zip_path}: {error}")
        return None

    if os.path.exists(month_dir):
        try:
            os.remove(zip_path)
            print(f"Removed extracted archive {zip_path}")
        except OSError as error:
            print(f"Warning: Extracted successfully but could not delete {zip_path}: {error}")
        return month_dir

    print(f"Warning: Extraction completed but {month_dir} was not created.")
    return None


def get_available_archive_months(base_dir: str, include_zip: bool = True) -> list[str]:
    """Discover available archive months from extracted folders and optionally YYYY-MM.zip files."""
    months: set[datetime] = set()
    month_pattern = re.compile(r"^\d{4}-\d{2}$")

    def add_month(value: str) -> None:
        if not month_pattern.fullmatch(value):
            return
        try:
            month_date = datetime.strptime(value, "%Y-%m")
            months.add(month_date)
        except ValueError:
            return

    if not os.path.exists(base_dir):
        return []

    for entry in os.listdir(base_dir):
        entry_path = os.path.join(base_dir, entry)

        if os.path.isdir(entry_path):
            add_month(entry)
        elif include_zip and os.path.isfile(entry_path) and entry.lower().endswith(".zip"):
            month_str = entry[:-4]
            add_month(month_str)

    return [month.strftime("%Y-%m") for month in sorted(months)]

def combine_leeds_data():
    base_dir = "data/archive"
    output_dir = "data/processed"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    seed_months = get_available_archive_months(base_dir, include_zip=True)

    if not seed_months:
        print(f"No archive months found in {base_dir}. Expected folders or ZIP files named YYYY-MM.")
        return

    print(f"Preparing archive months from {seed_months[0]} to {seed_months[-1]} ({len(seed_months)} seed month(s))...")

    for date in seed_months:
        ensure_month_directory(base_dir, date)

    dates = get_available_archive_months(base_dir, include_zip=False)

    if not dates:
        print(f"No extracted archive month folders found in {base_dir} after preparation.")
        return

    start_date = dates[0]
    end_date = dates[-1]
    
    MIN_LAT = 53.69
    MAX_LAT = 53.96
    MIN_LON = -1.80
    MAX_LON = -1.29
    
    street_dfs = []
    outcomes_dfs = []
    stop_search_dfs = []
    
    print(f"Processing data from {start_date} to {end_date} ({len(dates)} month(s))...")
    
    for date in dates:
        month_dir = os.path.join(base_dir, date)
        if not os.path.exists(month_dir):
            print(f"Warning: Directory {month_dir} does not exist.")
            continue
            
        print(f"Processing {date}...")
        
        street_file = os.path.join(month_dir, f"{date}-west-yorkshire-street.csv")
        if os.path.exists(street_file):
            try:
                df = pd.read_csv(street_file)
                if 'LSOA name' in df.columns:
                    leeds_df = df[df['LSOA name'].str.contains('Leeds', case=False, na=False)]
                    street_dfs.append(leeds_df)
            except Exception as e:
                print(f"Error reading {street_file}: {e}")

        outcomes_file = os.path.join(month_dir, f"{date}-west-yorkshire-outcomes.csv")
        if os.path.exists(outcomes_file):
            try:
                df = pd.read_csv(outcomes_file)
                if 'LSOA name' in df.columns:
                    leeds_df = df[df['LSOA name'].str.contains('Leeds', case=False, na=False)]
                    outcomes_dfs.append(leeds_df)
            except Exception as e:
                print(f"Error reading {outcomes_file}: {e}")
                
        stop_search_file = os.path.join(month_dir, f"{date}-west-yorkshire-stop-and-search.csv")
        if os.path.exists(stop_search_file):
            try:
                df = pd.read_csv(stop_search_file)
                if 'Latitude' in df.columns and 'Longitude' in df.columns:
                    df = df.dropna(subset=['Latitude', 'Longitude'])
                    
                    mask = (
                        (df['Latitude'] >= MIN_LAT) & 
                        (df['Latitude'] <= MAX_LAT) & 
                        (df['Longitude'] >= MIN_LON) & 
                        (df['Longitude'] <= MAX_LON)
                    )
                    leeds_df = df[mask]
                    stop_search_dfs.append(leeds_df)
            except Exception as e:
                 print(f"Error reading {stop_search_file}: {e}")

    print("Combining and saving files...")
    
    if street_dfs:
        combined_street = pd.concat(street_dfs, ignore_index=True)
        output_path = os.path.join(output_dir, "leeds_street_archive.csv")
        combined_street.to_csv(output_path, index=False)
        print(f"Saved {len(combined_street)} street records to {output_path}")
    else:
        print("No street data found.")

    if outcomes_dfs:
        combined_outcomes = pd.concat(outcomes_dfs, ignore_index=True)
        output_path = os.path.join(output_dir, "leeds_outcomes_combined.csv")
        combined_outcomes.to_csv(output_path, index=False)
        print(f"Saved {len(combined_outcomes)} outcome records to {output_path}")
    else:
        print("No outcome data found.")

    if stop_search_dfs:
        combined_stop_search = pd.concat(stop_search_dfs, ignore_index=True)
        output_path = os.path.join(output_dir, "leeds_stop_and_search_combined.csv")
        combined_stop_search.to_csv(output_path, index=False)
        print(f"Saved {len(combined_stop_search)} stop and search records to {output_path}")
    else:
         print("No stop and search data found.")

if __name__ == "__main__":
    combine_leeds_data()
