import requests
import pandas as pd
import time
import os
import numpy as np

def fetch_crime_data(start_date, end_date, output_dir="data/raw"):
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    MIN_LAT = 53.69
    MAX_LAT = 53.96
    MIN_LON = -1.80
    MAX_LON = -1.29
    STEP = 0.02
    
    lats = np.arange(MIN_LAT, MAX_LAT, STEP)
    lons = np.arange(MIN_LON, MAX_LON, STEP)
    
    dates = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()
    
    base_url = "https://data.police.uk/api/crimes-street/all-crime"
    
    print(f"Fetching data for {len(dates)} months using grid ({len(lats) * len(lons)} points)...")
    
    for date in dates:
        output_file = os.path.join(output_dir, f"leeds_crime_{date.replace('-', '_')}.csv")
        
        if os.path.exists(output_file):
            print(f"Skipping {date}, already exists.")
            continue
            
        print(f"Fetching data for {date}...")
        all_crimes = []
        status_counts = {}
        month_unavailable = False
        
        count = 0
        total_points = len(lats) * len(lons)
        
        for lat in lats:
            for lon in lons:
                count += 1
                if count % 50 == 0:
                    print(f"  Processed {count}/{total_points} grid points...")
                    
                try:
                    response = requests.get(base_url, params={'lat': lat, 'lng': lon, 'date': date}, timeout=10)
                    status_counts[response.status_code] = status_counts.get(response.status_code, 0) + 1
                    
                    if response.status_code == 200:
                        data = response.json()
                        all_crimes.extend(data)
                    elif response.status_code == 429:
                        print(f"Rate limited on {date}. Waiting for 5 seconds...")
                        time.sleep(5)
                        response = requests.get(base_url, params={'lat': lat, 'lng': lon, 'date': date}, timeout=10)
                        if response.status_code == 200:
                             all_crimes.extend(response.json())
                        else:
                            status_counts[response.status_code] = status_counts.get(response.status_code, 0) + 1
                    elif response.status_code == 404:
                        print(f"Date {date} is not available from Police API (HTTP 404). Skipping this month.")
                        month_unavailable = True
                        break
                    else:
                        pass
                        
                except Exception as e:
                    print(f"Exception for {date} at {lat},{lon}: {e}")
                
                time.sleep(0.1)

            if month_unavailable:
                break

        if month_unavailable:
            continue

        non_success_status = {code: count for code, count in status_counts.items() if code != 200}
        if non_success_status:
            status_summary = ", ".join(f"{code}:{count}" for code, count in sorted(non_success_status.items()))
            print(f"HTTP status summary for {date}: {status_summary}")
        
        if all_crimes:
            df = pd.DataFrame(all_crimes)
            initial_len = len(df)
            if 'id' in df.columns:
                df = df.drop_duplicates(subset=['id'])
            elif 'persistent_id' in df.columns:
                df = df.drop_duplicates(subset=['persistent_id'])
            
            print(f"Fetched {initial_len} records. Deduplicated to {len(df)} records.")
            
            df.to_csv(output_file, index=False)
            print(f"Saved to {output_file}")
        else:
            print(f"No records found for {date}")

if __name__ == "__main__":
    START_DATE = "2022-11" 
    END_DATE = "2025-12"
    
    fetch_crime_data(START_DATE, END_DATE)
