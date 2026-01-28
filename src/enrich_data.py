import pandas as pd
import requests
import os
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def enrich_data():
    input_file = "data/processed/leeds_street_combined.csv"
    
    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file, low_memory=False)
    
    unique_coords = df[['Latitude', 'Longitude']].drop_duplicates().dropna()
    print(f"Unique locations to enrich: {len(unique_coords)}")
    
    coord_map = {}
    
    batch_size = 100
    records = [row for row in unique_coords.itertuples(index=False)]
    chunks = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
    
    print(f"Fetching data for {len(chunks)} batches using 10 threads...")
    
    start_time = time.time()
    
    def fetch_batch(chunk):
        results_map = {}
        payload = {
            "geolocations": [
                {"longitude": r.Longitude, "latitude": r.Latitude, "limit": 1, "radius": 200} 
                for r in chunk
            ]
        }
        try:
            resp = requests.post("https://api.postcodes.io/postcodes", json=payload, timeout=20)
            if resp.status_code == 200:
                results = resp.json().get('result', [])
                for i, res in enumerate(results):
                    lat = chunk[i].Latitude
                    lon = chunk[i].Longitude
                    
                    ward = "Unknown"
                    pcd = "Unknown"
                    
                    if res['result']:
                         item = res['result'][0]
                         ward = item.get('admin_ward') or item.get('ward') or "Unknown"
                         raw_pc = item.get('postcode')
                         if raw_pc:
                             pcd = raw_pc.split(' ')[0]
                    
                    results_map[(lat, lon)] = {'ward': ward, 'pcd': pcd}
            return results_map
        except Exception as e:
            print(f"Error: {e}")
            return {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_batch, chunk) for chunk in chunks]
        
        for future in tqdm(as_completed(futures), total=len(chunks)):
            res = future.result()
            coord_map.update(res)
            


    print(f"Postcode/Ward lookup complete in {time.time() - start_time:.1f}s")
    print("Starting Polling District enrichment (Bulk Fetch & Local Join)...")
    
    polling_districts_data = [] # List of (shape, code, ward)
    
    try:
        url = "https://mapservices.leeds.gov.uk/arcgis/rest/services/Public/Boundary/MapServer/7/query"
        params = {
            "where": "1=1",
            "outFields": "POLLING_DI,WARD",
            "returnGeometry": "true",
            "f": "json",
            "outSR": "4326"
        }
        print("Fetching all polling district boundaries...")
        resp = requests.get(url, params=params, timeout=30)
        
        if resp.status_code == 200:
            data = resp.json()
            features = data.get("features", [])
            print(f"Retrieved {len(features)} polling district features.")
            
            from shapely.geometry import shape, Point
            from shapely.prepared import prep
            
            for feat in features:
                attr = feat.get("attributes", {})
                geom = feat.get("geometry", {})
                
                if geom and "rings" in geom:
                    
                    poly = None
                    try:
                        poly = shape({"type": "Polygon", "coordinates": geom["rings"]})
                    except:
                         from shapely.geometry import Polygon
                         if len(geom["rings"]) > 0:
                             poly = Polygon(geom["rings"][0], geom["rings"][1:])
                    
                    if poly:
                        polling_districts_data.append({
                            "poly": poly,
                            "prepared": prep(poly),
                            "code": attr.get("POLLING_DI"),
                            "ward": attr.get("WARD")
                        })
        else:
            print(f"Failed to fetch boundaries: {resp.status_code}")
            
    except Exception as e:
        print(f"Error fetching/parsing polygons: {e}")
        
    print(f"Built {len(polling_districts_data)} spatial objects.")
    
    print("Performing spatial join...")
    polling_map = {}
    
    hits = 0
    from shapely.geometry import Point
    
    for row in tqdm(unique_coords.itertuples(index=False), total=len(unique_coords), desc="Spatial Join"):
        pt = Point(row.Longitude, row.Latitude)
        found = False
        
        for item in polling_districts_data:
            if item["prepared"].contains(pt):
                polling_map[(row.Latitude, row.Longitude)] = item["code"]
                input_ward = item["ward"]
                # Optionally use this ward if missing from postcode lookup
                found = True
                hits += 1
                break
        
    print(f"Spatial join complete. Matches: {hits}/{len(unique_coords)}")

    print("Applying mappings to main dataset...")
    
    lats = df['Latitude'].values
    lons = df['Longitude'].values
    
    wards = []
    pcds = []
    polling_districts = []
    
    count_hit = 0
    count_miss = 0
    
    for lat, lon in zip(lats, lons):
        val = coord_map.get((lat, lon))
        ward_val = "Unknown"
        pcd_val = "Unknown"
        
        if val:
            ward_val = val['ward']
            pcd_val = val['pcd']
            count_hit += 1
        else:
            count_miss += 1
            
        pd_val = polling_map.get((lat, lon), "Unknown")
        
        wards.append(ward_val)
        pcds.append(pcd_val)
        polling_districts.append(pd_val)
            
    df['Ward Name'] = wards
    df['Postcode District'] = pcds
    df['Polling District'] = polling_districts
    
    print(f"Applied. Hits: {count_hit}, Misses: {count_miss}")
    
    print(f"Saving to {input_file} (overwriting)...")
    temp_save = input_file + ".tmp"
    df.to_csv(temp_save, index=False)
    
    if os.path.exists(input_file):
        os.remove(input_file)
    os.rename(temp_save, input_file)
    print("Done.")

if __name__ == "__main__":
    enrich_data()
