import pandas as pd
import requests
import os
import glob
import ast
import json
from shapely.geometry import shape, Point
from shapely.prepared import prep
from tqdm import tqdm

RAW_DIR = "data/raw"
OUTPUT_FILE = "data/processed/leeds_street_api_clean.csv"
LEEDS_BOUNDARY_URL = "https://nominatim.openstreetmap.org/search?q=Leeds,+West+Yorkshire,+United+Kingdom&polygon_geojson=1&format=json"
LSOA_BOUNDARY_URL = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query?where=LSOA11NM%20like%20%27Leeds%25%27&outFields=*&f=geojson"
LSOA_FILE = "data/raw/leeds_lsoa_2011.geojson"

def normalize_raw_data():
    print("Step 1: Loading and Normalizing Raw Data...")
    raw_files = glob.glob(os.path.join(RAW_DIR, "*.csv"))
    if not raw_files:
        print("No raw files found.")
        return None
        
    raw_dfs = []
    for f in raw_files:
        try:
            if "leeds_crime" in f:
                df = pd.read_csv(f)
                raw_dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not raw_dfs:
        print("No valid raw API data found.")
        return None
        
    df_raw = pd.concat(raw_dfs, ignore_index=True)
    print(f"Loaded {len(df_raw)} raw records.")
    
    def get_lat_lon_loc(loc_str):
        try:
            if pd.isna(loc_str): return None, None, None
            d = ast.literal_eval(loc_str)
            return d.get('latitude'), d.get('longitude'), d.get('street', {}).get('name')
        except: return None, None, None

    def get_outcome(outcome_str):
        try:
            if pd.isna(outcome_str): return ""
            d = ast.literal_eval(outcome_str)
            return d.get('category', "")
        except: return ""

    print("Parsing JSON columns...")
    loc_data = df_raw['location'].apply(get_lat_lon_loc)
    df_raw['Latitude'] = [float(x[0]) if x[0] else None for x in loc_data]
    df_raw['Longitude'] = [float(x[1]) if x[1] else None for x in loc_data]
    df_raw['Location'] = [x[2] for x in loc_data]
    
    df_raw['Last outcome category'] = df_raw['outcome_status'].apply(get_outcome)
    
    df_raw['Crime ID'] = df_raw['persistent_id'].combine_first(df_raw['id'])
    df_raw['Month'] = df_raw['month']
    df_raw['Reported by'] = "West Yorkshire Police"
    df_raw['Falls within'] = "West Yorkshire Police"
    category_map = {
        'anti-social-behaviour': 'Anti-social behaviour',
        'burglary': 'Burglary',
        'criminal-damage-arson': 'Criminal damage and arson',
        'drugs': 'Drugs',
        'other-theft': 'Other theft',
        'possession-of-weapons': 'Possession of weapons',
        'public-order': 'Public order',
        'robbery': 'Robbery',
        'shoplifting': 'Shoplifting',
        'theft-from-the-person': 'Theft from the person',
        'vehicle-crime': 'Vehicle crime',
        'violent-crime': 'Violence and sexual offences',
        'bicycle-theft': 'Bicycle theft',
        'other-crime': 'Other crime'
    }
    df_raw['Crime type'] = df_raw['category'].map(category_map).fillna(df_raw['category'])
    
    cols = ['Crime ID', 'Month', 'Reported by', 'Falls within',
            'Longitude', 'Latitude', 'Location', 'Crime type',
            'Last outcome category']
            
    return df_raw[cols].copy()

def filter_leeds_boundary(df):
    print("Step 2: Filtering Non-Leeds Data...")
    
    headers = {'User-Agent': 'LeedsCrimeAnalysis/1.0'}
    
    try:
        print("Fetching Leeds boundary...")
        resp = requests.get(LEEDS_BOUNDARY_URL, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        poly = None
        for item in data:
            if item.get('geojson') and item.get('type') == 'administrative':
                poly = shape(item['geojson'])
                break
        if not poly and data: poly = shape(data[0]['geojson'])
        
        if not poly: raise Exception("No polygon found")
        
    except Exception as e:
        print(f"Error fetching boundary: {e}")
        return df
        
    prepared = prep(poly)
    
    initial = len(df)
    unique_coords = df[['Latitude', 'Longitude']].drop_duplicates()
    print(f"Checking {len(unique_coords)} unique locations...")
    
    valid_coords = set()
    for idx, row in unique_coords.iterrows():
        lat, lon = row['Latitude'], row['Longitude']
        if pd.isna(lat) or pd.isna(lon): continue
        if prepared.contains(Point(lon, lat)):
             valid_coords.add((lat, lon))
             
    keep_indices = []
    for idx in df.index:
        pt = (df.at[idx, 'Latitude'], df.at[idx, 'Longitude'])
        if pt in valid_coords:
            keep_indices.append(idx)
            
    df_clean = df.loc[keep_indices].copy()
    print(f"Filtered: {initial} -> {len(df_clean)} records.")
    return df_clean

def assign_lsoa(df):
    print("Step 3: Assigning LSOA Codes...")
    
    if not os.path.exists(LSOA_FILE):
        print("Downloading LSOA boundaries...")
        try:
             resp = requests.get(LSOA_BOUNDARY_URL, timeout=30)
             resp.raise_for_status()
             with open(LSOA_FILE, 'wb') as f: f.write(resp.content)
        except Exception as e:
            print(f"Error downloading LSOA: {e}")
            df['LSOA code'] = ""
            df['LSOA name'] = ""
            return df
            
    with open(LSOA_FILE, 'r') as f:
        geojson = json.load(f)
        
    lsoa_polys = []
    for feature in geojson['features']:
        props = feature['properties']
        lsoa_polys.append({
            'code': props['LSOA11CD'],
            'name': props['LSOA11NM'],
            'poly': prep(shape(feature['geometry']))
        })
        
    unique_coords = df[['Latitude', 'Longitude']].drop_duplicates()
    coord_map = {}
    
    print(f"Mapping {len(unique_coords)} locations to LSOAs...")
    for idx, row in tqdm(unique_coords.iterrows(), total=len(unique_coords)):
        lat, lon = row['Latitude'], row['Longitude']
        pt = Point(lon, lat)
        
        match = None
        for lsoa in lsoa_polys:
            if lsoa['poly'].contains(pt):
                match = (lsoa['code'], lsoa['name'])
                break
        
        if match:
            coord_map[(lat, lon)] = match
        else:
            coord_map[(lat, lon)] = ("E01000000", "Leeds (Unmatched)")
            
    codes = []
    names = []
    for idx in df.index:
        lat, lon = df.at[idx, 'Latitude'], df.at[idx, 'Longitude']
        val = coord_map.get((lat, lon), ("", ""))
        codes.append(val[0])
        names.append(val[1])
        
    df['LSOA code'] = codes
    df['LSOA name'] = names
    
    return df

def process_api_data():
    df = normalize_raw_data()
    if df is None: return
    
    df = filter_leeds_boundary(df)
    
    df = assign_lsoa(df)
    
    print(f"Saving {len(df)} records to {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")

if __name__ == "__main__":
    process_api_data()
