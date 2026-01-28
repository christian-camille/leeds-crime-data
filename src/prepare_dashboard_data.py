"""
Prepares aggregated crime data for the dashboard.
Aggregates raw data into a grid structure grouped by crime type and year-month.
Includes ward data for top wards chart and city centre filtering.
"""

import pandas as pd
import numpy as np
import json
import os

GRID_SIZE = 80
INPUT_PATH = os.path.join("data", "processed", "leeds_street_combined.csv")
OUTPUT_PATH = os.path.join("dashboard", "data", "crime_data.json")
CITY_CENTRE_WARD = "Little London & Woodhouse"


def prepare_dashboard_data():
    print(f"Loading data from {INPUT_PATH}...")
    df = pd.read_csv(INPUT_PATH)
    
    df_clean = df.dropna(subset=['Latitude', 'Longitude', 'Month', 'Crime type', 'Ward Name']).copy()
    print(f"Records with valid data: {len(df_clean):,}")
    
    df_clean['Year'] = df_clean['Month'].str[:4]
    df_clean['MonthNum'] = df_clean['Month'].str[5:7]
    
    min_lat, max_lat = df_clean['Latitude'].min(), df_clean['Latitude'].max()
    min_lon, max_lon = df_clean['Longitude'].min(), df_clean['Longitude'].max()
    
    lat_bins = np.linspace(min_lat, max_lat, GRID_SIZE + 1)
    lon_bins = np.linspace(min_lon, max_lon, GRID_SIZE + 1)
    
    lat_centers = (lat_bins[:-1] + lat_bins[1:]) / 2
    lon_centers = (lon_bins[:-1] + lon_bins[1:]) / 2
    
    df_clean['lat_idx'] = np.digitize(df_clean['Latitude'], lat_bins) - 1
    df_clean['lon_idx'] = np.digitize(df_clean['Longitude'], lon_bins) - 1
    
    df_clean['lat_idx'] = df_clean['lat_idx'].clip(0, GRID_SIZE - 1)
    df_clean['lon_idx'] = df_clean['lon_idx'].clip(0, GRID_SIZE - 1)
    
    df_clean['is_city_centre'] = (df_clean['Ward Name'] == CITY_CENTRE_WARD).astype(int)
    
    polling_districts = sorted(df_clean['Polling District'].astype(str).unique().tolist())
    dist_map = {d: i for i, d in enumerate(polling_districts)}
    df_clean['dist_idx'] = df_clean['Polling District'].astype(str).map(dist_map)
    
    print("Aggregating by grid cell, crime type, ward, and year-month...")
    grouped = df_clean.groupby(
        ['lat_idx', 'lon_idx', 'Crime type', 'Year', 'MonthNum', 'is_city_centre', 'dist_idx']
    ).size().reset_index(name='count')
    
    grouped['lat'] = grouped['lat_idx'].apply(lambda x: round(lat_centers[x], 4))
    grouped['lon'] = grouped['lon_idx'].apply(lambda x: round(lon_centers[x], 4))
    
    crime_types = sorted(df_clean['Crime type'].unique().tolist())
    years = sorted(df_clean['Year'].unique().tolist())
    wards = sorted(df_clean['Ward Name'].unique().tolist())
    
    crime_type_map = {ct: i for i, ct in enumerate(crime_types)}
    
    print(f"Crime types: {len(crime_types)}")
    print(f"Years: {years}")
    print(f"Wards: {len(wards)}")
    print(f"Polling Districts: {len(polling_districts)}")
    print(f"Aggregated points: {len(grouped):,}")
    
    points = []
    for _, row in grouped.iterrows():
        points.append([
            row['lat'],
            row['lon'],
            crime_type_map[row['Crime type']],
            int(row['Year']),
            int(row['MonthNum']),
            int(row['count']),
            int(row['is_city_centre']),
            int(row['dist_idx'])
        ])
    
    print("Building Polling District -> Ward mapping...")
    pd_ward_map = df_clean.groupby('Polling District')['Ward Name'].first().to_dict()
    ward_map = {w: i for i, w in enumerate(wards)}
    dist_ward_indices = [ward_map[pd_ward_map[d]] for d in polling_districts]

    output_data = {
        't': crime_types,
        'y': [int(y) for y in years],
        'w': wards,
        'pd': polling_districts,
        'dw': dist_ward_indices,
        'cc': CITY_CENTRE_WARD,
        'c': {
            'lat': round((min_lat + max_lat) / 2, 4),
            'lon': round((min_lon + max_lon) / 2, 4)
        },
        'p': points
    }
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    print(f"Writing to {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output_data, f, separators=(',', ':'))
    
    file_size = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"Done! File size: {file_size:.2f} MB")


if __name__ == "__main__":
    prepare_dashboard_data()
