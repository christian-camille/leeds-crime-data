import requests
import json
import os
import time
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from shapely.validation import make_valid

def fetch_wards():
    # Leeds City Council MapServer - Polling Districts Layer
    url = "https://mapservices.leeds.gov.uk/arcgis/rest/services/Public/Boundary/MapServer/7/query"
    params = {
        "where": "1=1",
        "outFields": "WARD",
        "returnGeometry": "true",
        "f": "geojson", 
        "outSR": "4326"
    }
    
    output_file = "dashboard/data/leeds_wards.geojson"
    print(f"Fetching boundaries from {url}...")
    
    try:
        resp = requests.get(url, params=params, timeout=60)
        data = resp.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    features = data.get('features', [])
    if not features:
        # Fallback to check if it returned regular JSON instead of GeoJSON
        if 'features' in data:
            print("Received JSON features (not GeoJSON), requiring conversion...")
            pass
        else:
            print("No features found in response.")
            return

    print(f"Retrieved {len(features)} polling district fragments.")
    
    # Group by WARD
    ward_polys = {}
    
    for feat in features:
        props = feat.get('properties', {})
        if not props and 'attributes' in feat:
            props = feat['attributes']
            
        ward_name = props.get('WARD')
        
        if not ward_name:
            continue
            
        # FIX: Align MapServer name with Crime Data name
        if ward_name == "Crossgates & Whinmoor":
            ward_name = "Cross Gates & Whinmoor"
            
        try:
            geom_data = feat.get('geometry')
            if not geom_data:
                continue
            
            poly = shape(geom_data)
            if not poly.is_valid:
                poly = make_valid(poly)
                
            if ward_name not in ward_polys:
                ward_polys[ward_name] = []
            ward_polys[ward_name].append(poly)
        except Exception as e:
            print(f"Error parsing geometry for {ward_name}: {e}")
            continue

    print(f"Aggregating into {len(ward_polys)} unique wards...")
    
    final_features = []
    
    for ward, polys in ward_polys.items():
        try:
            buffered_polys = [p.buffer(0.0001) for p in polys]
            
            unified_poly = unary_union(buffered_polys)
            
            eroded_poly = unified_poly.buffer(-0.0001)
            
            simplified_poly = eroded_poly.simplify(0.0001, preserve_topology=True)
            
            final_features.append({
                "type": "Feature",
                "properties": {
                    "WARD_NAME": ward
                },
                "geometry": mapping(simplified_poly)
            })
        except Exception as e:
            print(f"Error dissolving ward {ward}: {e}")

    geojson = {
        "type": "FeatureCollection",
        "name": "Leeds Wards",
        "features": final_features
    }
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(geojson, f)
        
    print(f"Successfully saved {len(final_features)} wards to {output_file}")
    file_size_kb = os.path.getsize(output_file) / 1024
    print(f"File size: {file_size_kb:.2f} KB")

if __name__ == "__main__":
    fetch_wards()
