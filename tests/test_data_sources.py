"""Tests for external data source availability."""
import pytest
import requests


class TestDataSources:
    """Verify that external APIs and data sources are accessible."""
    
    @pytest.mark.timeout(10)
    def test_police_api_available(self):
        """UK Police API should respond to requests."""
        url = "https://data.police.uk/api/crimes-street/all-crime"
        params = {'lat': 53.8, 'lng': -1.55, 'date': '2024-01'}
        
        response = requests.get(url, params=params, timeout=10)
        
        assert response.status_code == 200, f"Police API returned {response.status_code}"
    
    @pytest.mark.timeout(10)
    def test_postcodes_api_available(self):
        """Postcodes.io API should respond to requests."""
        url = "https://api.postcodes.io/postcodes?lon=-1.55&lat=53.8&limit=1"
        
        response = requests.get(url, timeout=10)
        
        assert response.status_code == 200, f"Postcodes API returned {response.status_code}"
    
    @pytest.mark.timeout(10)
    def test_osm_nominatim_available(self):
        """OpenStreetMap Nominatim API should respond."""
        url = "https://nominatim.openstreetmap.org/search"
        params = {'q': 'Leeds, UK', 'format': 'json'}
        headers = {'User-Agent': 'LeedsCrimeTests/1.0'}
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        assert response.status_code == 200, f"OSM API returned {response.status_code}"
    
    @pytest.mark.timeout(15)
    def test_ons_lsoa_api_available(self):
        """ONS ArcGIS API for LSOA boundaries should respond."""
        url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query"
        params = {'where': '1=1', 'returnCountOnly': 'true', 'f': 'json'}

        response = requests.get(url, params=params, timeout=15)

        assert response.status_code == 200, f"ONS API returned {response.status_code}"

    @pytest.mark.timeout(15)
    def test_leeds_mapserver_available(self):
        """Leeds Council MapServer API for ward/polling district boundaries should respond."""
        url = "https://mapservices.leeds.gov.uk/arcgis/rest/services/Public/Boundary/MapServer/7/query"
        params = {'where': '1=1', 'returnCountOnly': 'true', 'f': 'json'}

        response = requests.get(url, params=params, timeout=15)

        assert response.status_code == 200, f"Leeds MapServer returned {response.status_code}"

    @pytest.mark.timeout(10)
    def test_postcodes_batch_post_available(self):
        """Postcodes.io batch POST endpoint should respond - this is the pattern used by the pipeline."""
        url = "https://api.postcodes.io/postcodes"
        payload = {"geolocations": [{"longitude": -1.55, "latitude": 53.8, "limit": 1}]}

        response = requests.post(url, json=payload, timeout=10)

        assert response.status_code == 200, f"Postcodes batch POST returned {response.status_code}"
        data = response.json()
        assert data.get("status") == 200
        assert isinstance(data.get("result"), list), "Batch POST response should contain a 'result' list"
