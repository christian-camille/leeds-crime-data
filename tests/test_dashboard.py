"""Tests for the dashboard output file (dashboard/data/crime_data.json)."""
import json
import os
import pytest


DASHBOARD_JSON_PATH = os.path.join("dashboard", "data", "crime_data.json")
POSTCODE_SEARCH_JSON_PATH = os.path.join("dashboard", "data", "postcode_search.json")

# Leeds coordinate bounds (must match prepare_dashboard_data.py source data)
LAT_MIN, LAT_MAX = 53.69, 53.96
LON_MIN, LON_MAX = -1.80, -1.29

# Point array field positions (defined by prepare_dashboard_data.py)
P_LAT = 0
P_LON = 1
P_CRIME_TYPE_IDX = 2
P_YEAR = 3
P_MONTH = 4
P_COUNT = 5
P_IS_CITY_CENTRE = 6
P_DIST_IDX = 7
P_WARD_IDX = 8

S_LAT = 0
S_LON = 1
S_CRIME_TYPE_IDX = 2
S_YEAR = 3
S_MONTH = 4
S_COUNT = 5


@pytest.fixture(scope="module")
def dashboard_data():
    """Load and parse the dashboard JSON file, skipping if not present."""
    if not os.path.exists(DASHBOARD_JSON_PATH):
        pytest.skip(f"Dashboard file not found: {DASHBOARD_JSON_PATH} — run the pipeline first")
    with open(DASHBOARD_JSON_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def postcode_search_data():
    """Load and parse the postcode search JSON file, skipping if not present."""
    if not os.path.exists(POSTCODE_SEARCH_JSON_PATH):
        pytest.skip(f"Postcode search file not found: {POSTCODE_SEARCH_JSON_PATH} — run the pipeline first")
    with open(POSTCODE_SEARCH_JSON_PATH, encoding="utf-8") as f:
        return json.load(f)


class TestDashboardSchema:
    """Validate the structure and content of crime_data.json."""

    def test_required_top_level_keys(self, dashboard_data):
        """All required top-level keys must be present."""
        required = {'t', 'y', 'w', 'pd', 'dw', 'cc', 'c', 'p'}
        missing = required - set(dashboard_data.keys())
        assert not missing, f"Missing top-level keys: {missing}"

    def test_crime_types_non_empty_strings(self, dashboard_data):
        """'t' must be a non-empty list of strings."""
        crime_types = dashboard_data['t']
        assert isinstance(crime_types, list), "'t' must be a list"
        assert len(crime_types) > 0, "'t' must not be empty"
        assert all(isinstance(ct, str) for ct in crime_types), "All crime types must be strings"

    def test_years_non_empty_integers(self, dashboard_data):
        """'y' must be a non-empty list of integers."""
        years = dashboard_data['y']
        assert isinstance(years, list), "'y' must be a list"
        assert len(years) > 0, "'y' must not be empty"
        assert all(isinstance(y, int) for y in years), "All years must be integers"
        assert all(2017 <= y <= 2030 for y in years), "Years must be in plausible range"

    def test_wards_non_empty_strings(self, dashboard_data):
        """'w' must be a non-empty list of ward name strings."""
        wards = dashboard_data['w']
        assert isinstance(wards, list), "'w' must be a list"
        assert len(wards) > 0, "'w' must not be empty"
        assert all(isinstance(w, str) for w in wards), "All ward names must be strings"

    def test_polling_districts_non_empty_strings(self, dashboard_data):
        """'pd' must be a non-empty list of polling district strings."""
        polling_districts = dashboard_data['pd']
        assert isinstance(polling_districts, list), "'pd' must be a list"
        assert len(polling_districts) > 0, "'pd' must not be empty"
        assert all(isinstance(d, str) for d in polling_districts), "All polling districts must be strings"

    def test_district_ward_mapping_valid(self, dashboard_data):
        """'dw' must be a list of valid ward indices with same length as 'pd'."""
        dw = dashboard_data['dw']
        pd_list = dashboard_data['pd']
        wards = dashboard_data['w']
        assert isinstance(dw, list), "'dw' must be a list"
        assert len(dw) == len(pd_list), f"'dw' length ({len(dw)}) must match 'pd' length ({len(pd_list)})"
        assert all(isinstance(i, int) for i in dw), "All district-ward indices must be integers"
        assert all(0 <= i < len(wards) for i in dw), "All district-ward indices must be valid ward indices"

    def test_city_centre_ward_is_string_in_wards(self, dashboard_data):
        """'cc' must be a string that exists in the ward list."""
        cc = dashboard_data['cc']
        assert isinstance(cc, str), "'cc' (city centre ward) must be a string"
        assert cc in dashboard_data['w'], f"City centre ward '{cc}' must appear in ward list 'w'"

    def test_centre_coordinates_in_leeds_bounds(self, dashboard_data):
        """'c' must have lat/lon within Leeds bounds."""
        centre = dashboard_data['c']
        assert isinstance(centre, dict), "'c' must be a dict"
        assert 'lat' in centre and 'lon' in centre, "'c' must have 'lat' and 'lon' keys"
        assert LAT_MIN <= centre['lat'] <= LAT_MAX, f"Centre lat {centre['lat']} outside Leeds bounds"
        assert LON_MIN <= centre['lon'] <= LON_MAX, f"Centre lon {centre['lon']} outside Leeds bounds"

    def test_points_non_empty(self, dashboard_data):
        """'p' must be a non-empty list of data points."""
        points = dashboard_data['p']
        assert isinstance(points, list), "'p' must be a list"
        assert len(points) > 0, "'p' must not be empty"

    def test_points_have_nine_fields(self, dashboard_data):
        """Every point must have exactly 9 fields."""
        points = dashboard_data['p']
        bad = [i for i, pt in enumerate(points) if len(pt) != 9]
        assert not bad, f"Points at indices {bad[:5]} do not have 9 fields"

    def test_point_coordinates_in_leeds_bounds(self, dashboard_data):
        """All point coordinates must fall within Leeds bounds."""
        points = dashboard_data['p']
        n = len(points)
        bad_lat = [pt[P_LAT] for pt in points if not (LAT_MIN <= pt[P_LAT] <= LAT_MAX)]
        bad_lon = [pt[P_LON] for pt in points if not (LON_MIN <= pt[P_LON] <= LON_MAX)]
        assert len(bad_lat) / n < 0.01, f"{len(bad_lat)} points have latitude outside Leeds bounds"
        assert len(bad_lon) / n < 0.01, f"{len(bad_lon)} points have longitude outside Leeds bounds"

    def test_point_crime_type_indices_valid(self, dashboard_data):
        """Crime type indices in points must be valid indices into 't'."""
        n_types = len(dashboard_data['t'])
        invalid = [pt[P_CRIME_TYPE_IDX] for pt in dashboard_data['p'] if not (0 <= pt[P_CRIME_TYPE_IDX] < n_types)]
        assert not invalid, f"{len(invalid)} points have out-of-range crime type indices"

    def test_point_years_in_year_list(self, dashboard_data):
        """Years in points must all appear in 'y'."""
        valid_years = set(dashboard_data['y'])
        invalid = [pt[P_YEAR] for pt in dashboard_data['p'] if pt[P_YEAR] not in valid_years]
        assert not invalid, f"{len(invalid)} points reference years not in 'y': {set(invalid)}"

    def test_point_months_in_valid_range(self, dashboard_data):
        """Month numbers in points must be 1–12."""
        invalid = [pt[P_MONTH] for pt in dashboard_data['p'] if not (1 <= pt[P_MONTH] <= 12)]
        assert not invalid, f"{len(invalid)} points have month values outside 1–12"

    def test_point_counts_positive(self, dashboard_data):
        """Crime counts in points must be positive integers."""
        invalid = [pt[P_COUNT] for pt in dashboard_data['p'] if not (isinstance(pt[P_COUNT], int) and pt[P_COUNT] > 0)]
        assert not invalid, f"{len(invalid)} points have non-positive or non-integer counts"

    def test_point_is_city_centre_binary(self, dashboard_data):
        """is_city_centre flag must be 0 or 1."""
        invalid = [pt[P_IS_CITY_CENTRE] for pt in dashboard_data['p'] if pt[P_IS_CITY_CENTRE] not in (0, 1)]
        assert not invalid, f"{len(invalid)} points have invalid is_city_centre values (must be 0 or 1)"

    def test_point_ward_indices_valid(self, dashboard_data):
        """Ward indices in points must be valid indices into 'w'."""
        n_wards = len(dashboard_data['w'])
        invalid = [pt[P_WARD_IDX] for pt in dashboard_data['p'] if not (0 <= pt[P_WARD_IDX] < n_wards)]
        assert not invalid, f"{len(invalid)} points have out-of-range ward indices"

    def test_point_district_indices_valid(self, dashboard_data):
        """Polling district indices in points must be valid indices into 'pd'."""
        n_districts = len(dashboard_data['pd'])
        invalid = [pt[P_DIST_IDX] for pt in dashboard_data['p'] if not (0 <= pt[P_DIST_IDX] < n_districts)]
        assert not invalid, f"{len(invalid)} points have out-of-range polling district indices"


class TestPostcodeSearchSchema:
    """Validate the structure and content of postcode_search.json."""

    def test_required_top_level_keys(self, postcode_search_data):
        required = {'t', 'y', 'r', 'p'}
        missing = required - set(postcode_search_data.keys())
        assert not missing, f"Missing top-level keys: {missing}"

    def test_search_crime_types_match_expected_shape(self, postcode_search_data):
        crime_types = postcode_search_data['t']
        assert isinstance(crime_types, list), "'t' must be a list"
        assert len(crime_types) > 0, "'t' must not be empty"
        assert all(isinstance(ct, str) for ct in crime_types), "All crime types must be strings"

    def test_search_years_non_empty_integers(self, postcode_search_data):
        years = postcode_search_data['y']
        assert isinstance(years, list), "'y' must be a list"
        assert len(years) > 0, "'y' must not be empty"
        assert all(isinstance(y, int) for y in years), "All years must be integers"

    def test_search_radius_is_100m(self, postcode_search_data):
        assert postcode_search_data['r'] == 100, "Expected default postcode search radius to be 100m"

    def test_search_points_non_empty(self, postcode_search_data):
        points = postcode_search_data['p']
        assert isinstance(points, list), "'p' must be a list"
        assert len(points) > 0, "'p' must not be empty"

    def test_search_points_have_six_fields(self, postcode_search_data):
        bad = [i for i, pt in enumerate(postcode_search_data['p']) if len(pt) != 6]
        assert not bad, f"Search points at indices {bad[:5]} do not have 6 fields"

    def test_search_point_coordinates_in_leeds_bounds(self, postcode_search_data):
        points = postcode_search_data['p']
        n = len(points)
        bad_lat = [pt[S_LAT] for pt in points if not (LAT_MIN <= pt[S_LAT] <= LAT_MAX)]
        bad_lon = [pt[S_LON] for pt in points if not (LON_MIN <= pt[S_LON] <= LON_MAX)]
        assert len(bad_lat) / n < 0.01, f"{len(bad_lat)} search points have latitude outside Leeds bounds"
        assert len(bad_lon) / n < 0.01, f"{len(bad_lon)} search points have longitude outside Leeds bounds"

    def test_search_point_crime_type_indices_valid(self, postcode_search_data):
        n_types = len(postcode_search_data['t'])
        invalid = [pt[S_CRIME_TYPE_IDX] for pt in postcode_search_data['p'] if not (0 <= pt[S_CRIME_TYPE_IDX] < n_types)]
        assert not invalid, f"{len(invalid)} search points have out-of-range crime type indices"

    def test_search_point_years_in_year_list(self, postcode_search_data):
        valid_years = set(postcode_search_data['y'])
        invalid = [pt[S_YEAR] for pt in postcode_search_data['p'] if pt[S_YEAR] not in valid_years]
        assert not invalid, f"{len(invalid)} search points reference years not in 'y': {set(invalid)}"

    def test_search_point_months_in_valid_range(self, postcode_search_data):
        invalid = [pt[S_MONTH] for pt in postcode_search_data['p'] if not (1 <= pt[S_MONTH] <= 12)]
        assert not invalid, f"{len(invalid)} search points have month values outside 1–12"

    def test_search_point_counts_positive(self, postcode_search_data):
        invalid = [pt[S_COUNT] for pt in postcode_search_data['p'] if not (isinstance(pt[S_COUNT], int) and pt[S_COUNT] > 0)]
        assert not invalid, f"{len(invalid)} search points have non-positive or non-integer counts"
