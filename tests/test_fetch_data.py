import json

import pandas as pd
import requests

from src import fetch_data


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or []

    def json(self):
        return self._payload


def test_fetch_crime_data_resumes_partial_month(tmp_path, monkeypatch):
    calls = []
    payloads = [
        [{"id": 1, "category": "burglary"}],
        [{"id": 2, "category": "robbery"}],
    ]

    def fake_arange(start, stop, step):
        if start > 0:
            return [53.7]
        return [-1.8, -1.78]

    def interrupted_get(url, params, timeout):
        calls.append((params["lat"], params["lng"], params["date"]))
        if len(calls) == 1:
            return FakeResponse(200, payloads[0])
        raise KeyboardInterrupt()

    monkeypatch.setattr(fetch_data.np, "arange", fake_arange)
    monkeypatch.setattr(fetch_data.requests, "get", interrupted_get)
    monkeypatch.setattr(fetch_data.time, "sleep", lambda _: None)

    try:
        fetch_data.fetch_crime_data("2024-01", "2024-01", output_dir=str(tmp_path))
    except KeyboardInterrupt:
        pass

    state_dir = tmp_path / fetch_data.CHECKPOINT_DIRNAME
    checkpoint_path = state_dir / "leeds_crime_2024_01.json"
    partial_path = state_dir / "leeds_crime_2024_01.jsonl"
    assert checkpoint_path.exists()
    assert partial_path.exists()
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["next_point_index"] == 1
    assert checkpoint["status"] == "in_progress"
    assert checkpoint["failed_points"] == []

    resumed_calls = []

    def resumed_get(url, params, timeout):
        resumed_calls.append((params["lat"], params["lng"], params["date"]))
        return FakeResponse(200, payloads[1])

    monkeypatch.setattr(fetch_data.requests, "get", resumed_get)
    fetch_data.fetch_crime_data("2024-01", "2024-01", output_dir=str(tmp_path))

    output_file = tmp_path / "leeds_crime_2024_01.csv"
    assert output_file.exists()
    result = pd.read_csv(output_file)
    assert sorted(result["id"].tolist()) == [1, 2]
    assert resumed_calls == [(53.7, -1.78, "2024-01")]
    assert not checkpoint_path.exists()
    assert not partial_path.exists()


def test_fetch_crime_data_skips_known_unavailable_month(tmp_path, monkeypatch):
    calls = []

    def fake_arange(start, stop, step):
        return [53.7] if start > 0 else [-1.8]

    def unavailable_get(url, params, timeout):
        calls.append((params["lat"], params["lng"], params["date"]))
        return FakeResponse(404)

    monkeypatch.setattr(fetch_data.np, "arange", fake_arange)
    monkeypatch.setattr(fetch_data.requests, "get", unavailable_get)
    monkeypatch.setattr(fetch_data.time, "sleep", lambda _: None)

    fetch_data.fetch_crime_data("2024-02", "2024-02", output_dir=str(tmp_path))

    checkpoint_path = tmp_path / fetch_data.CHECKPOINT_DIRNAME / "leeds_crime_2024_02.json"
    assert checkpoint_path.exists()
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["status"] == "unavailable"
    assert checkpoint["failed_points"] == []
    assert calls == [(53.7, -1.8, "2024-02")]

    def fail_if_called(url, params, timeout):
        raise AssertionError("requests.get should not be called for a known unavailable month")

    monkeypatch.setattr(fetch_data.requests, "get", fail_if_called)
    fetch_data.fetch_crime_data("2024-02", "2024-02", output_dir=str(tmp_path))


def test_fetch_crime_data_retries_transient_errors_before_marking_failure(tmp_path, monkeypatch):
    calls = []

    def fake_arange(start, stop, step):
        return [53.7] if start > 0 else [-1.8]

    responses = [requests.exceptions.ReadTimeout("read timed out"), FakeResponse(200, [{"id": 10}])]

    def flaky_get(url, params, timeout):
        calls.append((params["lat"], params["lng"], params["date"]))
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(fetch_data.np, "arange", fake_arange)
    monkeypatch.setattr(fetch_data.requests, "get", flaky_get)
    monkeypatch.setattr(fetch_data.time, "sleep", lambda _: None)

    fetch_data.fetch_crime_data("2024-03", "2024-03", output_dir=str(tmp_path), max_retries=2)

    result = pd.read_csv(tmp_path / "leeds_crime_2024_03.csv")
    assert result["id"].tolist() == [10]
    assert len(calls) == 2


def test_fetch_crime_data_repairs_existing_month_without_checkpoint(tmp_path, monkeypatch):
    output_file = tmp_path / "leeds_crime_2024_04.csv"
    pd.DataFrame([{"id": 1, "category": "burglary"}]).to_csv(output_file, index=False)

    def fake_arange(start, stop, step):
        if start > 0:
            return [53.7]
        return [-1.8, -1.78]

    calls = []

    def repair_get(url, params, timeout):
        calls.append((params["lat"], params["lng"], params["date"]))
        if params["lng"] == -1.8:
            return FakeResponse(200, [{"id": 1, "category": "burglary"}])
        return FakeResponse(200, [{"id": 2, "category": "robbery"}])

    monkeypatch.setattr(fetch_data.np, "arange", fake_arange)
    monkeypatch.setattr(fetch_data.requests, "get", repair_get)
    monkeypatch.setattr(fetch_data.time, "sleep", lambda _: None)

    fetch_data.fetch_crime_data("2024-04", "2024-04", output_dir=str(tmp_path), repair_existing=True)

    result = pd.read_csv(output_file)
    assert sorted(result["id"].tolist()) == [1, 2]
    assert calls == [(53.7, -1.8, "2024-04"), (53.7, -1.78, "2024-04")]