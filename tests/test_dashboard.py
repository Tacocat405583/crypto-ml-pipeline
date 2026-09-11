"""The dashboard renders end to end against the API, and degrades cleanly without it.

Streamlit's AppTest runs the page headless in this process. Its HTTP calls are
routed into the FastAPI app itself, over the same Parquet fixtures the API
tests use, so this exercises the real dashboard against the real API code.
"""

import sys
from pathlib import Path

import pytest
import requests
import streamlit as st
from streamlit.testing.v1 import AppTest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "api"))
import app as api  # noqa: E402
from test_api import TestClient, forecasts, zones  # noqa: E402,F401  (fixtures)

PAGE = str(Path(__file__).resolve().parents[1] / "services" / "dashboard" / "app.py")


@pytest.fixture(autouse=True)
def fresh_cache():
    st.cache_data.clear()
    yield
    st.cache_data.clear()


def route_to(client, monkeypatch):
    def fake_get(url, params=None, timeout=None):
        path = "/" + url.split("://", 1)[1].split("/", 1)[1]     # http://host:port/bars -> /bars
        return client.get(path, params=params)
    monkeypatch.setattr(requests, "get", fake_get)


def test_page_renders_every_section_from_the_api(zones, forecasts, monkeypatch):
    route_to(TestClient(api.create_app(*zones, forecasts_dir=forecasts)), monkeypatch)
    at = AppTest.from_file(PAGE, default_timeout=60).run()
    assert not at.exception, at.exception
    assert at.title[0].value == "BTC-USD pipeline"
    assert [m.label for m in at.metric][:2] == ["Last close", "Volatility, last hour"]
    assert len(at.metric) == 4
    assert {s.value for s in at.subheader} >= {"Hourly close", "Data quality", "Latest ticks"}
    assert at.success and "No errors" in at.success[0].value


def test_api_down_shows_how_to_start_it(monkeypatch):
    def refuse(*a, **k):
        raise requests.ConnectionError("refused")
    monkeypatch.setattr(requests, "get", refuse)
    at = AppTest.from_file(PAGE, default_timeout=60).run()
    assert not at.exception
    assert "Can't reach the API" in at.error[0].value and "uvicorn" in at.error[0].value
