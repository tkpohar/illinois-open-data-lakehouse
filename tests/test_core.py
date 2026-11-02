import json
from pathlib import Path

import pytest

from lakehouse.config import SourceConfig, load_sources
from lakehouse.ingestion.socrata import SocrataClient
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"
SRC = SourceConfig(
    name="chicago_crimes",
    domain="example.org",
    dataset_id="abcd-1234",
    primary_key="id",
    incremental_column="updated_on",
)


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class FakeSession:
    """Serves the fixture in pages according to $limit/$offset."""

    def __init__(self, records):
        self.records, self.calls, self.headers = records, [], {}

    def get(self, url, params, timeout):
        self.calls.append(params)
        start, limit = params["$offset"], params["$limit"]
        return FakeResponse(self.records[start : start + limit])


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


def test_load_sources_from_repo_config():
    sources = load_sources()
    assert "chicago_crimes" in sources
    assert sources["chicago_crimes"].endpoint.endswith("/resource/ijzp-q8t2.json")


def test_load_sources_rejects_missing_keys(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("sources:\n  x:\n    domain: a.org\n")
    with pytest.raises(ValueError, match="missing required keys"):
        load_sources(bad)


def test_client_paginates(records):
    session = FakeSession(records)
    client = SocrataClient(page_size=2, session=session)
    out = list(client.fetch(SRC))
    assert len(out) == len(records)
    assert len(session.calls) == 3  # 2 + 2 + 1


def test_client_respects_max_records_and_since(records):
    session = FakeSession(records)
    client = SocrataClient(page_size=10, session=session)
    out = list(client.fetch(SRC, since="2026-01-01T00:00:00", max_records=3))
    assert len(out) == 3
    assert session.calls[0]["$where"] == "updated_on > '2026-01-01T00:00:00'"


def test_bronze_write_and_watermark(records):
    store = BronzeStore(":memory:")
    n = store.write("chicago_crimes", records, primary_key="id", incremental_column="updated_on")
    assert n == len(records) == store.count("chicago_crimes")
    assert store.last_watermark("chicago_crimes") == max(r["updated_on"] for r in records)
