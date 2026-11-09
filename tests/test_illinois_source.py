"""The fourth source: City of Urbana Police Arrests on data.illinois.gov.

The fixture is synthetic: it mirrors the dataset's confirmed columns and the Socrata
system fields, not real arrest records.
"""

import json
from pathlib import Path

import duckdb
import pytest

from lakehouse import cli
from lakehouse.config import SourceConfig, load_sources
from lakehouse.ingestion.socrata import SocrataClient
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "illinois_urbana_arrests_sample.json"
NAME = "illinois_urbana_arrests"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def src():
    return load_sources()[NAME]


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class FakeSession:
    def __init__(self, records):
        self.records, self.calls, self.headers = records, [], {}

    def get(self, url, params, timeout):
        self.calls.append((url, params))
        start, limit = params["$offset"], params["$limit"]
        return FakeResponse(self.records[start : start + limit])


def test_source_is_configured_on_state_portal(src):
    assert src.domain == "data.illinois.gov"
    assert src.endpoint == "https://data.illinois.gov/resource/s2ps-ct5e.json"
    assert src.primary_key == ":id" and src.incremental_column == ":updated_at"
    assert src.uses_system_fields


def test_four_sources_across_two_portals():
    sources = load_sources()
    assert len(sources) == 4
    assert {s.domain for s in sources.values()} == {"data.cityofchicago.org", "data.illinois.gov"}
    chicago = [s for s in sources.values() if s.domain == "data.cityofchicago.org"]
    assert not any(s.uses_system_fields for s in chicago)


def test_uses_system_fields_detection():
    plain = SourceConfig("x", "d.org", "abcd-1234", primary_key="id", incremental_column="ts")
    key_only = SourceConfig("x", "d.org", "abcd-1234", primary_key=":id")
    inc_only = SourceConfig("x", "d.org", "abcd-1234", "id", incremental_column=":updated_at")
    assert not plain.uses_system_fields
    assert key_only.uses_system_fields and inc_only.uses_system_fields


def test_client_selects_system_fields_and_filters_on_updated_at(src, records):
    session = FakeSession(records)
    client = SocrataClient(page_size=3, session=session)
    out = list(client.fetch(src, since="2025-10-10T14:02:11.000Z"))
    assert out == records
    url, params = session.calls[0]
    assert url == src.endpoint
    assert params["$select"] == ":*, *"
    assert params["$order"] == ":updated_at"
    assert params["$where"] == ":updated_at > '2025-10-10T14:02:11.000Z'"
    assert len(session.calls) == 2  # 3 + 1


def test_client_does_not_select_for_regular_sources(records):
    chicago = load_sources()["chicago_crimes"]
    session = FakeSession(records)
    list(SocrataClient(session=session).fetch(chicago))
    assert "$select" not in session.calls[0][1]


def test_bronze_keys_on_system_id_and_keeps_multi_arrest_incidents(src, records):
    store = BronzeStore(":memory:")
    n = store.write(
        NAME, records, primary_key=src.primary_key, incremental_column=src.incremental_column
    )
    assert n == len(records)
    keys = {r[0] for r in store.con.execute(f"SELECT record_key FROM bronze.{NAME}").fetchall()}
    assert keys == {r[":id"] for r in records}
    # Two arrests share incident U23-000101; keying on :id keeps both.
    latest = store.latest(NAME)
    assert sum(r["incident_number"] == "U23-000101" for r in latest) == 2
    assert store.last_watermark(NAME) == "2025-10-10T14:02:12.000Z"
    store.close()


def test_latest_view_ranks_by_system_updated_at(src, records):
    store = BronzeStore(":memory:")
    kw = {"primary_key": src.primary_key, "incremental_column": src.incremental_column}
    newer = {**records[0], ":updated_at": "2025-11-01T00:00:00.000Z", "arrest_code": "Z"}
    store.write(NAME, [newer], **kw)
    store.write(NAME, records, **kw)  # stale copy of row 0 arrives later
    (row,) = [r for r in store.latest(NAME) if r[":id"] == records[0][":id"]]
    assert row["arrest_code"] == "Z"
    store.close()


def test_schema_snapshot_of_illinois_fixture(src, records):
    store = BronzeStore(":memory:")
    store.write(
        NAME, records, primary_key=src.primary_key, incremental_column=src.incremental_column
    )
    snap = store.schema_snapshot(NAME)
    types = {name: f.inferred_type for name, f in snap.fields.items()}
    assert types[":updated_at"] == "timestamp"
    assert types["date_of_arrest"] == "timestamp"
    assert types["year_of_arrest"] == "integer"
    assert types["incident_number"] == "string"
    store.close()


class _FakeClient:
    def __init__(self, records):
        self.records = records

    def fetch(self, source, since=None, max_records=None):
        assert source.name == NAME
        yield from self.records


def test_cli_ingests_illinois_source(tmp_path, monkeypatch, records, capsys):
    monkeypatch.setattr(cli, "SocrataClient", lambda **kw: _FakeClient(records))
    db = tmp_path / "lh.duckdb"
    assert cli.main(["ingest", NAME, "--db", str(db)]) == 0
    con = duckdb.connect(str(db))
    assert con.execute(f"SELECT count(*) FROM bronze.{NAME}__latest").fetchone()[0] == 4
    con.close()

    assert cli.main(["sources"]) == 0
    out = capsys.readouterr().out
    assert NAME in out and "data.illinois.gov/resource/s2ps-ct5e.json" in out
