from places_index import build_place_doc


def test_build_place_doc_locality():
    row = (
        5391959,
        "locality",
        "San Francisco",
        "San Francisco",
        "San Francisco, California, United States",
        "US",
        "United States",
        "CA",
        "California",
        37.77493,
        -122.41942,
        864816,
        "PPL",
        ["san francisco", "san francisco california", "san francisco united states"],
    )
    doc = build_place_doc(row)
    assert doc["id"] == "5391959"
    assert doc["kind_priority"] == 3
    assert doc["admin1_key"] == "US-CA"
    assert doc["supports_radius"] is True
    assert doc["_geo"] == {"lat": 37.77493, "lng": -122.41942}


def test_build_place_doc_country():
    row = (
        6252001,
        "country",
        "United States",
        "United States",
        "United States",
        "US",
        "United States",
        None,
        None,
        None,
        None,
        331002651,
        "PCLI",
        ["united states", "us", "usa"],
    )
    doc = build_place_doc(row)
    assert doc["kind_priority"] == 0
    assert doc["admin1_key"] is None
    assert doc["supports_radius"] is False
    assert "_geo" not in doc


def test_build_place_doc_admin1_prioritizes_before_locality():
    row = (
        6093943,
        "admin1",
        "Ontario",
        "Ontario",
        "Ontario, Canada",
        "CA",
        "Canada",
        "08",
        "Ontario",
        None,
        None,
        None,
        "ADM1",
        ["ontario", "ontario canada"],
    )
    doc = build_place_doc(row)
    assert doc["kind_priority"] == 1


def test_sync_places_index_keeps_id_filterable(monkeypatch):
    # The web app resolves ?loc=<geoname id> with `id = "<id>"` filters; without this every such URL 500s.
    import sys
    import types

    import places_index

    applied = {}

    class FakeIndex:
        def update_filterable_attributes(self, attrs):
            applied["filterable"] = list(attrs)

        def __getattr__(self, name):
            return lambda *a, **k: types.SimpleNamespace(task_uid=1, number_of_documents=0)

    class FakeCursor:
        def execute(self, query, params):
            pass

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fake_index = FakeIndex()
    client = types.SimpleNamespace(index=lambda name: fake_index, wait_for_task=lambda *a, **k: None)
    monkeypatch.setitem(sys.modules, "meilisearch", types.SimpleNamespace(Client=lambda host, key: client))
    places_index.sync_places_index(types.SimpleNamespace(cursor=FakeCursor), "http://meili", "key")
    assert "id" in applied["filterable"]
    assert {"kind", "country_code", "admin1_key", "supports_radius"} <= set(applied["filterable"])
