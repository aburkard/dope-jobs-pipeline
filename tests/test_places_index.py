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
