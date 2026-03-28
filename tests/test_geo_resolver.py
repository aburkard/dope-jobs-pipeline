from geo_resolver import _dedupe_resolved_locations


def test_dedupe_resolved_locations_by_geoname_id():
    locations = [
        {
            "label": "Austin, Texas, United States",
            "city": "Austin",
            "state": "Texas",
            "country_code": "US",
            "geoname_id": 4671654,
            "lat": 30.26715,
            "lng": -97.74306,
        },
        {
            "label": "Austin, Texas, United States",
            "city": "Austin",
            "state": "Texas",
            "country_code": "US",
            "geoname_id": 4671654,
            "lat": 30.26715,
            "lng": -97.74306,
        },
    ]

    assert _dedupe_resolved_locations(locations) == [locations[0]]


def test_dedupe_resolved_locations_by_normalized_structure_without_geoname():
    locations = [
        {
            "label": "Mountain View, CA",
            "city": "Mountain View",
            "state": "CA",
            "country_code": None,
            "geoname_id": None,
        },
        {
            "label": "mountain view, ca",
            "city": "Mountain View",
            "state": "CA",
            "country_code": None,
            "geoname_id": None,
        },
    ]

    assert _dedupe_resolved_locations(locations) == [locations[0]]
