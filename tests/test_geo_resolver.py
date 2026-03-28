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


def test_dedupe_resolved_locations_by_identical_label_prefers_richer_entry():
    richer = {
        "label": "Kuala Lumpur, Malaysia",
        "city": "Kuala Lumpur",
        "state": "Kuala Lumpur",
        "country_code": "MY",
        "geoname_id": 1735161,
        "lat": 3.1412,
        "lng": 101.68653,
    }
    poorer = {
        "label": "Kuala Lumpur, Malaysia",
        "city": "Kuala Lumpur",
        "state": None,
        "country_code": "Malaysia",
        "geoname_id": None,
        "lat": None,
        "lng": None,
    }

    assert _dedupe_resolved_locations([poorer, richer]) == [richer]
