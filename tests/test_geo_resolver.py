from geo_resolver import GeoResolver


class StubGeoResolver(GeoResolver):
    def __init__(self, matches):
        self.conn = None
        self._cache = {}
        self.matches = matches

    def _lookup_candidate(self, candidate: str, *, kind: str | None = None,
                          country_code: str | None = None):
        return self.matches.get((candidate, kind, country_code))


def test_resolve_work_location_falls_back_to_city_country():
    resolver = StubGeoResolver({
        ("Toronto, CA", "locality", "CA"): {
            "geoname_id": 6167865,
            "kind": "locality",
            "canonical_name": "Toronto",
            "display_name": "Toronto, Ontario, Canada",
            "country_code": "CA",
            "country_name": "Canada",
            "admin1_code": "08",
            "admin1_name": "Ontario",
            "latitude": 43.70011,
            "longitude": -79.4163,
            "population": 2731571,
        }
    })
    resolved = resolver.resolve_work_location({
        "label": "Toronto, ON, Canada",
        "city": "Toronto",
        "state": "ON",
        "country_code": "CA",
    })
    assert resolved["geoname_id"] == 6167865
    assert resolved["label"] == "Toronto, Ontario, Canada"
    assert resolved["state"] == "Ontario"
    assert resolved["lat"] == 43.70011


def test_resolve_work_location_normalizes_country_names_before_lookup():
    resolver = StubGeoResolver({
        ("San Mateo, CA, US", "locality", "US"): {
            "geoname_id": 5392423,
            "kind": "locality",
            "canonical_name": "San Mateo",
            "display_name": "San Mateo, California, United States",
            "country_code": "US",
            "country_name": "United States",
            "admin1_code": "CA",
            "admin1_name": "California",
            "latitude": 37.563,
            "longitude": -122.3255,
            "population": 97207,
        }
    })
    resolved = resolver.resolve_work_location({
        "city": "San Mateo",
        "state": "CA",
        "country_code": "United States",
    })
    assert resolved["geoname_id"] == 5392423
    assert resolved["country_code"] == "US"
    assert resolved["label"] == "San Mateo, California, United States"


def test_resolve_work_location_drops_remote_pseudo_locations():
    resolver = StubGeoResolver({})
    assert resolver.resolve_work_location({"label": "Remote, USA"}, office_type="remote") is None


def test_resolve_work_location_country_only():
    resolver = StubGeoResolver({
        ("DE", "country", None): {
            "geoname_id": 2921044,
            "kind": "country",
            "canonical_name": "Germany",
            "display_name": "Germany",
            "country_code": "DE",
            "country_name": "Germany",
            "admin1_code": None,
            "admin1_name": None,
            "latitude": None,
            "longitude": None,
            "population": 83019213,
        }
    })
    resolved = resolver.resolve_work_location({"label": "DE", "country_code": "DE"})
    assert resolved["geoname_id"] == 2921044
    assert resolved["label"] == "Germany"
    assert resolved["country_code"] == "DE"


def test_resolve_applicant_requirement_country():
    resolver = StubGeoResolver({
        ("USA", "country", None): {
            "geoname_id": 6252001,
            "kind": "country",
            "canonical_name": "United States",
            "display_name": "United States",
            "country_code": "US",
            "country_name": "United States",
            "admin1_code": None,
            "admin1_name": None,
            "latitude": None,
            "longitude": None,
            "population": 327167434,
        }
    })
    resolved = resolver.resolve_applicant_requirement({
        "scope": "country",
        "name": "USA",
        "country_code": "US",
        "region": None,
    })
    assert resolved["geoname_id"] == 6252001
    assert resolved["name"] == "United States"
    assert resolved["country_code"] == "US"


def test_resolve_parsed_geo_updates_locations_and_requirements():
    resolver = StubGeoResolver({
        ("Berlin, DE", "locality", "DE"): {
            "geoname_id": 2950159,
            "kind": "locality",
            "canonical_name": "Berlin",
            "display_name": "Berlin, Berlin, Germany",
            "country_code": "DE",
            "country_name": "Germany",
            "admin1_code": "16",
            "admin1_name": "Berlin",
            "latitude": 52.52437,
            "longitude": 13.41053,
            "population": 3426354,
        },
        ("Germany", "country", None): {
            "geoname_id": 2921044,
            "kind": "country",
            "canonical_name": "Germany",
            "display_name": "Germany",
            "country_code": "DE",
            "country_name": "Germany",
            "admin1_code": None,
            "admin1_name": None,
            "latitude": None,
            "longitude": None,
            "population": 83019213,
        },
    })
    parsed = resolver.resolve_parsed_geo({
        "office_type": "hybrid",
        "locations": [{"label": "Berlin, DE", "city": "Berlin", "country_code": "DE"}],
        "applicant_location_requirements": [{"scope": "country", "name": "Germany", "country_code": "DE", "region": None}],
    })
    assert parsed["locations"][0]["geoname_id"] == 2950159
    assert parsed["applicant_location_requirements"][0]["geoname_id"] == 2921044
