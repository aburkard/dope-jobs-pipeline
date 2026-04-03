from job_recommendations import (
    build_location_filter_passes,
    build_similar_filter_passes,
    get_access_headers,
    normalize_string_list,
)


def test_normalize_string_list_dedupes_and_trims():
    assert normalize_string_list([" US ", "", "US", None, "CA"]) == ["US", "CA"]


def test_build_location_filter_passes_for_remote_uses_applicant_countries():
    parsed = {
        "office_type": "remote",
        "applicant_location_requirements": [
            {"country_code": "US"},
            {"country_code": "CA"},
            {"country_code": "US"},
        ],
    }

    assert build_location_filter_passes(parsed) == [
        ['(applicant_country_codes = "US" OR applicant_country_codes = "CA")'],
        [],
    ]


def test_build_location_filter_passes_for_onsite_prefers_geo_then_country():
    parsed = {
        "office_type": "onsite",
        "locations": [
            {"geoname_id": 5391959, "country_code": "US"},
            {"geoname_id": 5128581, "country_code": "US"},
        ],
    }

    assert build_location_filter_passes(parsed) == [
        ["(work_geoname_ids = 5128581 OR work_geoname_ids = 5391959)"],
        ['(work_country_codes = "US")'],
        [],
    ]


def test_build_similar_filter_passes_combines_exact_filters_and_location_backoffs():
    parsed = {
        "job_type": "full-time",
        "office_type": "hybrid",
        "experience_level": "senior",
        "locations": [{"country_code": "US"}],
    }

    assert build_similar_filter_passes(parsed) == [
        'job_type = "full-time" AND experience_level = "senior" AND office_type = "hybrid" AND (work_country_codes = "US")',
        'job_type = "full-time" AND experience_level = "senior" AND office_type = "hybrid"',
    ]


def test_get_access_headers_only_includes_present_values():
    assert get_access_headers("client-id", None) == {
        "CF-Access-Client-Id": "client-id",
    }
    assert get_access_headers("client-id", "client-secret") == {
        "CF-Access-Client-Id": "client-id",
        "CF-Access-Client-Secret": "client-secret",
    }
