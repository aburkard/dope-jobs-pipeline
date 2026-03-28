from geo_places import (
    build_admin1_row,
    build_city_row,
    build_country_row,
    build_display_name,
    build_search_names,
    normalize_geo_text,
)


def test_normalize_geo_text_strips_accents_and_punctuation():
    assert normalize_geo_text('São Paulo, Brazil') == 'sao paulo brazil'


def test_build_display_name_collapses_duplicate_admin1():
    assert build_display_name('Tokyo', country_name='Japan', admin1_name='Tokyo') == 'Tokyo, Japan'


def test_build_search_names_keeps_structured_variant_for_matching_old_labels():
    search_names = build_search_names(
        'Tokyo',
        'Tokyo',
        'Tokyo, Japan',
        country_code='JP',
        country_name='Japan',
        admin1_code='40',
        admin1_name='Tokyo',
    )
    assert 'tokyo japan' in search_names
    assert 'tokyo tokyo jp' in search_names


def test_build_country_row():
    fields = ['JP', 'JPN', '392', 'JA', 'Japan', 'Tokyo', '377835', '127288000', 'AS', '.jp', 'JPY', 'Yen', '81', '', '', 'ja', '1861060', '', '']
    row = build_country_row(fields)
    assert row['geoname_id'] == 1861060
    assert row['kind'] == 'country'
    assert row['country_code'] == 'JP'
    assert row['display_name'] == 'Japan'


def test_build_admin1_row():
    row = build_admin1_row(['US.CA', 'California', 'California', '5332921'], {'US': 'United States'})
    assert row['kind'] == 'admin1'
    assert row['country_code'] == 'US'
    assert row['admin1_code'] == 'CA'
    assert row['display_name'] == 'California, United States'


def test_build_city_row_uses_admin1_and_country_context():
    fields = [
        '5391959', 'San Francisco', 'San Francisco', 'SF',
        '37.77493', '-122.41942', 'P', 'PPLA2', 'US', '', 'CA', '', '', '',
        '864816', '', '', 'America/Los_Angeles', '2026-01-01',
    ]
    row = build_city_row(fields, {'US': 'United States'}, {('US', 'CA'): 'California'})
    assert row['kind'] == 'locality'
    assert row['country_code'] == 'US'
    assert row['admin1_name'] == 'California'
    assert row['display_name'] == 'San Francisco, California, United States'
    assert 'san francisco ca us' in row['search_names']


def test_build_search_names_adds_common_country_aliases():
    search_names = build_search_names('Mountain View', 'Mountain View', 'Mountain View, California, United States', country_code='US', country_name='United States', admin1_code='CA', admin1_name='California')
    assert 'mountain view ca' in search_names
    assert 'mountain view ca usa' in search_names


def test_build_city_row_uses_alternate_names_for_matching():
    fields = [
        '2267057', 'Lisbon', 'Lisbon', 'Lisboa,Lisbonne',
        '38.71667', '-9.13333', 'P', 'PPLC', 'PT', '', '14', '', '', '',
        '517802', '', '', 'Europe/Lisbon', '2026-01-01',
    ]
    row = build_city_row(fields, {'PT': 'Portugal'}, {('PT', '14'): 'Lisbon'})
    assert 'lisboa' in row['search_names']
