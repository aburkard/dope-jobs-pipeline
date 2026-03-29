from salary_normalization import (
    compute_usd_per_unit_rates,
    normalize_salary_annual_usd,
    parse_ecb_daily_xml,
)


ECB_XML_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2026-03-27">
      <Cube currency="USD" rate="1.0812"/>
      <Cube currency="CAD" rate="1.4701"/>
      <Cube currency="JPY" rate="161.33"/>
    </Cube>
  </Cube>
</gesmes:Envelope>
"""


def test_parse_ecb_daily_xml_extracts_date_and_quotes():
    as_of_date, eur_quotes = parse_ecb_daily_xml(ECB_XML_FIXTURE)

    assert as_of_date.isoformat() == "2026-03-27"
    assert eur_quotes["EUR"] == 1.0
    assert eur_quotes["USD"] == 1.0812
    assert eur_quotes["CAD"] == 1.4701


def test_compute_usd_per_unit_rates_uses_usd_cross_rate():
    usd_rates = compute_usd_per_unit_rates({
        "EUR": 1.0,
        "USD": 1.0812,
        "CAD": 1.4701,
    })

    assert usd_rates["USD"] == 1.0
    assert usd_rates["EUR"] == 1.0812
    assert round(usd_rates["CAD"], 6) == round(1.0812 / 1.4701, 6)


def test_normalize_salary_annual_usd_annualizes_hourly_ranges():
    normalized = normalize_salary_annual_usd(
        {"min": 50, "max": 75, "currency": "CAD", "period": "hourly"},
        {"CAD": 0.74},
    )

    assert normalized == {
        "salary_annual_min_usd": 76960,
        "salary_annual_max_usd": 115440,
        "salary_fx_currency": "CAD",
        "salary_fx_usd_per_unit": 0.74,
    }


def test_normalize_salary_annual_usd_handles_single_point_salary():
    normalized = normalize_salary_annual_usd(
        {"min": 120000, "max": None, "currency": "EUR", "period": "annually"},
        {"EUR": 1.08},
    )

    assert normalized == {
        "salary_annual_min_usd": 129600,
        "salary_annual_max_usd": 129600,
        "salary_fx_currency": "EUR",
        "salary_fx_usd_per_unit": 1.08,
    }


def test_normalize_salary_annual_usd_rejects_invalid_currency_codes():
    assert normalize_salary_annual_usd(
        {"min": 100000, "max": 150000, "currency": "USD/CAD", "period": "annually"},
        {"USD": 1.0},
    ) is None
