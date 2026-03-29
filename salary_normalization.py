from __future__ import annotations

from datetime import date
from xml.etree import ElementTree
import re


ECB_DAILY_XML_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

ANNUALIZATION_FACTORS: dict[str, float] = {
    "hourly": 2080.0,
    "weekly": 52.0,
    "monthly": 12.0,
    "annually": 1.0,
}


def normalize_currency_code(currency_code: str | None) -> str | None:
    value = (currency_code or "").strip().upper()
    if not value:
        return None
    if not re.fullmatch(r"[A-Z]{3}", value):
        return None
    return value


def annualize_amount(amount: float | int | None, period: str | None) -> float | None:
    if amount is None:
        return None
    factor = ANNUALIZATION_FACTORS.get((period or "").strip().lower())
    if factor is None:
        return None
    return float(amount) * factor


def parse_ecb_daily_xml(xml_text: str) -> tuple[date, dict[str, float]]:
    root = ElementTree.fromstring(xml_text)

    dated_cube = None
    for element in root.iter():
        if element.tag.endswith("Cube") and "time" in element.attrib:
            dated_cube = element
            break

    if dated_cube is None:
        raise ValueError("ECB XML did not contain a dated Cube element")

    as_of_date = date.fromisoformat(dated_cube.attrib["time"])
    eur_quotes: dict[str, float] = {"EUR": 1.0}

    for child in dated_cube:
        currency = normalize_currency_code(child.attrib.get("currency"))
        rate = child.attrib.get("rate")
        if not currency or not rate:
            continue
        eur_quotes[currency] = float(rate)

    if "USD" not in eur_quotes:
        raise ValueError("ECB XML did not include USD quote")

    return as_of_date, eur_quotes


def compute_usd_per_unit_rates(eur_quotes: dict[str, float]) -> dict[str, float]:
    usd_per_eur = eur_quotes["USD"]
    usd_per_unit: dict[str, float] = {"USD": 1.0, "EUR": usd_per_eur}

    for currency_code, units_per_eur in eur_quotes.items():
        if currency_code in {"USD", "EUR"}:
            continue
        usd_per_unit[currency_code] = usd_per_eur / units_per_eur

    return usd_per_unit


def normalize_salary_annual_usd(salary: dict | None, fx_rates: dict[str, float]) -> dict[str, float | int | str] | None:
    if not isinstance(salary, dict):
        return None

    currency_code = normalize_currency_code(salary.get("currency"))
    period = (salary.get("period") or "").strip().lower() or "annually"
    fx_rate = fx_rates.get(currency_code) if currency_code else None
    if fx_rate is None:
        return None

    raw_min = salary.get("min")
    raw_max = salary.get("max")
    annual_min = annualize_amount(raw_min, period) if raw_min not in (None, 0) else None
    annual_max = annualize_amount(raw_max, period) if raw_max not in (None, 0) else None

    if annual_min is None and annual_max is None:
        return None
    if annual_min is None:
        annual_min = annual_max
    if annual_max is None:
        annual_max = annual_min
    if annual_min is None or annual_max is None:
        return None

    min_usd = round(annual_min * fx_rate)
    max_usd = round(annual_max * fx_rate)
    if min_usd > max_usd:
        min_usd, max_usd = max_usd, min_usd

    return {
        "salary_annual_min_usd": min_usd,
        "salary_annual_max_usd": max_usd,
        "salary_fx_currency": currency_code,
        "salary_fx_usd_per_unit": fx_rate,
    }
