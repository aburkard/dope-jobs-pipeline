from public_ids import derive_company_slug_map, short_public_job_id


def test_short_public_job_id_is_stable_and_compact():
    value = short_public_job_id("greenhouse__scaleai__4655244005")
    assert value == short_public_job_id("greenhouse__scaleai__4655244005")
    assert len(value) == 16
    assert value.islower()


def test_company_slug_prefers_company_identity_over_board_token():
    rows = [
        {
            "ats": "greenhouse",
            "board_token": "scaleai",
            "company_name": "Scale AI",
            "domain": "scale.com",
        },
        {
            "ats": "lever",
            "board_token": "scaleai",
            "company_name": "Scale AI",
            "domain": "scale.com",
        },
    ]

    slug_map = derive_company_slug_map(rows)
    assert slug_map[("greenhouse", "scaleai")] == "scale-ai"
    assert slug_map[("lever", "scaleai")] == "scale-ai"


def test_company_slug_disambiguates_name_collisions():
    rows = [
        {
            "ats": "greenhouse",
            "board_token": "acme1",
            "company_name": "Acme",
            "domain": "acme.com",
        },
        {
            "ats": "lever",
            "board_token": "acme2",
            "company_name": "Acme",
            "domain": "acme.io",
        },
    ]

    slug_map = derive_company_slug_map(rows)
    assert slug_map[("greenhouse", "acme1")] == "acme"
    assert slug_map[("lever", "acme2")] != "acme"
    assert slug_map[("lever", "acme2")].startswith("acme-")
