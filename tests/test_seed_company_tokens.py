from seed_company_tokens import load_tokens


def test_load_tokens_dedupes_and_skips_comments(tmp_path):
    path = tmp_path / "tokens.txt"
    path.write_text("# comment\nloopme\n\nloopme\ntelegraph\n")

    assert load_tokens(path) == ["loopme", "telegraph"]
