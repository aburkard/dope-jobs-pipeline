from probe_workable_widget import classify_response, extract_title, load_tokens


class DummyResponse:
    def __init__(self, status_code=200, headers=None, text="", json_data=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self._json_data = json_data

    def json(self):
        if self._json_data is None:
            raise ValueError("not json")
        return self._json_data


def test_load_tokens_skips_comments_and_dupes(tmp_path):
    path = tmp_path / "workable.txt"
    path.write_text("# comment\nloopme\n\nloopme\ntelegraph\n")

    assert load_tokens(path) == ["loopme", "telegraph"]


def test_extract_title():
    assert extract_title("<html><title>Security challenge</title></html>") == "Security challenge"


def test_classify_ok_json():
    response = DummyResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        json_data={"name": "LoopMe", "jobs": [1, 2, 3]},
    )

    classification, details = classify_response(response)
    assert classification == "ok"
    assert details["job_count"] == 3
    assert details["name"] == "LoopMe"


def test_classify_cloudflare_challenge():
    response = DummyResponse(
        status_code=403,
        headers={"cf-mitigated": "challenge", "content-type": "text/html"},
        text="<html><title>Security challenge</title></html>",
    )

    classification, details = classify_response(response)
    assert classification == "challenge"
    assert details["title"] == "Security challenge"
