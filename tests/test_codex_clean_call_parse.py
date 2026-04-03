import codex_clean_call_parse as parse_mod


class DummyModule:
    def __init__(self, responses):
        self.responses = list(responses)
        self.auth_calls = 0
        self.stream_calls = 0

    def ensure_fresh_auth(self, auth_path):
        self.auth_calls += 1
        return {"ok": True}

    def stream_call(self, auth_path, auth, payload):
        self.stream_calls += 1
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_call_codex_retries_rate_limit_then_succeeds(monkeypatch):
    module = DummyModule(
        [
            RuntimeError('HTTP 429: {"detail":"Rate limit exceeded"}'),
            RuntimeError('HTTP 429: {"detail":"Rate limit exceeded"}'),
            {"text": "{}"},
        ]
    )
    sleeps = []
    monkeypatch.setattr(parse_mod.time, "sleep", sleeps.append)

    result = parse_mod._call_codex(module, auth_path="auth.json", payload={"x": 1})

    assert result == {"text": "{}"}
    assert module.stream_calls == 3
    assert sleeps == [2.0, 4.0]


def test_call_codex_does_not_retry_non_rate_limit(monkeypatch):
    module = DummyModule([RuntimeError("HTTP 500: boom")])
    sleeps = []
    monkeypatch.setattr(parse_mod.time, "sleep", sleeps.append)

    try:
        parse_mod._call_codex(module, auth_path="auth.json", payload={"x": 1})
    except RuntimeError as exc:
        assert str(exc) == "HTTP 500: boom"
    else:
        raise AssertionError("expected RuntimeError")

    assert module.stream_calls == 1
    assert sleeps == []


def test_is_rate_limit_error_matches_known_messages():
    assert parse_mod._is_rate_limit_error(RuntimeError('HTTP 429: {"detail":"Rate limit exceeded"}'))
    assert parse_mod._is_rate_limit_error(RuntimeError("Rate limit exceeded"))
    assert not parse_mod._is_rate_limit_error(RuntimeError("HTTP 500: boom"))
