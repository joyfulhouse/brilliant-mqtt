from brilliant_hue_ca.state import (
    PendingReload,
    clear_pending,
    load_pending,
    save_pending,
)


class FakeFS:
    """Minimal FileSystem for the state round-trip: only the methods state.py
    touches are exercised (exists/read_text/write_text)."""

    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def append_text(self, path: str, text: str) -> None:  # pragma: no cover - unused
        self.files[path] = self.files.get(path, "") + text

    def write_text(self, path: str, text: str) -> None:
        self.files[path] = text

    def glob(self, root: str, name: str) -> str | None:  # pragma: no cover - unused
        return None


def test_load_missing_file_is_no_pending() -> None:
    fs = FakeFS()
    assert load_pending(fs, "/state.json") is None


def test_save_then_load_round_trips() -> None:
    fs = FakeFS()
    pending = PendingReload(bundle_path="/b", fingerprint="abc123", last_attempt_at=1000.0)
    save_pending(fs, "/state.json", pending)
    assert load_pending(fs, "/state.json") == pending


def test_save_overwrites_previous_generation() -> None:
    fs = FakeFS()
    save_pending(fs, "/state.json", PendingReload("/b", "old", 1.0))
    save_pending(fs, "/state.json", PendingReload("/b", "new", 2.0))
    loaded = load_pending(fs, "/state.json")
    assert loaded is not None
    assert loaded.fingerprint == "new"
    assert loaded.last_attempt_at == 2.0


def test_clear_makes_load_return_none() -> None:
    fs = FakeFS()
    save_pending(fs, "/state.json", PendingReload("/b", "abc", 1.0))
    assert load_pending(fs, "/state.json") is not None
    clear_pending(fs, "/state.json")
    assert load_pending(fs, "/state.json") is None


def test_corrupt_file_is_treated_as_no_pending_not_fatal() -> None:
    fs = FakeFS()
    fs.files["/state.json"] = "{not valid json"
    assert load_pending(fs, "/state.json") is None


def test_json_missing_keys_is_no_pending() -> None:
    fs = FakeFS()
    fs.files["/state.json"] = '{"bundle_path": "/b"}'  # no fingerprint / timestamp
    assert load_pending(fs, "/state.json") is None


def test_json_non_object_is_no_pending() -> None:
    fs = FakeFS()
    fs.files["/state.json"] = "[1, 2, 3]"
    assert load_pending(fs, "/state.json") is None
