import pytest
from migretti.io_utils import atomic_write


def test_atomic_write_success(tmp_path):
    target = tmp_path / "test.txt"

    with atomic_write(str(target)) as f:
        f.write("content")

    assert target.exists()
    assert target.read_text(encoding="utf-8") == "content"


def test_atomic_write_failure(tmp_path):
    target = tmp_path / "fail.txt"

    try:
        with atomic_write(str(target)) as f:
            f.write("partial")
            raise RuntimeError("Boom")
    except RuntimeError:
        pass

    assert not target.exists()


def test_atomic_write_exclusive(tmp_path):
    target = tmp_path / "exist.txt"
    target.write_text("old", encoding="utf-8")

    with pytest.raises(FileExistsError):
        with atomic_write(str(target), exclusive=True) as f:
            f.write("new")

    assert target.read_text(encoding="utf-8") == "old"
