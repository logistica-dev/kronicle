# tests/unit/utils/test_file_utils.py
from hashlib import md5, sha256, sha512
from pathlib import Path

import pytest

from kronicle.utils import file_utils


def test_is_dir_and_check_is_dir(tmp_path):
    d = tmp_path / "mydir"
    d.mkdir()
    assert file_utils.is_dir(d) is True
    assert file_utils.check_is_dir(d) == d

    # check error for non-existent dir
    with pytest.raises(FileNotFoundError):
        file_utils.check_is_dir(d / "nonexistent", "custom error")


def test_make_dir(tmp_path):
    new_dir = tmp_path / "new_dir"
    assert file_utils.make_dir(new_dir) == new_dir
    assert new_dir.exists()
    # calling again should not raise
    assert file_utils.make_dir(new_dir) == new_dir

    # if a file exists at the path, should raise
    f = tmp_path / "afile"
    f.write_text("data")
    with pytest.raises(FileNotFoundError):
        file_utils.make_dir(f)


def test_exists_file_and_is_file(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("hello")
    assert file_utils.exists_file(f) is True
    assert file_utils.is_file(f) is True

    # non-existent file
    assert file_utils.exists_file(tmp_path / "nofile") is False
    assert file_utils.is_file(tmp_path / "nofile") is False


def test_check_is_file(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("content")
    assert file_utils.check_is_file(f) == f

    with pytest.raises(FileNotFoundError):
        file_utils.check_is_file(tmp_path / "nofile", "err")


def test_expand_file_path(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("x")
    expanded = file_utils.expand_file_path(str(f))
    assert str(f) in expanded
    assert Path(expanded).is_absolute()


def test_get_file_size(tmp_path):
    f = tmp_path / "file.txt"
    content = "12345"
    f.write_text(content)
    assert file_utils.get_file_size(f) == len(content)


@pytest.mark.parametrize("algo", ["md5", "sha256", "sha512", "SHA-256", "SHA512"])
def test_get_file_hash(tmp_path, algo):
    f = tmp_path / "file.txt"
    f.write_text("test content")
    result = file_utils.get_file_hash(f, algo)
    # Verify against hashlib directly
    content = f.read_bytes()
    if algo.upper() == "MD5":
        expected = md5(content).hexdigest()
    elif algo.upper() in ("SHA256", "SHA-256"):
        expected = sha256(content).hexdigest()
    else:
        expected = sha512(content).hexdigest()
    assert result == expected

    # invalid algo
    with pytest.raises(ValueError):
        file_utils.get_file_hash(f, "invalid")


def test_read_and_write_json_file(tmp_path):
    f = tmp_path / "file.json"
    data = {"a": 1, "b": 2}
    file_utils.write_json_file(f, data)
    loaded = file_utils.read_json_file(f)
    assert loaded == data


def test_read_and_write_file(tmp_path):
    f = tmp_path / "file.txt"
    content = "Hello World"
    file_utils.write_file(f, content)
    read_content = f.read_text()
    assert read_content == content


def test_read_ini_conf(tmp_path):
    ini_file = tmp_path / "config.ini"
    ini_file.write_text("[section]\nkey = value\n")
    config = file_utils.read_ini_conf(ini_file)
    assert config.get("section", "key") == "value"


def test_load_ini_file(tmp_path):
    ini_file = tmp_path / "config.ini"
    ini_file.write_text("[main]\nfoo = bar\n")
    config = file_utils.load_ini_file(ini_file)
    assert config.get("main", "foo") == "bar"
