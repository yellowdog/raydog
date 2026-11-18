from __future__ import annotations

from pathlib import Path

import pytest

from yellowdog_ray.credentials import Credentials, load_credentials
from yellowdog_ray.errors import CredentialsError

DEFAULT_URL = "https://api.yellowdog.ai"


def write(path: Path, **values: str) -> Path:
    path.write_text("".join(f"{k}={v}\n" for k, v in values.items()))
    return path


@pytest.fixture(autouse=True)
def empty_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run in an empty directory so no stray .env is picked up."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_explicit_arguments_win() -> None:
    env = {"YD_API_KEY_ID": "env-id", "YD_API_KEY_SECRET": "env-secret"}
    creds = load_credentials("arg-id", "arg-secret", "https://x", environ=env)
    assert creds == Credentials("arg-id", "arg-secret", "https://x")


def test_environment_is_used_and_url_defaults() -> None:
    env = {"YD_API_KEY_ID": "env-id", "YD_API_KEY_SECRET": "env-secret"}
    assert load_credentials(environ=env) == Credentials(
        "env-id", "env-secret", DEFAULT_URL
    )


def test_environment_beats_env_file(tmp_path: Path) -> None:
    env_file = write(
        tmp_path / "my.env", YD_API_KEY_ID="file-id", YD_API_KEY_SECRET="file-secret",
        YD_API_URL="https://file",
    )  # fmt: skip
    creds = load_credentials(environ={"YD_API_KEY_ID": "env-id"}, env_file=env_file)
    assert creds == Credentials("env-id", "file-secret", "https://file")


def test_nearest_dotenv_above_the_working_directory_is_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path / ".env", YD_API_KEY_ID="up-id", YD_API_KEY_SECRET="up-secret")
    below = tmp_path / "a" / "b"
    below.mkdir(parents=True)
    monkeypatch.chdir(below)
    assert load_credentials(environ={}).key_id == "up-id"


def test_credentials_file_is_the_last_resort(tmp_path: Path) -> None:
    cred_file = write(
        tmp_path / "creds", YD_API_KEY_ID="cf-id", YD_API_KEY_SECRET="cf-secret"
    )
    env_file = write(tmp_path / "my.env", YD_API_KEY_ID="file-id")
    creds = load_credentials(environ={}, env_file=env_file, credentials_file=cred_file)
    assert creds == Credentials("file-id", "cf-secret", DEFAULT_URL)


def test_empty_values_count_as_unset(tmp_path: Path) -> None:
    cred_file = write(tmp_path / "creds", YD_API_KEY_ID="cf-id", YD_API_KEY_SECRET="s")
    creds = load_credentials(
        "", environ={"YD_API_KEY_ID": "", "YD_API_URL": ""}, credentials_file=cred_file
    )
    assert creds == Credentials("cf-id", "s", DEFAULT_URL)


def test_missing_values_are_named() -> None:
    with pytest.raises(CredentialsError) as caught:
        load_credentials(environ={"YD_API_KEY_ID": "only-id"})
    assert "YD_API_KEY_SECRET" in str(caught.value)
    assert "YD_API_KEY_ID" not in str(caught.value)
    with pytest.raises(CredentialsError) as caught:
        load_credentials(environ={})
    assert "YD_API_KEY_ID" in str(caught.value) and "YD_API_KEY_SECRET" in str(
        caught.value
    )


@pytest.mark.parametrize("argument", ["env_file", "credentials_file"])
def test_a_named_file_must_exist(tmp_path: Path, argument: str) -> None:
    env = {"YD_API_KEY_ID": "i", "YD_API_KEY_SECRET": "s"}
    with pytest.raises(CredentialsError, match="nope"):
        load_credentials(environ=env, **{argument: tmp_path / "nope"})  # type: ignore[arg-type]


def test_the_secret_is_never_shown() -> None:
    creds = Credentials("the-id", "the-secret")
    assert "the-secret" not in repr(creds) and "the-secret" not in str(creds)
    assert "the-id" in repr(creds)


# --- added after the phase 3 review ------------------------------------------------


def test_url_is_never_taken_from_a_weaker_source_than_the_secret(
    tmp_path: Path,
) -> None:
    """A key given explicitly must not be sent to a URL found in some stray file."""
    env_file = write(tmp_path / "my.env", YD_API_URL="https://stray.example")
    creds = load_credentials("id", "secret", environ={}, env_file=env_file)
    assert creds.url == DEFAULT_URL
    creds = load_credentials(
        "id", environ={"YD_API_KEY_SECRET": "s"}, env_file=env_file
    )
    assert creds.url == DEFAULT_URL
    # ... but a stronger or equal source is fine
    cred_file = write(tmp_path / "creds", YD_API_KEY_SECRET="s", YD_API_KEY_ID="i")
    creds = load_credentials(
        environ={"YD_API_URL": "https://mine.example"}, credentials_file=cred_file
    )
    assert creds.url == "https://mine.example"
    both = write(
        tmp_path / "both", YD_API_KEY_SECRET="s", YD_API_URL="https://f.example"
    )
    assert load_credentials("i", environ={}, env_file=both).url == "https://f.example"


def test_values_in_files_are_taken_literally(tmp_path: Path) -> None:
    env_file = write(
        tmp_path / "my.env", YD_API_KEY_ID="i", YD_API_KEY_SECRET="pa${YD_API_KEY_ID}ss"
    )
    assert (
        load_credentials(environ={}, env_file=env_file).key_secret
        == "pa${YD_API_KEY_ID}ss"
    )
