"""YellowDog credentials: one loader for every way RayDog is used."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values, find_dotenv

from yellowdog_ray.errors import CredentialsError

KEY_ID_VAR = "YD_API_KEY_ID"
KEY_SECRET_VAR = "YD_API_KEY_SECRET"
URL_VAR = "YD_API_URL"
DEFAULT_URL = "https://api.yellowdog.ai"


@dataclass(frozen=True)
class Credentials:
    """A YellowDog application key and the platform it belongs to."""

    key_id: str
    key_secret: str = field(repr=False)
    url: str = DEFAULT_URL


def load_credentials(
    key_id: str | None = None,
    key_secret: str | None = None,
    url: str | None = None,
    *,
    env_file: str | os.PathLike[str] | None = None,
    credentials_file: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Credentials:
    """
    Find credentials, taking each value from the first place that has it.

    In order: the arguments; the environment (``YD_API_KEY_ID``,
    ``YD_API_KEY_SECRET``, ``YD_API_URL``); ``env_file``, or failing that the nearest
    ``.env`` at or above the working directory; ``credentials_file``. The files hold
    ``NAME=value`` lines, taken literally. An empty value counts as not set. The URL
    is only taken from the place the secret came from, or one ahead of it.
    """
    sources: list[Mapping[str, str | None]] = [
        {KEY_ID_VAR: key_id, KEY_SECRET_VAR: key_secret, URL_VAR: url},
        os.environ if environ is None else environ,
    ]
    if env_file is not None:
        sources.append(_read(env_file, "env_file"))
    else:
        nearest = find_dotenv(usecwd=True)
        if nearest:
            sources.append(dotenv_values(nearest, interpolate=False))
    if credentials_file is not None:
        sources.append(_read(credentials_file, "credentials_file"))

    def first(name: str, within: int | None = None) -> str | None:
        return next((s[name] for s in sources[:within] if s.get(name)), None)

    found_id, found_secret = first(KEY_ID_VAR), first(KEY_SECRET_VAR)
    missing = [
        name
        for name, value in ((KEY_ID_VAR, found_id), (KEY_SECRET_VAR, found_secret))
        if not value
    ]
    if missing or found_id is None or found_secret is None:
        raise CredentialsError(
            "YellowDog credentials not found: set "
            + " and ".join(missing)
            + " in the environment or in a .env file"
        )
    # The URL decides where the secret is sent, so it is never taken from a weaker
    # source than the secret itself: a key given explicitly is not sent to a URL
    # that happens to be in some .env file.
    secret_source = next(i for i, s in enumerate(sources) if s.get(KEY_SECRET_VAR))
    found_url = first(URL_VAR, within=secret_source + 1)
    return Credentials(found_id, found_secret, found_url or DEFAULT_URL)


def _read(path: str | os.PathLike[str], what: str) -> Mapping[str, str | None]:
    if not Path(path).is_file():
        raise CredentialsError(f"{what} not found: {os.fspath(path)}")
    return dotenv_values(path, interpolate=False)
