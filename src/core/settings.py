import os
from pathlib import Path
from typing import (
    Optional,
    Union,
    List,
)

from pydantic_settings import BaseSettings, SettingsConfigDict


_PathLike = Union[os.PathLike[str], str, Path]


def root_dir() -> Path:
    return Path(__file__).resolve().parent.parent.parent 


def path(*paths: _PathLike, base_path: Optional[_PathLike] = None) -> str:
    if base_path is None:
        base_path = root_dir()

    return os.path.join(base_path, *paths)


class RozenkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="./.env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    max_workers: int
    filters: List[str] | None
    sort_list: List[str] | None
    default_parse: bool
    batch_size: int
    save_data: str
    max_retries: int
    request_timeout: int
    min_delay: float
    max_delay: float

class Settings(BaseSettings):

    rozetka: RozenkaSettings


def load_settings(
        rozetka: Optional[RozenkaSettings] = None,

) -> Settings:
    return Settings(
        rozetka=rozetka or RozenkaSettings(),
    )

