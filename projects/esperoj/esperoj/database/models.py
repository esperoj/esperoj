import json
from typing import Annotated, Any  # , TypedDict

from pydantic import BeforeValidator, Field, PlainSerializer
from typing_extensions import TypedDict

from esperoj.database.database import ID, Record

JsonFieldConfig = (
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
    PlainSerializer(lambda v: json.dumps(v) if v is not None else v),
)


class SourceInfo(TypedDict):
    src: str
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    verified: Annotated[bool, Field(default=False)]


class MirrorInfo(TypedDict):
    sources: list[SourceInfo]
    encrypted: Annotated[bool, Field(default=False)]


class Album(Record):
    title: Annotated[str, Field(min_length=1, max_length=255)]
    creator: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    description: str | None = None
    subjects: list[str] | None = None
    collections: list[str] | None = None
    sources: Annotated[list[str], *JsonFieldConfig] | None = None
    files: list[ID] | None = None
    date: str | None = None
    modified: str | None = None
    created: str | None = None


class Song(Record):
    title: Annotated[str, Field(min_length=1, max_length=255)]
    artist: Annotated[str, Field(min_length=1, max_length=255)]
    subjects: list[str] | None = None
    collections: list[str] | None = None
    description: str | None = None
    files: list[ID] | None = None
    modified: str | None = None
    created: str | None = None
    date: str | None = None
    www: str | None = None
    album: str | None = None
    language: str | None = None
    composer: str | None = None


class File(Record):
    name: Annotated[str, Field(min_length=1, max_length=255)]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    mirrors: Annotated[dict[str, MirrorInfo], *JsonFieldConfig]
    musics: list[ID] | None = None
    albums: list[ID] | None = None
    modified: str | None = None
    created: str | None = None
    metadata: Annotated[dict[Any, Any], *JsonFieldConfig] | None = None
    verified: bool | None = False


table_models = {"albums": Album, "songs": Song, "files": File}
