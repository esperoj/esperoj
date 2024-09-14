import json
from datetime import datetime
from typing import Annotated, Any, TypedDict

from pydantic import BeforeValidator, Field, PlainSerializer

from esperoj.database.database import ID, Record

JsonFieldConfig = (
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
    PlainSerializer(lambda v: json.dumps(v) if not None else v),
)


class MirrorInfo(TypedDict):
    sources: Annotated[list[str], Field(min_length=1)]
    encrypted: Annotated[bool, Field(default=False)]


class Music(Record):
    title: Annotated[str, Field(min_length=1, max_length=255)]
    comment: str = ""
    files: list[ID] = []
    modified: datetime | None = None
    created: datetime | None = None


class File(Record):
    name: Annotated[str, Field(min_length=1, max_length=255)]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    mirrors: Annotated[dict[str, MirrorInfo], *JsonFieldConfig]
    musics: list[ID] = []
    modified: datetime | None = None
    created: datetime | None = None
    metadata: Annotated[dict[Any, Any], *JsonFieldConfig] = {}
    verified: bool = False


table_models = {"musics": Music, "files": File}
