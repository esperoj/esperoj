import json
from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Field, PlainSerializer

from esperoj.database.database import ID, Record

JsonFieldConfig = (
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
    PlainSerializer(lambda x: json.dumps(x) if not None else x),
)


class MirrorInfo(BaseModel):
    sources: Annotated[list[str], Field(min_length=1)]
    encrypted: bool = False


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
