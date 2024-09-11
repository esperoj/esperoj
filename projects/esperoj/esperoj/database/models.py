import json
from datetime import datetime
from typing import Annotated, Any
from uuid import uuid4

from pydantic import BaseModel, BeforeValidator, Field, PlainSerializer

from esperoj.database.orm import OrmRecord

# Validator and Serializer for the json field, if there is a day that I use a database that support json type, I'll make this work for both database by access the _database_type of the OrmRecord
JsonFieldConfig = (
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
    PlainSerializer(lambda x: json.dumps(x) if not None else x),
)

ID = Annotated[str, Field(default_factory=lambda: str(uuid4())[:22], min_length=1, max_length=36)]


class MirrorInfo(BaseModel):
    sources: Annotated[list[str], Field(min_length=1)]
    encrypted: bool = False


class Audio(OrmRecord):
    _table_name = "audios"

    id: ID
    title: Annotated[str, Field(min_length=1, max_length=255)]
    comment: str = ""
    files: list[ID] = []
    modified: datetime | None = None
    created: datetime | None = None


class File(OrmRecord):
    _table_name = "files"

    id: ID
    name: Annotated[str, Field(min_length=1, max_length=255)]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    mirrors: Annotated[dict[str, MirrorInfo], *JsonFieldConfig]
    musics: list[ID] = []
    modified: datetime | None = None
    created: datetime | None = None
    metadata: Annotated[dict[Any, Any], *JsonFieldConfig] = {}
    verified: bool = False
