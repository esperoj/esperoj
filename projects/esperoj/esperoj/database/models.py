import json
from datetime import datetime
from re import sub
from typing import Annotated, Any
from uuid import uuid4

from pydantic import AnyUrl, BaseModel, BeforeValidator, Field


def json_to_object(v: str | object):
    if isinstance(v, str):
        return json.loads(v)
    return v


def to_snake(s):
    return "_".join(sub("([A-Z][a-z]+)", r" \1", sub("([A-Z]+)", r" \1", s.replace("-", " "))).split()).lower()


ID = Annotated[str, Field(min_length=1, max_length=36)]


def generate_id() -> ID:
    return str(uuid4())[:22]


class MirrorInfo(BaseModel):
    urls: Annotated[list[AnyUrl], Field(min_length=1)]
    is_encrypted: bool = False


file = {
    "name": "hello.pdf",
    "sha256": "4cdd2e94b21ac7cd0f64da62750671ea45daac6b818449e2780a4d43ec16a913",
    "size": 1,
    "mirrors": json.dumps(
        {
            "file_haus": {
                "urls": ["file:///h"],
            }
        }
    ),
}


class File(BaseModel):
    id: ID = Field(default_factory=generate_id)
    name: Annotated[str, Field(min_length=1, max_length=255)]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    mirrors: Annotated[dict[str, MirrorInfo], BeforeValidator(json_to_object)]
    musics: list[ID] = []
    modified: datetime | None = None
    created: datetime | None = None
    metadata: Annotated[dict[Any, Any], BeforeValidator(json_to_object)] = {}
    verified: bool = False


print(File(**file))
