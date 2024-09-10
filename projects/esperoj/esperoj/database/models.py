import json
from datetime import datetime
from typing import Annotated, Any, Optional
from uuid import uuid4

from pydantic import AnyUrl, BaseModel, BeforeValidator, Field, PlainSerializer

JsonFieldConfig = (
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
    PlainSerializer(lambda x: json.dumps(x) if not None else x),
)

ID = Annotated[str, Field(min_length=1, max_length=36)]


class OrmRecord(BaseModel):
    _client: Optional["OrmClient"] = None
    _table_name: str

    def get_client(self) -> "OrmClient":
        return self._private_client

    def set_client(self, client: "OrmClient") -> "OrmClient":
        self._private_client = client
        return self._private_client


class OrmClient:
    pass


def generate_id() -> ID:
    return str(uuid4())[:22]


class MirrorInfo(BaseModel):
    urls: Annotated[list[AnyUrl], Field(min_length=1)]
    is_encrypted: bool = False


file = {
    "name": "hello.pdf",
    "sha256": '"cdd2e94b21ac7cd0f64da62750671ea45daac6b818449e2780a4d43ec16a913',
    "size": 1,
    "metadata": '{"hello":"world"}',
    "mirrors": json.dumps(
        {
            "file_haus": {
                "urls": ['file:///helo"'],
            }
        }
    ),
}


class File(OrmRecord):
    _table_name = "files"

    id: ID = Field(default_factory=generate_id)
    name: Annotated[str, Field(min_length=1, max_length=255)]
    sha256: Annotated[str, Field(min_length=64, max_length=64)]
    size: Annotated[int, Field(gt=0)]
    mirrors: Annotated[dict[str, MirrorInfo], *JsonFieldConfig]
    musics: list[ID] = []
    modified: datetime | None = None
    created: datetime | None = None
    metadata: Annotated[dict[Any, Any], *JsonFieldConfig] = {}
    verified: bool = False


class Test(OrmRecord):
    metadata: Annotated[dict[str, Any], *JsonFieldConfig]


print(Test(**file).set_client(OrmClient()))
print(Test(**file).model_dump())
print(Test(**file).metadata["hello"])
