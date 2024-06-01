import sys
from os import getenv
from pathlib import Path

import requests
from litestar import Litestar, get
from litestar.response import File
from litestar.static_files import create_static_files_router

from esperoj.esperoj import EsperojFactory

scripts_folder = Path.home() / "esperoj-scripts"
if getenv("ESPEROJ_SCRIPTS_FOLDER"):
    scripts_folder = Path(getenv("ESPEROJ_SCRIPTS_FOLDER"))
sys.path.append(str(scripts_folder))

PUBLIC_DIR = Path("public")

config_file = getenv("ESPEROJ_CONFIG_FILE", "")
esperoj = EsperojFactory.create(config_file)


@get("/backup.7z", media_type="application/x-7z-compressed")
async def get_backup() -> File:
    file_path = "/tmp/backup.7z"
    url = "https://public.esperoj.eu.org/backup.7z"
    response = requests.get(url)
    Path(file_path).write_bytes(response.content)
    return File(
        path=file_path,
        filename="backup.7z",
    )


def on_startup():
    PUBLIC_DIR.mkdir(exist_ok=True)


app = Litestar(
    route_handlers=[
        get_backup,
        create_static_files_router(path="/", directories=[str(PUBLIC_DIR)], html_mode=True),
    ],
    on_startup=[on_startup],
)
