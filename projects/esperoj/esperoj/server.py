"""Server module."""

from pathlib import Path

import requests
from litestar import Litestar, get
from litestar.response import Stream
from litestar.static_files import create_static_files_router

PUBLIC_DIR = Path("public")


@get("/backup.7z", media_type="application/x-7z-compressed")
async def get_backup() -> Stream:
    url = "https://public.esperoj.eu.org/backup.7z"
    response = requests.get(url, stream=True, timeout=30)
    return Stream(response.iter_content(2**20))


@get("/api/pwd")
async def pwd() -> list[str]:
    cwd = Path.cwd()
    return [item.name for item in cwd.iterdir()]


def on_startup():
    PUBLIC_DIR.mkdir(exist_ok=True)


app = Litestar(
    route_handlers=[
        pwd,
        get_backup,
        create_static_files_router(path="/", directories=[str(PUBLIC_DIR)], html_mode=True),
    ],
    on_startup=[on_startup],
)
