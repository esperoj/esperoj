from pathlib import Path

from litestar import Litestar
from litestar.static_files import create_static_files_router
from os import getenv
import sys

scripts_folder = Path.home() / "esperoj-scripts"
if getenv("ESPEROJ_SCRIPTS_FOLDER"):
    scripts_folder = Path(getenv("ESPEROJ_SCRIPTS_FOLDER"))
sys.path.append(str(scripts_folder))

PUBLIC_DIR = Path("public")


def on_startup():
    PUBLIC_DIR.mkdir(exist_ok=True)


app = Litestar(
    route_handlers=[
        create_static_files_router(path="/", directories=[str(PUBLIC_DIR)], html_mode=True),
    ],
    on_startup=[on_startup],
)
