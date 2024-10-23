"""Server module."""

import requests
from litestar import Litestar, get
from litestar.response import Stream


@get("/backup.7z", media_type="application/x-7z-compressed")
async def get_backup() -> Stream:
    url = "https://public.esperoj.eu.org/backup.7z"
    response = requests.get(url, stream=True, timeout=30)
    return Stream(response.iter_content(2**20))


app = Litestar(route_handlers=[get_backup])


def run():
    import os

    import uvicorn

    port = int(os.environ.get("PORT", 20711))
    uvicorn.run(app, host="localhost", port=port, reload=False)


if __name__ == "__main__":
    run()
