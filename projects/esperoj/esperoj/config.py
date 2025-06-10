import tomllib
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory

from py7zr import SevenZipFile

configs = {}


def get_config(config_file: str = ""):
    config_text = ""
    if not (config := configs.get("esperoj")):
        config_path = (
            Path(config_file)
            if config_file
            else Path(getenv("ESPEROJ_CONFIG_FILE", str(Path.home() / ".config" / "esperoj" / "esperoj.toml")))
        )
        if config_path.suffix == ".7z":
            with SevenZipFile(str(config_path), password=getenv("ENCRYPTION_PASSPHRASE")) as seven_zip_file:
                with TemporaryDirectory() as tmpdirname:
                    seven_zip_file.extractall(path=tmpdirname)
                    config_text = (Path(tmpdirname) / seven_zip_file.getnames()[0]).read_text()
        else:
            config_text = config_path.read_text()
        config = tomllib.loads(config_text)
        configs["esperoj"] = config
    return config
