import tomllib
from os import getenv
from pathlib import Path
from py7zr import SevenZipFile

config = {}


def getConfig(config_file: str = ""):
    global config
    config_text = ""
    if not config:
        config_path = Path(config_file) if config_file else Path(getenv("ESPEROJ_CONFIG_FILE", str(Path.home() / ".config" / "esperoj" / "esperoj.toml")))
        if config_path.suffix == ".7z":
            with SevenZipFile(str(config_path), password=getenv("ENCRYPTION_PASSPHRASE")) as seven_zip_file:
                seven_zip_contents = seven_zip_file.readall()
                if seven_zip_contents is not None:
                    for _, bio in seven_zip_contents.items():
                        config_text = bio.read().decode("utf-8")
        else:
            config_text = config_path.read_text()
        config = tomllib.loads(config_text)
    return config
