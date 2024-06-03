import tomllib

with open("pyproject.toml", "rb") as f:
    pyproject = tomllib.load(f)

version = pyproject["project"]["version"]

print(version, end="")
