import tomllib

with open("pyproject.toml", "rb") as f:
    pyproject = tomllib.load(f)

version = pyproject["tool"]["poetry"]["version"]

print(version, end="")