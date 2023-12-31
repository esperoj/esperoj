[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/esperoj/esperoj/main.svg)](https://results.pre-commit.ci/latest/github/esperoj/esperoj/main)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/186389a941314e89abf9034106aa291e)](https://app.codacy.com/gh/esperoj/esperoj/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
# Esperoj

Esperoj package with cli.

<details>
<summary>Developing</summary>
- Run `poe` from within the development environment to print a list of [Poe the Poet](https://github.com/nat-n/poethepoet) tasks available to run on this project.
- Run `poetry add {package}` from within the development environment to install a run time dependency and add it to `pyproject.toml` and `poetry.lock`. Add `--group test` or `--group dev` to install a CI or development dependency, respectively.
- Run `poetry update` from within the development environment to upgrade all dependencies to the latest versions allowed by `pyproject.toml`.
- Run `pre-commit autoupdate` to update pre-commit
</details>
