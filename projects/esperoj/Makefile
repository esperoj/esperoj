# Define variables
ENV ?= dev
VENV_DIR ?= .venv
TMP_VENV_DIR ?= $(TMPDIR)/esperoj-venv
REQUIREMENTS_DEV ?= requirements-dev.txt
REQUIREMENTS_NUITKA ?= requirements-nuitka.txt

.ONESHELL:
setup: $(VENV_DIR)
$(VENV_DIR):
	@if [ ! -d $(VENV_DIR) ]; then
		rm -r $(TMP_VENV_DIR) ;
		python3 -m venv $(TMP_VENV_DIR) ;
		ln -s $(TMP_VENV_DIR) $(VENV_DIR);
	fi
	@. $(VENV_DIR)/bin/activate && pip install -e ".[cli,server,$(ENV)]"

lock:
	@. $(VENV_DIR)/bin/activate && pip cache purge
	@. $(VENV_DIR)/bin/activate && pip-compile pyproject.toml --extra nuitka --universal --output-file $(REQUIREMENTS_NUITKA)
	@. $(VENV_DIR)/bin/activate && pip-compile pyproject.toml --extra dev --universal --output-file $(REQUIREMENTS_DEV)

nuitka: setup
	cp esperoj/__main__.py .
	@defer rm __main__.py
	@. $(VENV_DIR)/bin/activate && nuitka --static-libpython=yes --standalone --onefile --output-dir="build" __main__.py

lint: setup
	@pre-commit run --all-files --color always
	@. $(VENV_DIR)/bin/activate && pyright
	@if [ -d "$${ESPEROJ_SCRIPTS_FOLDER}" ]; then
		cd "$${ESPEROJ_SCRIPTS_FOLDER}";
		ruff check .;
		ruff format .;
		pyright .;
	fi

update:
	rm requirements-*
	make lock

test: setup
	@. $(VENV_DIR)/bin/activate && pytest --cov-report term --cov=esperoj

dev-server: setup
	@. $(VENV_DIR)/bin/activate && litestar --app esperoj.server:app run --unix-domain-socket "${HOME}/.sockets/dev.sock" --reload --reload-dir="./esperoj" --reload-exclude="__pycache__"

start: setup
	@. $(VENV_DIR)/bin/activate && litestar --app esperoj.server:app run --port $${PORT:-20711}

release:
	@export VERSION=$$(python ../../scripts/get_version.py); \
	git tag -a "v$${VERSION}" -m "Release version $${VERSION}"; \
	git push origin "v$${VERSION}"
