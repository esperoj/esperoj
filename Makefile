# Define variables
ENV ?= dev
VENV_DIR ?= .venv
TMP_VENV_DIR ?= $(TMPDIR)/esperoj-venv

.ONESHELL:
setup: $(VENV_DIR)
$(VENV_DIR):
	@if [ ! -d $(VENV_DIR) ]; then
		rm -fr $(TMP_VENV_DIR) ;
		python3 -m venv $(TMP_VENV_DIR) ;
		ln -s $(TMP_VENV_DIR) $(VENV_DIR);
	fi
	@. $(VENV_DIR)/bin/activate && pip install -e ".[$(ENV)]"

dev-server:
	./manage.py runserver
lock:
	@. $(VENV_DIR)/bin/activate && pip cache purge
	@. $(VENV_DIR)/bin/activate && pip-compile pyproject.toml --extra dev --universal --output-file $(REQUIREMENTS_DEV)

nuitka: setup
	cp esperoj/__main__.py .
	@defer rm __main__.py
	@. $(VENV_DIR)/bin/activate && nuitka --static-libpython=yes --standalone --onefile --output-dir="build" __main__.py

lint: setup
	@. $(VENV_DIR)/bin/activate && pyright

test: setup
	@. $(VENV_DIR)/bin/activate && pytest --cov-report term --cov=esperoj

release:
	@export VERSION=$$(python scripts/get_version.py); \
	git tag -a "v$${VERSION}" -m "Release version $${VERSION}"; \
	git push origin "v$${VERSION}"
