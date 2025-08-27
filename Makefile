# Define variables
ENV ?= dev
VENV_DIR ?= .venv
TMP_VENV_DIR ?= $(TMPDIR)/esperoj-venv

.ONESHELL:

# ------------------------------------------------------------------------------
# 1. Environment Setup
# ------------------------------------------------------------------------------

setup: $(VENV_DIR)
$(VENV_DIR):
	@echo "Setting up virtual environment in $(VENV_DIR)..."
	@if [ ! -d $(VENV_DIR) ]; then \
		rm -fr $(TMP_VENV_DIR) ; \
		python3 -m venv $(TMP_VENV_DIR) ; \
		ln -s $(TMP_VENV_DIR) $(VENV_DIR); \
	fi
	@. $(VENV_DIR)/bin/activate && pip install -e ".[$(ENV)]"
	@echo "Virtual environment setup complete."

clean-venv:
	@echo "Removing virtual environment..."
	@rm -fr $(VENV_DIR)
	@rm -fr $(TMP_VENV_DIR)
	@echo "Virtual environment removed."

lock: setup
	@echo "Purging pip cache and locking dependencies..."
	@. $(VENV_DIR)/bin/activate && pip cache purge
	@. $(VENV_DIR)/bin/activate && pip-compile pyproject.toml --extra dev --universal --output-file requirements.txt
	@echo "Dependencies locked to requirements.txt."

# ------------------------------------------------------------------------------
# 2. Django Management Commands
# ------------------------------------------------------------------------------

manage = ./manage.py

run: setup
	@echo "Starting Django development server..."
	$(manage) runserver

run_plus: setup
	@echo "Starting Django development server with django-extensions (runserver_plus)..."
	$(manage) runserver_plus

shell: setup
	@echo "Opening Django shell..."
	$(manage) shell

shell_plus: setup
	@echo "Opening Django shell_plus with django-extensions..."
	$(manage) shell_plus

makemigrations: setup
	@echo "Creating new migrations..."
	$(manage) makemigrations esperoj

migrate: setup
	@echo "Applying database migrations..."
	$(manage) migrate

fresh_db: clean-migrations clean-db setup makemigrations migrate createsuperuser
	@echo "Fresh database setup complete."

createsuperuser: setup
	@echo "Creating superuser 'admin' with password 'pass' and email 'admin@example.com'..."
	@echo "from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.create_superuser('admin', 'admin@example.com', 'pass')" | $(manage) shell
	@echo "Superuser created."

# ------------------------------------------------------------------------------
# 3. Code Quality & Testing
# ------------------------------------------------------------------------------

lint: setup
	@echo "Running Pyright static type checker..."
	@. $(VENV_DIR)/bin/activate && pyright
	@echo "Running Ruff linter and formatter..."
	@. $(VENV_DIR)/bin/activate && ruff check esperoj/ --fix
	@. $(VENV_DIR)/bin/activate && ruff format esperoj/

test: setup
	@echo "Running tests with pytest and coverage..."
	@. $(VENV_DIR)/bin/activate && pytest --cov-report term-missing --cov=esperoj

# ------------------------------------------------------------------------------
# 4. Build & Distribution
# ------------------------------------------------------------------------------

nuitka: setup
	@echo "Building standalone executable with Nuitka..."
	@cp esperoj/__main__.py .
	@defer rm __main__.py
	@. $(VENV_DIR)/bin/activate && nuitka --static-libpython=yes --standalone --onefile --output-dir="build" __main__.py
	@echo "Nuitka build complete."

release: setup
	@echo "Creating git tag for new release..."
	@export VERSION=$$(python scripts/get_version.py); \
	git tag -a "v$${VERSION}" -m "Release version $${VERSION}"; \
	git push origin "v$${VERSION}"
	@echo "Release tag v$${VERSION} created and pushed."

# ------------------------------------------------------------------------------
# 5. Clean-up
# ------------------------------------------------------------------------------

clean: clean-pyc clean-build clean-migrations clean-db clean-ruff-cache
	@echo "Full clean-up complete."

clean-pyc:
	@echo "Removing Python bytecode files..."
	@find . -name "*.pyc" -exec rm -f {} +
	@find . -name "*.pyo" -exec rm -f {} +
	@find . -name "*~" -exec rm -f {} +
	@find . -name "__pycache__" -exec rm -fr {} +

clean-build:
	@echo "Removing build artifacts..."
	@rm -fr build/
	@rm -fr dist/
	@rm -fr .eggs/
	@rm -fr *.egg-info/
	@rm -fr .pytest_cache/

clean-migrations:
	@echo "Deleting migration files..."
	@find esperoj -path "*/migrations/*.py" -not -name "__init__.py" -delete
	@find esperoj -path "*/migrations/*.pyc" -delete

clean-db:
	@echo "Deleting database file..."
	@rm -f db.sqlite3

clean-ruff-cache:
	@echo "Deleting Ruff cache..."
	@rm -fr .ruff_cache/

.PHONY: setup clean-venv lock run run_plus shell shell_plus makemigrations migrate fresh_db createsuperuser lint test nuitka release clean clean-pyc clean-build clean-migrations clean-db clean-ruff-cache
