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

.PHONY: fresh_db
fresh_db:
	@echo "Deleting migration files..."
	@find . -path "*/migrations/*.py" -not -name "__init__.py" -delete
	@find . -path "*/migrations/*.pyc"  -delete
	@echo "Deleting database..."
	@rm -f db.sqlite3
	@echo "Creating new migrations..."
	@python manage.py makemigrations
	@echo "Applying migrations..."
	@python manage.py migrate
	@echo "Creating superuser..."
	@echo "from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.create_superuser('admin', 'admin@example.com', 'pass')" | python manage.py shell
	@echo "Done!"
