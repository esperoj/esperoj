#!/bin/bash
info.sh
set -Exeo pipefail
python -m venv .venv
. ./.venv/bin/activate
poetry install --with test,dev
poetry run poe docs
rclone sync workspace:backup ./backup
7zz a "-p${ENCRYPTION_PASSPHRASE}" backup.7z ./backup
mv backup.7z docs public