#!/bin/bash

set -Exeo pipefail
chezmoi update --force --no-tty
. ~/.profile
python -m venv .venv
. ./.venv/bin/activate
poetry install --with test,dev
poetry run poe docs
rclone sync -v workspace:backup ./backup
7zz a "-p${ENCRYPTION_PASSPHRASE}" backup.7z ./backup
mv backup.7z docs public
