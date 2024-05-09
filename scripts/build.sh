#!/bin/bash
poetry install --with test,dev
poetry run poe docs
wget --no-verbose "https://public.esperoj.eu.org/backup.7z"
mv backup.7z docs public
cd public
url="$(curl -sL https://raw.githubusercontent.com/esperoj/dotfiles/main/bin/pomfload | sh -s -- backup.7z)"
echo "/backup ${url}" > _redirects