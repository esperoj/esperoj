#!/bin/bash
poetry install --with test,dev
poetry run poe docs
rclone copy -v pcloud:workspace/backup backup
7zz a "-p${ENCRYPTION_PASSPHRASE}" backup.7z backup
rm -r backup
mkdir public
mv backup.7z docs public
cd public
echo "/backup $(pomfload backup.7z)" > _redirects