#!/bin/bash
uname -a
uptime
lscpu
df -hT
apt update -yqq
apt install -yqq curl wget
curl -sL yabs.sh | bash -s -- -s https://www.vpsbenchmarks.com/yabs/upload
pip install poetry
poetry install --with test,dev
poetry run poe docs
