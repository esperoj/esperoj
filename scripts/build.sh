#!/bin/bash

pip install poetry
poetry install --with test,dev
poetry run poe docs
