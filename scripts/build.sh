#!/bin/bash

pip install poetry
poetry install --with test,dev
poe docs
