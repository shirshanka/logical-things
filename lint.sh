#!/bin/bash
set -euxo pipefail
black dataset.py
isort dataset.py
flake8 dataset.py