#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"

# Find all python files in ./gobeventproducer
FILES=$(find . -type f -name "*.py" | grep gobeventproducer)

# DIRTY_FILES will skip mypy checks, but other checks will run
DIRTY_FILES=(
  ./gobeventproducer/__init__.py
  ./gobeventproducer/__main__.py
  ./gobeventproducer/database/local/contextmanager.py
  ./gobeventproducer/database/local/__init__.py
  ./gobeventproducer/database/local/model.py
  ./gobeventproducer/database/local/connection.py
  ./gobeventproducer/database/gob/contextmanager.py
  ./gobeventproducer/database/gob/__init__.py
  ./gobeventproducer/config.py
  ./gobeventproducer/mapping/__init__.py
  ./gobeventproducer/mapper.py
  ./gobeventproducer/producer.py
  ./gobeventproducer/eventbuilder.py
)

# CLEAN_FILES is FILES - DIRTY_FILES. CLEAN_FILES will see all checks
CLEAN_FILES=$(echo ${FILES[@]} ${DIRTY_FILES[@]} | tr ' ' '\n' | sort | uniq -u | tr '\n' ' ')

echo "Running mypy on non-dirty files"
mypy --follow-imports=skip --install-types ${CLEAN_FILES[@]}

echo "\nRunning unit tests"
coverage run --source=gobeventproducer -m pytest

echo "Coverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds no potential reformat fixes"
black --check --diff ${FILES[@]}

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobeventproducer ${FILES[@]}

echo "\nRunning Flake8 style checks"
flake8 ${FILES[@]}

echo "\nChecks complete"
