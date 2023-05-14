#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"


echo "Running mypy"
mypy gobeventproducer

echo "\nRunning unit tests"
coverage run --source=gobeventproducer -m pytest

echo "\nCoverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds potential reformat fixes"
black --check --diff gobeventproducer

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobeventproducer gobeventproducer

echo "\nRunning Flake8 style checks"
flake8 gobeventproducer

echo "\nChecks complete"
