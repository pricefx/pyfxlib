#!/usr/bin/env bash
set -euo pipefail

# If --fail-fast or -f is passed as an argument, the script will stop at the first failure.
PYTEST_OPTS=""
if [[ "${1:-}" == "--fail-fast" || "${1:-}" == "-f" ]]; then
    PYTEST_OPTS="-x"
fi

echo "Checking pyproject.toml formatting"
poetry run toml-sort --check pyproject.toml

echo "Checking import ordering"
poetry run isort --check-only --diff pyfxlib
poetry run isort --check-only --diff tests
poetry run isort --check-only --diff scripts

echo "Checking code formatting"
poetry run flake8 pyfxlib
poetry run flake8 --config .flake8-tests tests
poetry run flake8 scripts

echo "Checking type annotations"
poetry run mypy pyfxlib --exclude _testtooling

echo "Running unit tests"
poetry run pytest tests/unit $PYTEST_OPTS

echo "Running integration tests"
if nc -z localhost 2000; then
    poetry run pytest tests/integration -m "not extended" $PYTEST_OPTS
else
    echo "ERROR: pfx local instance not running on localhost:2000. To run a local instance, execute:" >&2
    echo "docker run --privileged -p 2000:2000 -e PFX_BASE_URL=http://localhost:2000 -e PFX_SERVER_TAG=develop -e MY_CI_REGISTRY_PASSWORD=XXXXX -e CI_REGISTRY_USER=XXX@pricefx.com -e CI_REGISTRY=cregistry.pricefx.eu --rm  --name int-test cregistry.pricefx.eu/engineering/remote-integrationtest:latest" >&2
    exit 1
fi
