#!/usr/bin/env bash
set -euo pipefail

poetry run flake8 pyfx2 &&\
    poetry run flake8 --config .flake8-tests tests &&\
    poetry run mypy pyfx2 --exclude _testtooling &&
    poetry run pytest tests/unit &&\
    if nc -z localhost 2000; then
        poetry run pytest tests/integration -m "not extended"
    else
        echo "ERROR: pfx local instance not running on localhost:2000. To run a local instance, execute:" >&2
        echo "docker run --privileged -p 2000:2000 -e PFX_BASE_URL=http://localhost:2000 -e PFX_SERVER_TAG=develop -e MY_CI_REGISTRY_PASSWORD=XXXXX -e CI_REGISTRY_USER=XXX@pricefx.com -e CI_REGISTRY=cregistry.pricefx.eu --rm  --name int-test cregistry.pricefx.eu/engineering/remote-integrationtest:latest" >&2
        exit 1
    fi
