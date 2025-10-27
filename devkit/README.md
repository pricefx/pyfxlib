# Dev tools

This folder contains various utilities helping with development of the project.

## `check-all.sh`

Small bash script running all the same stages of the testing pipeline than the CI.

To use, do `./devkit/check-all.sh` from the project root.

In order for `check-all.sh` to be able to run integration tests, you will need to ensure a local instance of the backend in running on `http://localhost:2000`. You can run such instance using the following command:

``` sh
docker run --privileged -p 2000:2000 -e PFX_BASE_URL=http://localhost:2000 -e PFX_SERVER_TAG=develop -e MY_CI_REGISTRY_PASSWORD=XXXXX -e CI_REGISTRY_USER=XXX@pricefx.com -e CI_REGISTRY=cregistry.pricefx.eu --rm  --name int-test cregistry.pricefx.eu/engineering/remote-integrationtest:latest
```