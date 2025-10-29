# Dev tools

This folder contains various utilities helping with development of the project.

## `check-all.sh`

Small bash script running all the same stages of the testing pipeline than the CI.

To use, do `./devkit/check-all.sh` from the project root.

If nothing runs on the port 2000, the integration tests don't run. See next session to run them. 

### Integration tests

In order for `check-all.sh` to be able to run integration tests, you will need to ensure a local instance of the backend in running on `http://localhost:2000`. You can run such instance using the following command:

``` sh
docker run --privileged -p 2000:2000 -e PFX_BASE_URL=http://localhost:2000 -e PFX_SERVER_TAG=develop -e MY_CI_REGISTRY_PASSWORD=XXXXX -e CI_REGISTRY_USER=XXX@pricefx.com -e CI_REGISTRY=cregistry.pricefx.eu --rm  --name int-test cregistry.pricefx.eu/engineering/remote-integrationtest:latest
```

- the user is the Gitlab user
- the password is a Gitlab token associated to the user, with a read_registry right
- you must be able to pull a Docker image. If it is the first time, log into Docker `cregistry.pricefx.eu`:
  - command: `docker login cregistry.pricefx.eu -u XXXt@pricefx.com` with your Gitlab account (the Gitlab password will be entered)
  - check that it works with `docker pull cregistry.pricefx.eu/engineering/remote-integrationtest:latest` (you can interrupt once you are sure that the access is not denied)
