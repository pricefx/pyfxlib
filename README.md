# pyfxlib

A set of utilities to be able to use the Pricefx API from a Python package.

## Installing the project

If you want to install the project locally (to use the API from a script, debug a python job locally or just hack around):

- have python 3.12
- install the latest version of `poetry`
- clone this project
- in the project root, run `poetry install`

This will install all the requirements inside a dedicated virtualenv. To get a shell inside this virtualenv, you can run `poetry shell`.

## Testing the project

The `./devkit/check-all.sh` offers a convenient method to run all tests at once. See the `./devkit/README.md` for details.

## Use pyfxlib in another project

The project needs to have access to the pyfxlib package. If you use poetry, configure its access:
- in a Gitlab CI: `poetry config http-basic.pyfxlib-gitlab gitlab-ci-token ${CI_JOB_TOKEN}`
- locally: `poetry config http-basic.pyfxlib-gitlab <your-username> <your-personal-access-token>`

And the pyproject.toml file must contain the repository definition:

```toml
[[tool.poetry.source]]

name = "pyfxlib-gitlab"
url = "https://gitlab.pricefx.eu/api/v4/projects/12158/packages/pypi/simple"
priority = "explicit"
```

And then in the `[tool.poetry.dependencies]` section:
```toml
pyfxlib = { version = "*", source = "pyfxlib-gitlab" }
```

## Documentation

You can generate an HTML version of the python engine documentation and API by running (after installing the project):

```
poetry run pdoc3 --html --config show_source_code=False -o html --force ./pyfxlib
```

## Release process

Semantic versioning:

- 
