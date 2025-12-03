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

This project follows [Semantic Versioning](https://semver.org/) (MAJOR.MINOR.PATCH):

  - **MAJOR**: Incompatible API changes (breaking changes)
  - **MINOR**: New functionality in a backward-compatible manner
  - **PATCH**: Backward-compatible bug fixes

The only source of truth for the release version is the toml file `pyproject.toml`.

### Git branching model

- **`develop`**: Main development branch - Always contains the latest changes for the next release
- **`vX`** (e.g., `v1`, `v2`): Maintenance branches for each major version

### Git tags

The release tags are in a **`vX.Y.Z`** format (e.g., `v1.2.3`, `v2.0.0`). They must be set only on `develop` branch or on a maintenance branch.
When a tag is set, the package is automatically created.

### Create a new release from develop

#### New major version

1. Create a maintenance branch from `develop` for the current major version (E.g., `v4` if you're bumping from v4.x.y to v5.0.0).
  This allows future hotfixes on the previous major version.
2. Create a release branch from `develop` (E.g., release/v5.0.0)
3. Update the version in `pyproject.toml`:
```commandline
poetry version major   # for breaking changes (4.2.0 → 5.0.0)
```
4. Commit the version changes
5. Merge the release branch into `develop` (via a merge request)
6. Create the version tag on `develop` (E.g., v5.0.0). It must correspond to the version set in `pyproject.toml` (checked in the CI)
7. The package will be automatically built and published to the Gitlab package repository
8. Delete the release branch

#### New minor or patch version

Follow the same process as for a new major version, except that the first step is not needed.

The command to update the version is either:
```commandline
 poetry version patch   # for bug fixes (0.1.0 → 0.1.1)
 poetry version minor   # for new features (0.1.0 → 0.2.0)
```

###  Patch/hotfix on a maintenance branch

To publish a hotfix for a previous version (E.g., v1.x.y while develop is at v2.z.t):
follow the same process from the maintenance branch instead of `develop` (_skip step 1 - no new maintenance branch needed_).

If the fix is relevant for future versions, cherry-pick the fix to `develop` after the release.

**Note**: Do NOT merge maintenance branches into develop. The main `develop` branch should only track the latest major version.
