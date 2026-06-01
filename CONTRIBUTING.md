# pyfxlib

A set of utilities to be able to use the Pricefx API from a Python package.

## Installing the project locally

If you want to install the project locally (to use the API from a script, debug a python job locally or just hack around):

- have python 3.11 or later installed
- install the latest version of `poetry`
- clone this project
- in the project root, run `poetry install`

This will install all the requirements inside a dedicated virtualenv. To get a shell inside this virtualenv, you can run `poetry shell`.

## Testing the project

The `./devkit/check-all.sh` offers a convenient method to run all tests at once. See the `./devkit/README.md` for details.

## Use pyfxlib in another project

Simply install the package from Pypi:

```bash
pip install pyfxlib
```

Or with Poetry:

```bash
poetry add pyfxlib
```

### Testing an unreleased version of pyfxlib in another project (internal Pricefx)

For testing purposes, you can manually publish a package from a merge request without creating an official release tag.

> ⚠️  **Important**: Always bump the version in `pyproject.toml` **before** publishing a test package.
> Without bumping, projects pinned to the current stable version may inadvertently install your dev package instead.

1. Run manually the job `publish-package` from the Gitlab CI interface. <!-- TODO: update to the Github registry when the migration is done -->
2. The package version will be `X.Y.Z+branch-name` (e.g., `1.2.3+feature-branch`) and will be available in the Gitlab package repository. <!-- TODO: update to the Github registry when the migration is done -->
3. In the target project, reference the test version in the `pyproject.toml` file: <!-- TODO: update to the Github registry when the migration is done -->
```toml
pyfxlib = { version = "X.Y.Z+branch-name", source = "pyfxlib-gitlab" }
```

Note: Test packages are not official releases and should only be used for development and testing purposes.

## Documentation

You can generate an HTML version of the pyfxlib documentation and API by running (after installing the project):

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

- **`develop`**: Main development branch
    - Contains the latest changes for the next release
    - Used for releasing new major, minor and patch versions
    - Can only release versions based on the latest version on this branch
- **`vX`** (e.g., `v1`, `v2`): Maintenance branches for each major version
    - Created when a new major version is released from `develop`
    - Used for releasing minor and patch versions on previous major versions
    - Can only release versions based on the latest version on this branch

**Important**: On any branch, you can only patch or release new minors for the **latest minor version** on that branch.

### Git tags

The release tags are in a **`vX.Y.Z`** format (e.g., `v1.2.3`, `v2.0.0`).
They must be set only on:
- `develop` branch (for new releases on the latest version)
- `vX` maintenance branch (for releases on previous major versions)

When a tag is set, the package is automatically created.

### Third-party licenses

If you add, remove, or update a dependency in `pyproject.toml`, update the following files accordingly:

- `THIRDPARTY.txt`: run `poetry run pip-licenses` and replace the file content
- `NOTICE`: check if any new direct dependency has a Apache 2.0 license with a NOTICE file, MPL-2.0, or LGPL license,
  and if so, add the required notices in this file.

### Release Workflows

#### New Major Version (e.g. 4.2.3 → 5.0.0)

1. Create a maintenance branch from `develop` for the current major version (E.g., `v4` if you're bumping from v4.x.y to v5.0.0).
  This allows future hotfixes on the previous major version.
2. Create a release branch from `develop` (E.g., release/v5.0.0)
3. Update the version in `pyproject.toml`:
```commandline
poetry version major   # for breaking changes (4.2.0 → 5.0.0)
```
4. Add the breaking changes to the `CHANGELOG.md` file under a new section with the new version and date.
5. Commit the version changes
6. Merge the release branch into `develop` (via a merge request)
7. Create the version tag on `develop` (E.g., v5.0.0). It must correspond to the version set in `pyproject.toml` (checked in the CI)
8. The package will be automatically built and published to both the Gitlab package repository and PyPI. <!-- TODO: update to the Github registry when the migration is done -->
9. Delete the release branch

#### New Minor Version (e.g. 2.3.4 → 2.4.0)

From `develop` branch: for the last major version. It is the same workflow as for a major version, except step 1.

1. Create a release branch from `develop` (E.g., release/v2.4.0)
2. Update the version in `pyproject.toml`:
```commandline
 poetry version minor   # for new features (2.3.4 → 2.4.0)
```
3. Add the new features to the `CHANGELOG.md` file under a new section with the new version and date.
4. Commit the version changes
5. Merge the release branch into `develop` (via a merge request)
6. Create the version tag on `develop` (E.g., v2.4.0). It must correspond to the version set in `pyproject.toml` (checked in the CI)
7. The package will be automatically built and published to the Gitlab package repository and PyPI. <!-- TODO: update to the Github registry when the migration is done -->
8. Delete the release branch

From a maintenance branch: for previous major versions (e.g, v3 at 3.2.1 → 3.3.0, while `develop` is at v4.x.y).

It is the same process as above, but:
- Replace `develop` with the maintenance branch name (e.g., `v3`)
- The tag is created on the maintenance branch

#### Patch Releases (e.g. 1.2.3 → 1.2.4)

It is the same process as a minor version, but use:
```commandline
poetry version patch   # for bug fixes (1.2.3 → 1.2.4)
```
Add the bugfix in the `CHANGELOG.md` if it makes sense.

It is done either on `develop` or on a maintenance branch, depending on which version is being patched.
Only the last minor version of a branch can be patched.

Cherry-picking fixes: If a fix needs to be applied to both a maintenance branch and develop, implement it first on the appropriate branch, then cherry-pick to the other:

**Important**: Do NOT merge maintenance branches into develop. Cherry-pick individual commits instead.
