# pyfxlib

A set of utilities to be able to use the Pricefx API from a Python package.

The working repository is available at: https://gitlab.pricefx.eu/pricefx/pyfxlib (Pricefx users only).
The public mirror is available at: https://github.com/pricefx/pyfxlib.
All the protected branches of the Gitlab repository are automatically mirrored to the public Github repository.

## Installing the project locally

If you want to install the project locally (to use the API from a script, debug a python job locally or just hack around):

- have python 3.11 or later installed
- install the latest version of `poetry`
- clone this project
- in the project root, run `poetry install`

This will install all the requirements inside a dedicated virtualenv. To get a shell inside this virtualenv, you can run `poetry shell`.

## Testing the project

The `./devkit/check-all.sh` offers a convenient method to run all tests at once. See the `./devkit/README.md` for details.

## Supported backend versions

pyfxlib follows the Pricefx major versions, so that projects depending on pyfxlib keep working
with the backends they run on. With N the current major:
- **N to N-2 are supported**, from `MIN_SUPPORTED_BACKEND_VERSION` in `pyfxlib/schema/core.py`.
- **N-3 is deprecated**, from `MIN_DEPRECATED_BACKEND_VERSION`, derived from it: its code is
  kept, so that it keeps working as it did, but it is no longer supported nor tested.
- **Anything older is not supported**, and has no code left.

A connection logs a warning, once, when it fetches the version of a deprecated or older backend.

A change the backend got after `MIN_DEPRECATED_BACKEND_VERSION` must be gated, so that older
backends fail with a clear error before the request is sent, rather than with an obscure
backend one. Such a change is declared with the backend versions it is available from, as
`VersionBounds` (one lower bound per major, as it is often backported to several majors):
- in the Query API, in `_REQUIREMENTS` or `_TABLE_KIND_RENAMES` of
  `pyfxlib/lowlevel/querycompat.py`. Unit tests fail when a table, stage, expression, field,
  function or literal type is added to the schema with no requirement nor in their baseline;
- for pushed field types, in `_FIELD_TYPE_MIN_BACKEND_VERSION` of
  `pyfxlib/lowlevel/pandasutil.py`;
- elsewhere, as a constant the method checks, e.g. `NOTIFICATIONS_SINCE` for
  `send_notification`, with a test failing once every backend pyfxlib keeps the code for has
  the change. Export it from `pyfxlib.lowlevel`, so that callers can check a backend before
  offering what needs the change.

### When a new major version is released

Raise `MIN_SUPPORTED_BACKEND_VERSION` to the first release of the major two below the new one
(e.g. 16.0.0 once 18 is released): the major below that becomes deprecated, and the one it
replaces loses its code. Then:

1. Update the versions in `README.md`, and add the change to the `CHANGELOG.md`.
2. Run the unit tests. They fail on every gate all the backends pyfxlib keeps the code for now
   meet, listing them:
   - `test_no_requirement_is_met_by_every_handled_backend`: drop each listed requirement from
     `_REQUIREMENTS`, and add it to the baseline of the matching
     `test_every_schema_*_is_baseline_*` test in `tests/unit/lowlevel/test_querycompat.py`, as
     the oldest deprecated backend now has it. Update the tests of those requirements accordingly.
   - `test_no_table_kind_rename_is_known_by_every_handled_backend`: drop each listed rename
     from `_TABLE_KIND_RENAMES`, and the old spelling from the schema (e.g. `datafeed` from
     `PADataFeed`). This is a breaking change for queries posted without a connection.
   - `test_no_field_type_gate_is_met_by_every_handled_backend`: drop each listed field type
     from `_FIELD_TYPE_MIN_BACKEND_VERSION`, and the whole gate once it is empty.
   - `test_the_notification_gate_is_not_met_by_every_handled_backend`: drop
     `NOTIFICATIONS_SINCE` and its check from `send_notification`.
   - `test_the_query_meta_tables_gate_is_not_met_by_every_handled_backend`: drop
     `QUERY_META_TABLES_SINCE` and its check from `query_meta_tables` and `query_meta_table`.

   These constants are public: dropping one is a breaking change for the callers checking it.
3. Search the code for version-specific code no test covers, which every deprecated backend
   no longer needs, and drop it: e.g. the `core < 15.0.0` payload of `import_files`.

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

1. Run manually the job `publish-package` from the Gitlab CI interface.
2. The package version will be `X.Y.Z+branch-name` (e.g., `1.2.3+feature-branch`) and will be available in the Gitlab package repository.
3. In the target project, reference the test version in the `pyproject.toml` file:
```toml
[[tool.poetry.source]]
name = "pyfxlib-gitlab"
url = "https://gitlab.pricefx.eu/api/v4/projects/12158/packages/pypi/simple"
priority = "explicit"
```

And in the bloc: `[tool.poetry.dependencies]`:
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
2. Mark the created maintenance branch as protected in Gitlab. **It is necessary for the branch to be mirrored in the public Github repository.**
3. Create a release branch from `develop` (E.g., release/v5.0.0)
4. Update the version in `pyproject.toml`:
```commandline
poetry version major   # for breaking changes (4.2.0 → 5.0.0)
```
5. Add the breaking changes to the `CHANGELOG.md` file under a new section with the new version and date.
6. Commit the version changes
7. Merge the release branch into `develop` (via a merge request)
8. Create the version tag on `develop` (E.g., v5.0.0). It must correspond to the version set in `pyproject.toml` (checked in the CI)
9. The package will be automatically built and published to both the Gitlab package repository and PyPI.
10. Delete the release branch

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
7. The package will be automatically built and published to the Gitlab package repository and PyPI.
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
