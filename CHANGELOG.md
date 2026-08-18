# Changelog

## [1.2.2] - 2026-08-18

### Changed
- `setuptools` minimal version is now `0.83.0` for security (CVE-2026-59890)

## [1.2.1] - 2026-08-12

### Added
- `BackendVersion.parse()` classmethod to parse a Pricefx backend version string
  (e.g. `"15.2.0"`, `"15.2.0-SNAPSHOT"`) into a `BackendVersion` instance.

### Fixed
- `list_lpg_items` now returns validated `LPGProduct` instances as documented,
  instead of raw dicts.

## [1.2.0] - 2026-08-12

### Added
- `stream`, `fetch_paginated`, `to_pandas`, and `to_pandas_paginated` on table
  entities now accept an optional `filters` argument for server-side row filtering,
  and an optional `columns` argument to fetch a subset of columns instead of the full table.

## [1.1.0] - 2026-07-31

### Added
- `push_pandas` and `update_pandas` now support the `LOB` field type via
  `manual_fields_specs`. Pushing a `LOB` field requires Pricefx core `16.3.13`
  or later (16.x) / `17.0.4` or later (17.x), `RuntimeError` otherwise.
- `push_pandas` and `update_pandas` gained a `check_oversized_values` parameter:
  when `True`, warns about `TEXT`/`LOB` values exceeding what the backend
  accepts (255 / 10000 characters) and would otherwise be silently truncated
  on push without error.
- A one-time warning is logged whenever TEXT/LOB fields are pushed, regardless of 
  the above flag.

## [1.0.0] - 2026-06-23

### Changed
- pyfxlib is now publicly available on [PyPI](https://pypi.org/project/pyfxlib/).
  The source code is hosted on [GitHub](https://github.com/pricefx/pyfxlib), making
  it easier to integrate pyfxlib in your projects without requiring access to
  Pricefx internal infrastructure.

## [0.7.0] - 2026-04-29

### Changed
- `to_pandas()` and `to_pandas_paginated()` now set key columns as the DataFrame
  index, consistent with the behavior when pushing a DataFrame to the platform.
  **Breaking change**: code using `reset_index(drop=True)` after these methods
  will silently drop key columns — replace with `reset_index()`.

## [0.6.0] - 2026-04-29

### Added
- `TableImmutable` and `TableMutable` now expose field metadata (`name`, `label`,
  `type`, `key`, `dimension`) via the `fields` attribute.

## [0.5.0] - 2026-04-28

### Added
- `ConnectionAsync.get()` and `ConnectionAsync.post()` for calling Pricefx
  endpoints not natively supported by pyfxlib.

## [0.4.1] - 2026-04-23

### Fixed
- `ConnectionSync` isolated in a dedicated thread to avoid event loop conflicts
  with pytest-asyncio and httpcore.

## [0.4.0] - 2026-03-12

### Added
- Pydantic models for core Pricefx objects.
- New connection methods: `send_notification`, `list_users`, `list_lpg_items`,
  `quick_search`, `update_lpg`, `submit_lpg`, `import_files`, `login_extended`,
  `backend_version`, `get_application_property`, `create_action`, `query_meta`.
- `AdvancedCriteria` for complex filtering.
- Published to [PyPI](https://pypi.org/project/pyfxlib/) (Apache 2.0 license).

### Changed
- `Instance` renamed to `Partition` (`Instance` kept as deprecated alias).
- `get_advanced_property` renamed to `get_application_property`.

## [0.3.0] - 2026-03-03

### Changed
- **Breaking change**: all connection internals are now async (httpx replaces
  requests). The high-level `api.domain` layer remains synchronous via the new
  `ConnectionSync` wrapper.

### Added
- `ConnectionSync`: synchronous wrapper around async connections, for use in
Python Engine and other sync contexts.
- `to_pandas_paginated()`: paginated DataFrame fetch for large tables.

## [0.2.0] - 2026-01-19

### Added
- `PasswordProviderEnvironment`: read credentials from environment variables.

## [0.1.1] - 2026-01-14

### Changed
- Relaxed Python version requirement to `>=3.11`.
- Relaxed package dependency version constraints.

## [0.1.0] - 2025-12-15

Initial release. Extracted from the Pricefx Python Engine project.

### Added
- Low-level session management (`PfxSession`) with JWT and keyring authentication.
- `ConnectionRemote`, `ConnectionLocal`, `ConnectionComposed` for interacting
  with Pricefx instances.
- High-level `api.domain` layer: `Partition`, `DataSource`, `Datamart`,
  `TableMutable`, `TableImmutable`, `LPGCollection`, `ModelObject`, and more.
