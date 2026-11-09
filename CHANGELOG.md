# Changelog

## [2.0.0] - 2026-10-13

Aligns the Query API schema with the backend `QueryApiDTO`.

### Added
- `HistoricalConditionRecords` and `Users` tables.
- `safeDiv` in `Operation`.
- `percentRankOver` and `cumeDistOver` (`RankOverOperation`, `PreviousStageRankOverFunction`),
  whose single argument is an `Order`.
- `LiteralValueType`, the type of a `LiteralValue`. It spells datetime `DATETIME` as the backend
  expects, and adds the `DURATION` and `PERIOD` literal types (ISO-8601 strings).
- `ResultColumnType`, the type of a `QueryAnswerMetaColumn`, as `LiteralType` was until now.
- `AggregateScalarFunction`, so aggregate stages accept scalar functions over aggregates,
  e.g. `div(sum(a), sum(b))`.
- Queries sent through a connection (`query_meta`, `query_execute`) are adapted to the backend
  version first (`pyfxlib.lowlevel.querycompat`), on a copy: the caller's query is left
  untouched. `dataFeed` and `datafeed`, and `dataSource` for older backends, are rewritten to the
  kind the backend knows, and each rewrite is logged at debug level. So is sending the deprecated
  `datasource` to a backend that knows `dataSource`.
- Queries sent through a connection are also checked against every Query API change since
  15.0.0, the first release of the endpoint: tables, stages, expressions, fields, functions and
  literal types, each bounded by the releases it was backported to. A query using anything the
  backend does not support raises `UnsupportedQueryError` (exported from `pyfxlib.lowlevel`)
  before being sent, listing every such use with the versions supporting it and its ticket.
  `safeDiv`, `percentRankOver` and `cumeDistOver` are accepted from 17.2.0 only: the few earlier
  releases that briefly had them (16.3.16, 17.0.7, 17.0.8, PFUN-35990) are rejected as well.
- `MIN_SUPPORTED_BACKEND_VERSION` (exported from `pyfxlib.schema`), the oldest backend pyfxlib
  supports: 15.0.0, as pyfxlib supports the current major and the two before it. The major
  before them is deprecated, from `MIN_DEPRECATED_BACKEND_VERSION` (14.0.0): pyfxlib keeps
  working with it as it did, without supporting it. A connection logs a warning, once, the first
  time it fetches the version of a deprecated or older backend.
- `BackendVersion` instances are ordered (`<`, `<=`, `>`, `>=`), by major, minor then patch.
- `VersionBounds` (exported from `pyfxlib.schema`), the backend versions a change is available
  from, as one lower bound per major: e.g. `VersionBounds("16.3.11", "17.0.2")`.
- What a backend supports is fetched once per connection, on the first query that needs it, and
  kept for the connection lifetime: a backend upgraded meanwhile is only seen by a new
  connection. Connections to several backends each keep their own.

### Changed
- **Breaking:** `BackendVersion.minor` and `BackendVersion.patch` are `int`, defaulting to 0,
  instead of `Optional[int]` defaulting to `None`. A missing or null minor or patch is now 0, so
  code checking them against `None` must check against 0. `BackendVersion(major=15)` now equals
  `BackendVersion(major=15, minor=0, patch=0)` and dumps the same, as their string forms already
  did. Likewise, `backend_version()` returns a `dict[str, int]` instead of a
  `dict[str, Optional[int]]`: its minor and patch are 0 instead of `None`.
- **Breaking:** `BackendVersion` is immutable: assigning a field raises a `ValidationError`.
  Use `model_copy(update=...)` for a changed copy. Versions are hashable, so they can be set
  members and dict keys.
- **Breaking:** `UnboundedBound` is split into `UnboundedPreceding` and `UnboundedFollowing`,
  and `Bound` into `StartBound` and `EndBound`. A frame can no longer start at the end of the
  partition or end at its start, which the backend rejects.
- `PADataFeed` now sends `dataFeed`: backends from 16.3.11 / 17.0.2 renamed `datafeed` to
  `dataFeed` (PFUN-35498). Queries sent through a connection still reach older backends as
  `datafeed` (see above). A query posted by other means needs `kind="datafeed"` for an older
  backend. Both ids parse.
- `PADataSource` now sends `dataSource` instead of the deprecated `datasource`: backends from
  16.3.11 / 17.0.2 added it (PFUN-35498). Queries sent through a connection still reach older
  backends as `datasource` (see above). A query posted by other means needs `kind="datasource"`
  for an older backend. Both ids parse.
- Function arguments, aggregate dimensions and join columns default to an empty list.
- `in` and `notIn` only accept literal values after their first argument, as the backend does.

### Deprecated
- `LiteralType`, an alias of `ResultColumnType` that warns on use, will be removed in the next
  major version. It typed both literal values and result columns, though the backend spells
  datetime differently in each (`DATETIME` against `DATE_TIME`), so every datetime literal was
  rejected. Use `ResultColumnType` for result columns and `LiteralValueType` for literal values.

## [1.4.0] - 2026-09-28

### Added
- Model object logics and calculations on the connections (`ConnectionAsync`,
`ConnectionSync` and their implementations):
  - `list_model_logic_parameters`: the inputs a model evaluation logic expects and the
  elements it returns, in the context of a model object;
  - `execute_model_logic`: run a model evaluation logic in the context of a model object
  and get the results of its elements;
  - `calculate_model_step`: start the calculation of a model object step as a background
  task, and get the typed ids of the dispatched jobs.
- `LogicParameters`, `LogicInput`, `LogicOutput` and `CalculationResult` schema models, in
the new `pyfxlib.schema.optimization` module and exported from `pyfxlib.schema`.
- Test tooling: `new_model_class` accepts an optional model class `definition`, and
`new_model_logic` creates a model evaluation or calculation logic.

## [1.3.0] - 2026-09-21

### Added
- Completing schema for Query API: adding `Pipeline`, `Stage` (and the union it is built from
`Source`, `JoinTable`, `JoinPipeline`, `AddColumns`, `RemoveColumns`, `RetainColumns`,
`SelectColumns`, `Filter`, `Aggregate`, `Distinct`, `Take`, `Sort`), together with the
expression, window function, function and selectable types the stages accept, and
`QueryAnswer` for a result set (`query`, `meta`, `rows`, plus `to_pandas()`).
- `PfxCamelCaseModel`, and `PfxPascalCaseModel`, the two base class every schema model now derives from. It
carries the camel/pascal wire to python that configuration of each model used to
repeat by hand, so subclasses declare only the configuration that differs. Exported from
`pyfxlib.schema` for anyone defining their own domain objects.

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
