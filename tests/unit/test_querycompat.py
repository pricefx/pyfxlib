# Copyright 2025-2026 Pricefx
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections.abc import Iterator, Set
import copy
import logging
from typing import Any, get_args

from pydantic import BaseModel
import pytest

from pyfxlib.lowlevel.querycompat import (
    _REQUIREMENTS,
    _TABLE_KIND_RENAMES,
    adapt_query_to_backend,
    QueryCompatibility,
    QueryCompatibilityProvider,
    UnsupportedQueryError,
    Usage,
    UsageCategory,
    VersionBounds,
)
from pyfxlib.schema import (
    BackendVersion,
    LiteralValueType,
    Operation,
    OperationAgg,
    RankOverOperation,
    Table,
)
import pyfxlib.schema.query as query_schema


class _Backend:
    """Compatibility provider of a backend at `version`, counting how many times it is asked."""

    def __init__(self, version: str) -> None:
        self.compatibility = QueryCompatibility.for_backend(BackendVersion.parse(version))
        self.calls = 0

    async def __call__(self) -> QueryCompatibility:
        self.calls += 1
        return self.compatibility


async def _never_asked() -> QueryCompatibility:
    raise AssertionError("the backend compatibility must not be asked for")


def _query(*tables: dict[str, Any]) -> dict[str, Any]:
    """A pipeline sourcing the first table, and joining each next one as a nested pipeline."""
    first, *joined = tables
    return {
        "kind": "pipeline",
        "stages": [
            {"kind": "source", "table": first},
            *(
                {
                    "kind": "join",
                    "type": "INNER",
                    "pipeline": {"kind": "pipeline", "stages": [{"kind": "source", "table": t}]},
                    "criteria": {"kind": "literal", "type": "BOOLEAN", "value": True},
                }
                for t in joined
            ),
        ],
    }


def _kinds(query: dict[str, Any]) -> list[str]:
    stages = query["stages"]
    return [stages[0]["table"]["kind"]] + [
        stage["pipeline"]["stages"][0]["table"]["kind"] for stage in stages[1:]
    ]


def _feed(kind: str) -> dict[str, Any]:
    return {"kind": kind, "dataFeedUniqueName": "DF"}


def _source(kind: str) -> dict[str, Any]:
    return {"kind": kind, "dataSourceUniqueName": "DS"}


# Backends on either side of PFUN-35498, which landed in 16.3.11 and 17.0.2.
_OLD_BACKENDS = ["15.2.0", "16.3.10", "17.0.1"]
_NEW_BACKENDS = ["16.3.11", "17.0.2", "18.0.0"]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", _OLD_BACKENDS)
@pytest.mark.parametrize("kind", ["dataFeed", "datafeed"])
async def test_data_feed_is_sent_as_datafeed_to_old_backends(version: str, kind: str) -> None:
    adapted = await adapt_query_to_backend(_query(_feed(kind)), _Backend(version))

    assert _kinds(adapted) == ["datafeed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", _NEW_BACKENDS)
@pytest.mark.parametrize("kind", ["dataFeed", "datafeed"])
async def test_data_feed_is_sent_as_data_feed_to_new_backends(version: str, kind: str) -> None:
    adapted = await adapt_query_to_backend(_query(_feed(kind)), _Backend(version))

    assert _kinds(adapted) == ["dataFeed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version,kind,expected",
    [
        *((version, "dataSource", "datasource") for version in _OLD_BACKENDS),
        *((version, "datasource", "datasource") for version in _OLD_BACKENDS),
        *((version, "dataSource", "dataSource") for version in _NEW_BACKENDS),
        # Deprecated but still accepted: sent as is, and logged.
        *((version, "datasource", "datasource") for version in _NEW_BACKENDS),
    ],
)
async def test_data_source_is_sent_as_a_kind_the_backend_knows(
    version: str, kind: str, expected: str
) -> None:
    adapted = await adapt_query_to_backend(_query(_source(kind)), _Backend(version))

    assert _kinds(adapted) == [expected]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version,kind,message",
    [
        ("17.0.1", "dataFeed", "'dataFeed' rewritten to 'datafeed'"),
        ("17.0.2", "datafeed", "'datafeed' rewritten to 'dataFeed'"),
        ("16.3.10", "dataSource", "'dataSource' rewritten to 'datasource'"),
        ("16.3.11", "datasource", "'datasource' is deprecated"),
    ],
)
async def test_each_rewrite_and_deprecated_kind_logs_one_debug_message(
    version: str, kind: str, message: str, caplog: pytest.LogCaptureFixture
) -> None:
    table = _feed(kind) if kind.lower() == "datafeed" else _source(kind)

    with caplog.at_level(logging.DEBUG, logger="pyfxlib.lowlevel.querycompat"):
        await adapt_query_to_backend(_query(table), _Backend(version))

    assert [record.levelno for record in caplog.records] == [logging.DEBUG]
    assert message in caplog.records[0].getMessage()
    assert version in caplog.records[0].getMessage()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version,table", [("17.0.2", _feed("dataFeed")), ("16.3.10", _source("datasource"))]
)
async def test_a_kind_the_backend_knows_logs_nothing(
    version: str, table: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="pyfxlib.lowlevel.querycompat"):
        await adapt_query_to_backend(_query(table), _Backend(version))

    assert caplog.records == []


@pytest.mark.asyncio
async def test_tables_of_joined_pipelines_are_adapted_too() -> None:
    query = _query({"kind": "products"}, _feed("dataFeed"), _source("dataSource"))

    adapted = await adapt_query_to_backend(query, _Backend("16.3.10"))

    assert _kinds(adapted) == ["products", "datafeed", "datasource"]


@pytest.mark.asyncio
async def test_the_caller_query_is_never_modified() -> None:
    query = _query(_feed("dataFeed"), _source("dataSource"))
    original = copy.deepcopy(query)

    adapted = await adapt_query_to_backend(query, _Backend("16.3.10"))

    assert query == original
    assert adapted != query


@pytest.mark.asyncio
async def test_the_compatibility_is_asked_for_once_per_query() -> None:
    backend = _Backend("16.3.10")

    await adapt_query_to_backend(_query(_feed("dataFeed"), _source("datasource")), backend)

    assert backend.calls == 1


@pytest.mark.asyncio
async def test_the_compatibility_is_not_asked_for_a_15_0_0_query() -> None:
    """Nothing renamed and nothing past the 15.0.0 baseline: no need to know the backend."""
    never: QueryCompatibilityProvider = _never_asked
    query = {
        "kind": "pipeline",
        "stages": [
            {"kind": "source", "table": {"kind": "products"}},
            {
                "kind": "join",
                "type": "LEFT_OUTER",
                "table": {"kind": "datamart", "datamartUniqueName": "DM"},
                "criteria": {"kind": "literal", "type": "BOOLEAN", "value": True},
            },
            {"kind": "take", "count": 10},
        ],
    }

    assert await adapt_query_to_backend(query, never) == query


_BOUNDS = VersionBounds({17: (17, 0, 2), 16: (16, 3, 11)})


@pytest.mark.parametrize(
    "version,supported",
    [
        ("15.9.9", False),  # older major than every bound
        ("16.3.10", False),
        ("16.3.11", True),
        ("16.4", True),
        # numerically higher than 16.3.11, but on the 17 major
        ("17.0.1", False),
        ("17.0.2", True),
        ("18.0.0", True),  # newer major than every bound
    ],
)
def test_version_bounds_compare_within_the_major_only(version: str, supported: bool) -> None:
    assert _BOUNDS.supports(BackendVersion.parse(version)) is supported


def test_version_bounds_are_kept_lowest_major_first() -> None:
    assert list(_BOUNDS.by_major) == [16, 17]
    assert str(_BOUNDS) == "16.3.11 or 17.0.2"


@pytest.mark.parametrize(
    "by_major", [{}, {16: (17, 0, 2)}], ids=["no bound", "bound on another major"]
)
def test_version_bounds_require_one_bound_per_major_keyed_by_it(
    by_major: dict[int, tuple[int, int, int]],
) -> None:
    with pytest.raises(ValueError, match="one lower bound per major"):
        VersionBounds(by_major)


def test_version_bounds_do_not_follow_changes_to_the_mapping_they_were_built_from() -> None:
    by_major = {16: (16, 3, 11)}
    bounds = VersionBounds(by_major)

    by_major[16] = (16, 0, 0)

    assert not bounds.supports(BackendVersion.parse("16.2.0"))


# The QueryApiDTO version history, written out independently of `_REQUIREMENTS`: for each
# table, releases that have it, and releases just before it on each backport line.
_TABLE_HISTORY: list[tuple[str, list[str], list[str]]] = [
    *(
        (table, ["15.0.3", "15.1.0", "16.0.0"], ["15.0.2"])
        for table in (
            "customers",
            "sellers",
            "customerExtensionRows",
            "sellerExtensionRows",
            "conditionRecords",
            "quoteLineItems",
            "rebateLineItems",
            "contractLineItems",
            "compensationLineItems",
            "calculationGrids",
            "calculationGridLineItems",
            "priceLists",
            "priceListLineItems",
            "priceGrids",
            "priceGridLineItems",
            "rebateRecords",
            "compensationRecords",
            "modelTable",
            "rollup",
        )
    ),
    ("actionItems", ["15.0.6", "15.1.0"], ["15.0.5"]),
    ("productCompetitions", ["15.3.8", "16.0.0"], ["15.3.7"]),
    *(
        (table, ["15.3.15", "16.1.1", "16.2.0"], ["15.3.14", "16.1.0"])
        for table in (
            "productBillOfMaterials",
            "companyParameterTables",
            "claimLineItems",
            "customForms",
            "jobStatusTrackers",
        )
    ),
    ("productReferences", ["15.3.16", "16.2.1", "16.3.0"], ["15.3.15", "16.2.0"]),
    ("claims", ["15.3.17", "16.2.2", "16.3.0"], ["15.3.16", "16.2.1"]),
    (
        "historicalConditionRecords",
        ["16.3.12", "17.0.3", "17.1.0"],
        ["16.3.11", "17.0.2", "15.3.30"],
    ),
    ("users", ["16.3.16", "17.0.7", "17.1.0"], ["16.3.15", "17.0.6", "15.3.30"]),
]


def _table(kind: str) -> dict[str, Any]:
    return {"kind": kind}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,version",
    [(kind, version) for kind, supported, _ in _TABLE_HISTORY for version in supported],
)
async def test_a_table_is_accepted_from_the_release_that_brought_it(
    kind: str, version: str
) -> None:
    query = _query(_table(kind))

    assert await adapt_query_to_backend(query, _Backend(version)) == query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,version",
    [(kind, version) for kind, _, unsupported in _TABLE_HISTORY for version in unsupported],
)
async def test_a_table_is_rejected_by_backends_predating_it(kind: str, version: str) -> None:
    with pytest.raises(UnsupportedQueryError, match=f"table '{kind}'") as raised:
        await adapt_query_to_backend(_query(_table(kind)), _Backend(version))

    assert raised.value.version == BackendVersion.parse(version)
    assert list(raised.value.unsupported) == [Usage(UsageCategory.TABLE, kind)]


@pytest.mark.asyncio
async def test_every_unsupported_table_is_reported_at_once_joined_pipelines_included() -> None:
    query = _query(_table("products"), _table("users"), _table("historicalConditionRecords"))

    with pytest.raises(UnsupportedQueryError) as raised:
        await adapt_query_to_backend(query, _Backend("16.3.11"))

    assert set(raised.value.unsupported) == {
        Usage(UsageCategory.TABLE, "users"),
        Usage(UsageCategory.TABLE, "historicalConditionRecords"),
    }
    message = str(raised.value)
    assert "backend 16.3.11" in message
    assert "table 'users', supported from 16.3.16 or 17.0.7 (PFUN-27391)" in message
    assert "table 'historicalConditionRecords', supported from 16.3.12 or 17.0.3" in message


def test_renamed_table_kinds_carry_no_requirement() -> None:
    """Rewriting them must not change what the query is checked against."""
    renamed = {kind for rename in _TABLE_KIND_RENAMES for kind in (rename.old, rename.new)}

    assert not {Usage(UsageCategory.TABLE, kind) for kind in renamed} & set(_REQUIREMENTS)


def _required(category: UsageCategory) -> set[str]:
    return {usage.name for usage in _REQUIREMENTS if usage.category is category}


def _assert_baseline_or_required(
    schema: Set[str],
    baseline: Set[str],
    category: UsageCategory,
    *,
    exempt: Set[str] = frozenset(),
) -> None:
    """Fail on any name of the schema neither in the baseline, exempt, nor required."""
    __tracebackhide__ = True
    missing = schema - baseline - exempt - _required(category)
    assert not missing, (
        f"{missing} added to the schema without a requirement: declare in"
        " `querycompat._REQUIREMENTS` the backend versions supporting it, or add it to the"
        " minimum version baseline here if the endpoint always had it."
    )


def _table_kinds() -> set[str]:
    return {
        kind
        for model in get_args(Table)
        for kind in get_args(model.model_fields["kind"].annotation)
    }


# What the endpoint shipped with in 15.0.0, which needs no requirement.
_BASELINE_TABLES = {"products", "productExtensionRows", "companyParameterRows", "datamart"}


def test_every_schema_table_kind_is_baseline_renamed_or_required() -> None:
    """A table added to the schema without a requirement fails here."""
    renamed = {kind for rename in _TABLE_KIND_RENAMES for kind in (rename.old, rename.new)}

    _assert_baseline_or_required(
        _table_kinds(), _BASELINE_TABLES, UsageCategory.TABLE, exempt=renamed
    )


def _schema_kinds() -> Iterator[tuple[str, type[BaseModel]]]:
    """Yield every table, stage and expression kind of the schema, with its model."""
    for model in vars(query_schema).values():
        if (
            isinstance(model, type)
            and issubclass(model, BaseModel)
            and "kind" in model.model_fields
        ):
            for kind in get_args(model.model_fields["kind"].annotation):
                yield kind, model


# What the endpoint shipped with in 15.0.0, which needs no requirement.
_BASELINE_KINDS = {
    *("pipeline", "source", "join", "filter", "aggregate", "distinct", "sort", "take"),
    *("addColumns", "removeColumns", "retainColumns"),
    *("columnReference", "selectable", "function", "literal", "order"),
}


def test_every_schema_stage_and_expression_kind_is_baseline_or_required() -> None:
    """A stage or expression added to the schema without a requirement fails here."""
    schema_kinds = {kind for kind, _ in _schema_kinds()} - _table_kinds()

    _assert_baseline_or_required(schema_kinds, _BASELINE_KINDS, UsageCategory.KIND)


# What the endpoint shipped with in 15.0.0, which needs no requirement, as "<kind>.<field>".
_BASELINE_FIELDS = {
    "pipeline.stages",
    *("source.table", "source.columns", "source.criteria"),
    *("join.type", "join.table", "join.columns", "join.criteria"),
    "filter.criteria",
    *("aggregate.columns", "aggregate.dimensions"),
    "sort.orders",
    "take.count",
    "addColumns.columns",
    "removeColumns.columns",
    "retainColumns.columns",
    *("columnReference.source", "columnReference.column"),
    *("selectable.expression", "selectable.alias"),
    *("function.name", "function.arguments"),
    *("literal.type", "literal.value"),
    *("order.expression", "order.direction"),
    "productExtensionRows.productExtensionName",
    "companyParameterRows.companyParameterUniqueName",
    *("datamart.datamartUniqueName", "datamart.currency", "datamart.uom"),
    *("datafeed.dataFeedUniqueName", "dataFeed.dataFeedUniqueName"),
    *("datasource.dataSourceUniqueName", "dataSource.dataSourceUniqueName"),
}


def test_every_schema_field_is_baseline_or_required() -> None:
    """A field added to the schema without a requirement fails here.

    Fields of a table or kind with a requirement come with it, so need none of their own: a
    field added to one after it shipped is not caught.
    """
    gated = _required(UsageCategory.TABLE) | _required(UsageCategory.KIND)
    fields = {
        f"{kind}.{field.serialization_alias or field.alias or name}"
        for kind, model in _schema_kinds()
        if kind not in gated
        for name, field in model.model_fields.items()
        if name != "kind"
    }

    _assert_baseline_or_required(fields, _BASELINE_FIELDS, UsageCategory.FIELD)


def _products_with(*stages: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "pipeline",
        "stages": [{"kind": "source", "table": _table("products")}, *stages],
    }


def _column(name: str) -> dict[str, Any]:
    return {"kind": "columnReference", "source": "previousStage", "column": name}


_WINDOW_RANK = {
    "kind": "windowFunction",
    "frame": {
        "type": "rows",
        "start": {"type": "unbounded", "direction": "PRECEDING"},
        "end": {"type": "currentRow"},
        "exclusion": "NONE",
    },
    "orders": [{"kind": "order", "expression": _column("sku"), "direction": "ASC_NULLS_LAST"}],
    "function": {"kind": "function", "name": "rank", "arguments": []},
}


def _add_column(expression: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "addColumns",
        "columns": [{"kind": "selectable", "expression": expression, "alias": "x"}],
    }


def _source_column(kind: str, **fields: str) -> dict[str, Any]:
    return {
        "kind": "pipeline",
        "stages": [
            {
                "kind": "source",
                "table": {"kind": "quoteLineItems", "quoteUniqueName": "Q"},
                "columns": [
                    {
                        "kind": "selectable",
                        "expression": {"kind": kind, "source": "table", **fields},
                        "alias": "x",
                    }
                ],
            }
        ],
    }


def _company_parameter_rows(target_date: str | None) -> dict[str, Any]:
    table = {
        "kind": "companyParameterRows",
        "companyParameterUniqueName": "CP",
        "targetDate": target_date,
    }
    return {"kind": "pipeline", "stages": [{"kind": "source", "table": table}]}


_JOIN_PIPELINE = _products_with(
    {
        "kind": "join",
        "type": "INNER",
        "pipeline": {
            "kind": "pipeline",
            "stages": [{"kind": "source", "table": _table("products")}],
        },
        "criteria": {"kind": "literal", "type": "BOOLEAN", "value": True},
    }
)

# Query using the kind or field, the usage it reports, releases having it, releases predating it.
_KIND_AND_FIELD_HISTORY: list[tuple[dict[str, Any], Usage, list[str], list[str]]] = [
    (
        _products_with(_add_column(_WINDOW_RANK)),
        Usage(UsageCategory.KIND, "windowFunction"),
        ["15.0.5", "15.1.0"],
        ["15.0.4"],
    ),
    *(
        (
            _source_column(kind, **fields),
            Usage(UsageCategory.KIND, kind),
            ["15.0.6", "15.1.0"],
            ["15.0.5"],
        )
        for kind, fields in (
            ("inputColumnReference", {"inputName": "I", "column": "c"}),
            ("outputColumnReference", {"elementName": "E", "column": "c"}),
            ("calculationResultColumnReference", {"elementName": "E"}),
            ("activeCalculationResultColumnReference", {"elementName": "E"}),
            ("previousCalculationResultColumnReference", {"elementName": "E"}),
        )
    ),
    (
        _products_with(
            {
                "kind": "selectColumns",
                "columns": [{"kind": "selectable", "expression": _column("sku"), "alias": "s"}],
            }
        ),
        Usage(UsageCategory.KIND, "selectColumns"),
        ["15.1.0", "16.0.0"],
        ["15.0.6"],
    ),
    (
        _company_parameter_rows("2026-10-14"),
        Usage(UsageCategory.FIELD, "companyParameterRows.targetDate"),
        ["15.3.22", "16.3.6", "17.0.0"],
        ["15.3.21", "16.3.5"],
    ),
    (
        _JOIN_PIPELINE,
        Usage(UsageCategory.FIELD, "join.pipeline"),
        ["16.3.10", "17.0.1", "17.1.0"],
        ["16.3.9", "17.0.0", "15.3.30"],
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query,version",
    [
        (query, version)
        for query, _, supported, _ in _KIND_AND_FIELD_HISTORY
        for version in supported
    ],
)
async def test_a_kind_or_field_is_accepted_from_the_release_that_brought_it(
    query: dict[str, Any], version: str
) -> None:
    assert await adapt_query_to_backend(query, _Backend(version)) == query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query,usage,version",
    [
        (query, usage, version)
        for query, usage, _, unsupported in _KIND_AND_FIELD_HISTORY
        for version in unsupported
    ],
)
async def test_a_kind_or_field_is_rejected_by_backends_predating_it(
    query: dict[str, Any], usage: Usage, version: str
) -> None:
    with pytest.raises(UnsupportedQueryError) as raised:
        await adapt_query_to_backend(query, _Backend(version))

    assert usage in raised.value.unsupported


@pytest.mark.asyncio
async def test_a_null_field_is_not_a_use_of_it() -> None:
    """A model dumped without `exclude_none` sends unset fields as null."""
    query = _company_parameter_rows(None)

    assert await adapt_query_to_backend(query, _never_asked) == query


@pytest.mark.asyncio
async def test_a_join_against_a_table_needs_no_recent_backend() -> None:
    query = _products_with(
        {
            "kind": "join",
            "type": "INNER",
            "table": _table("products"),
            "criteria": {"kind": "literal", "type": "BOOLEAN", "value": True},
        }
    )

    assert await adapt_query_to_backend(query, _never_asked) == query


_ORDER = {"kind": "order", "expression": _column("sku"), "direction": "ASC_NULLS_LAST"}


def _function_query(name: str) -> dict[str, Any]:
    arguments = [_ORDER] if name in ("percentRankOver", "cumeDistOver") else []
    return _products_with(_add_column({"kind": "function", "name": name, "arguments": arguments}))


_MATH_FUNCTIONS = (
    "acos",
    "asin",
    "atan",
    "atan2",
    "cos",
    "sin",
    "tan",
    "cbrt",
    "ceil",
    "floor",
    "degrees",
    "radians",
    "factorial",
    "ln",
    "log",
    "pi",
    "power",
    "random",
    "round",
    "sign",
    "trunc",
    "width_bucket",
)

# Function, releases having it, releases predating it.
_FUNCTION_HISTORY: list[tuple[str, list[str], list[str]]] = [
    *((name, ["15.1.0", "16.0.0"], ["15.0.6"]) for name in _MATH_FUNCTIONS),
    *(
        (name, ["15.3.17", "16.2.3", "16.3.0"], ["15.3.16", "16.2.2"])
        for name in ("greatest", "least")
    ),
    *(
        (name, ["16.3.7", "17.0.0"], ["16.3.6", "15.3.30"])
        for name in ("dateOnlyToday", "dateTimeNow")
    ),
    # PFUN-35990 had them in 16.3.16, 17.0.7 and 17.0.8 before reverting them: rejected too.
    *(
        (name, ["17.2.0", "18.0.0"], ["16.3.16", "16.3.22", "17.0.7", "17.0.8", "17.1.4"])
        for name in ("safeDiv", "percentRankOver", "cumeDistOver")
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,version",
    [(name, version) for name, supported, _ in _FUNCTION_HISTORY for version in supported],
)
async def test_a_function_is_accepted_from_the_release_that_brought_it(
    name: str, version: str
) -> None:
    query = _function_query(name)

    assert await adapt_query_to_backend(query, _Backend(version)) == query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,version",
    [(name, version) for name, _, unsupported in _FUNCTION_HISTORY for version in unsupported],
)
async def test_a_function_is_rejected_by_backends_predating_it(name: str, version: str) -> None:
    with pytest.raises(UnsupportedQueryError) as raised:
        await adapt_query_to_backend(_function_query(name), _Backend(version))

    assert list(raised.value.unsupported) == [Usage(UsageCategory.FUNCTION, name)]


@pytest.mark.asyncio
@pytest.mark.parametrize("type_,value", [("DURATION", "PT1H"), ("PERIOD", "P1M")])
@pytest.mark.parametrize(
    "version,supported", [("16.3.7", True), ("17.0.0", True), ("16.3.6", False), ("15.3.30", False)]
)
async def test_duration_and_period_literals_need_relative_dates(
    type_: str, value: str, version: str, supported: bool
) -> None:
    query = _products_with(_add_column({"kind": "literal", "type": type_, "value": value}))

    if supported:
        assert await adapt_query_to_backend(query, _Backend(version)) == query
    else:
        with pytest.raises(UnsupportedQueryError, match=f"literal type '{type_}'"):
            await adapt_query_to_backend(query, _Backend(version))


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["rank", "percentRank", "cumeDist", "sum"])
async def test_the_function_a_window_applies_is_not_checked_as_a_scalar_one(name: str) -> None:
    """Window functions date from 15.0.5, whatever scalar function shares their name."""
    window = {**_WINDOW_RANK, "function": {"kind": "function", "name": name, "arguments": []}}
    query = _products_with(_add_column(window))

    assert await adapt_query_to_backend(query, _Backend("15.0.5")) == query


@pytest.mark.asyncio
async def test_scalar_functions_in_window_arguments_are_checked() -> None:
    power = {"kind": "function", "name": "power", "arguments": [_column("price"), _column("qty")]}
    window = {**_WINDOW_RANK, "function": {"kind": "function", "name": "sum", "arguments": [power]}}

    with pytest.raises(UnsupportedQueryError) as raised:
        await adapt_query_to_backend(_products_with(_add_column(window)), _Backend("15.0.6"))

    assert list(raised.value.unsupported) == [Usage(UsageCategory.FUNCTION, "power")]


# What the endpoint shipped with in 15.0.0, which needs no requirement.
_BASELINE_FUNCTIONS = {
    *("and", "or", "not"),
    *("sum", "avg", "min", "max", "countAll", "countNonNull", "countDistinctNonNull"),
    *("caseWhen", "coalesce", "nullIf"),
    *("equal", "notEqual", "equalNullAware", "notEqualNullAware"),
    *("lessThan", "lessOrEqual", "greaterThan", "greaterOrEqual"),
    *("like", "ilike", "notLike", "notIlike"),
    *("in", "notIn", "isNull", "isNotNull", "isTrue", "isFalse"),
    *("concat", "substring", "upper", "lower", "trim", "length", "locate"),
    *("plus", "minus", "multiply", "div", "mod", "abs", "exp"),
    *("castAsInteger", "castAsReal", "castAsString", "castAsBoolean"),
    *("castAsDateOnly", "castAsDateTime"),
}
_BASELINE_LITERAL_TYPES = {"INTEGER", "REAL", "DATE_ONLY", "DATETIME", "STRING", "BOOLEAN"}


def test_the_baseline_has_the_51_functions_of_15_0_0() -> None:
    assert len(_BASELINE_FUNCTIONS) == 51
    assert not _BASELINE_FUNCTIONS & _required(UsageCategory.FUNCTION)


def test_every_schema_function_is_baseline_or_required() -> None:
    """A function added to the schema without a requirement fails here."""
    functions = {
        member.value for enum in (Operation, OperationAgg, RankOverOperation) for member in enum
    }

    _assert_baseline_or_required(functions, _BASELINE_FUNCTIONS, UsageCategory.FUNCTION)


def test_every_schema_literal_type_is_baseline_or_required() -> None:
    """A literal type added to the schema without a requirement fails here."""
    literal_types = {member.value for member in LiteralValueType}

    _assert_baseline_or_required(literal_types, _BASELINE_LITERAL_TYPES, UsageCategory.LITERAL_TYPE)
