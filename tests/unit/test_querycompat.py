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

import copy
import logging
from typing import Any, get_args

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
from pyfxlib.schema import BackendVersion, Table


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

    adapted = await adapt_query_to_backend(query, _Backend("16.2.0"))

    assert _kinds(adapted) == ["products", "datafeed", "datasource"]


@pytest.mark.asyncio
async def test_the_caller_query_is_never_modified() -> None:
    query = _query(_feed("dataFeed"), _source("dataSource"))
    original = copy.deepcopy(query)

    adapted = await adapt_query_to_backend(query, _Backend("16.2.0"))

    assert query == original
    assert adapted != query


@pytest.mark.asyncio
async def test_the_compatibility_is_asked_for_once_per_query() -> None:
    backend = _Backend("16.2.0")

    await adapt_query_to_backend(_query(_feed("dataFeed"), _source("datasource")), backend)

    assert backend.calls == 1


@pytest.mark.asyncio
async def test_the_compatibility_is_not_asked_for_a_15_0_0_query() -> None:
    """Nothing renamed and nothing past the 15.0.0 baseline: no need to know the backend."""
    never: QueryCompatibilityProvider = _never_asked
    query = _query({"kind": "products"}, {"kind": "datamart", "datamartUniqueName": "DM"})

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


# What the endpoint shipped with in 15.0.0, which needs no requirement.
_BASELINE_TABLES = {"products", "productExtensionRows", "companyParameterRows", "datamart"}


def test_every_schema_table_kind_is_baseline_renamed_or_required() -> None:
    """A table added to the schema without a requirement fails here."""
    schema_kinds = {
        kind
        for model in get_args(Table)
        for kind in get_args(model.model_fields["kind"].annotation)
    }
    renamed = {kind for rename in _TABLE_KIND_RENAMES for kind in (rename.old, rename.new)}
    required = {usage.name for usage in _REQUIREMENTS if usage.category is UsageCategory.TABLE}

    assert schema_kinds - _BASELINE_TABLES - renamed - required == set()
