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
from typing import Any

import pytest

from pyfxlib.lowlevel.querycompat import (
    adapt_query_to_backend,
    BackendVersionProvider,
    VersionBounds,
)
from pyfxlib.schema import BackendVersion


class _VersionFetcher:
    """Backend version provider counting how many times the version is fetched."""

    def __init__(self, version: str) -> None:
        self.version = BackendVersion.parse(version)
        self.calls = 0

    async def __call__(self) -> BackendVersion:
        self.calls += 1
        return self.version


async def _never_fetched() -> BackendVersion:
    raise AssertionError("the backend version must not be fetched")


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
    adapted = await adapt_query_to_backend(_query(_feed(kind)), _VersionFetcher(version))

    assert _kinds(adapted) == ["datafeed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", _NEW_BACKENDS)
@pytest.mark.parametrize("kind", ["dataFeed", "datafeed"])
async def test_data_feed_is_sent_as_data_feed_to_new_backends(version: str, kind: str) -> None:
    adapted = await adapt_query_to_backend(_query(_feed(kind)), _VersionFetcher(version))

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
    adapted = await adapt_query_to_backend(_query(_source(kind)), _VersionFetcher(version))

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
        await adapt_query_to_backend(_query(table), _VersionFetcher(version))

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
        await adapt_query_to_backend(_query(table), _VersionFetcher(version))

    assert caplog.records == []


@pytest.mark.asyncio
async def test_tables_of_joined_pipelines_are_adapted_too() -> None:
    query = _query({"kind": "products"}, _feed("dataFeed"), _source("dataSource"))

    adapted = await adapt_query_to_backend(query, _VersionFetcher("16.2.0"))

    assert _kinds(adapted) == ["products", "datafeed", "datasource"]


@pytest.mark.asyncio
async def test_the_caller_query_is_never_modified() -> None:
    query = _query(_feed("dataFeed"), _source("dataSource"))
    original = copy.deepcopy(query)

    adapted = await adapt_query_to_backend(query, _VersionFetcher("16.2.0"))

    assert query == original
    assert adapted != query


@pytest.mark.asyncio
async def test_the_version_is_fetched_once_per_query() -> None:
    fetcher = _VersionFetcher("16.2.0")

    await adapt_query_to_backend(_query(_feed("dataFeed"), _source("datasource")), fetcher)

    assert fetcher.calls == 1


@pytest.mark.asyncio
async def test_the_version_is_not_fetched_without_a_renamed_table() -> None:
    never: BackendVersionProvider = _never_fetched
    query = _query({"kind": "products"}, {"kind": "customers"})

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
