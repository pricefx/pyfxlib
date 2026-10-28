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

"""Adapt Query API queries to the version of the backend they are sent to.

Some ids of the query DTO changed across backend versions, while pyfxlib supports backends
on both sides of the change. The connection hands every query through
`adapt_query_to_backend` before sending it, which rewrites each id the backend does not know
to the spelling it does, and logs every rewrite and every deprecated id sent, at debug level.

Anything else the backend does not know, with no older spelling to fall back on, is rejected
before sending, with an `UnsupportedQueryError` listing every such use. The requirements are
declared from 15.0.0, the first release of the endpoint: the 15.0.0 baseline itself has no
entry. Older backends pyfxlib still keeps the code for, deprecated, have no endpoint.

What a backend accepts is a `QueryCompatibility`, built from the backend version the
connection keeps: fetched on the first call needing it, and kept for the connection lifetime. A
backend upgraded meanwhile is only seen by a new connection.

Only queries sent through a connection are adapted, and so are the tables and table kinds its
`queryapi.meta` methods send: a `Pipeline` dumped and posted by other means is sent as is.

Each change declares the backend versions it is available from as `VersionBounds`.

Once `MIN_DEPRECATED_BACKEND_VERSION` rises past 15.0.0, unit tests fail on every requirement
and rename that all the backends pyfxlib keeps the code for meet, to drop: see CONTRIBUTING.md.
Dropping both renames also drops the `datafeed` and `datasource` spellings from `PADataFeed`
and `PADataSource`.
"""

from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
import copy
from dataclasses import dataclass
from enum import StrEnum, unique
import logging
from types import MappingProxyType
from typing import Any

from pyfxlib.schema import BackendVersion, VersionBounds

LOGGER = logging.getLogger(__name__)


# The data feed and data source kinds changed together.
_PFUN_35498 = VersionBounds("16.3.11", "17.0.2")


@dataclass(frozen=True)
class TableKindRename:
    """A table kind the backend renamed."""

    old: str
    new: str
    since: VersionBounds
    # Whether backends that know `new` still accept `old`, as a deprecated id.
    old_accepted_after_rename: bool
    ticket: str


_TABLE_KIND_RENAMES: tuple[TableKindRename, ...] = (
    TableKindRename(
        old="datafeed",
        new="dataFeed",
        since=_PFUN_35498,
        old_accepted_after_rename=False,
        ticket="PFUN-35498",
    ),
    TableKindRename(
        old="datasource",
        new="dataSource",
        since=_PFUN_35498,
        old_accepted_after_rename=True,
        ticket="PFUN-35498",
    ),
)

_RENAMES_BY_KIND: dict[str, TableKindRename] = {
    kind: rename for rename in _TABLE_KIND_RENAMES for kind in (rename.old, rename.new)
}


@unique
class UsageCategory(StrEnum):
    """What part of the Query API a query uses."""

    TABLE = "table"
    # A stage or expression kind.
    KIND = "kind"
    # A field of a table, stage or expression, named "<kind>.<field>".
    FIELD = "field"
    # A scalar function: not the one a window function applies, which `windowFunction` covers.
    FUNCTION = "function"
    LITERAL_TYPE = "literal type"


@dataclass(frozen=True)
class Usage:
    """One thing a query uses, which may need a minimum backend version."""

    category: UsageCategory
    name: str

    def __str__(self) -> str:
        """E.g. "table 'users'"."""
        return f"{self.category} {self.name!r}"


@dataclass(frozen=True)
class Requirement:
    """The backend versions a usage is available from, and the change that brought it."""

    since: VersionBounds
    ticket: str


def _declare(
    *groups: tuple[UsageCategory, tuple[str, ...], VersionBounds, str],
) -> Mapping[Usage, Requirement]:
    """Requirements of each (category, names, since, ticket) group, each usage declared once."""
    requirements: dict[Usage, Requirement] = {}
    for category, names, since, ticket in groups:
        for name in names:
            usage = Usage(category, name)
            if usage in requirements:
                raise ValueError(f"Requirement of {usage} declared twice")
            requirements[usage] = Requirement(since, ticket)
    return MappingProxyType(requirements)


# Every change since 15.0.0, under its first X.Y.0 release, bounded by its backports.
_REQUIREMENTS = _declare(
    # 15.1.0
    (
        UsageCategory.TABLE,
        (
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
        ),
        VersionBounds("15.0.3"),
        "PFUN-30634",
    ),
    (UsageCategory.KIND, ("windowFunction",), VersionBounds("15.0.5"), "PFUN-31214"),
    (UsageCategory.TABLE, ("actionItems",), VersionBounds("15.0.6"), "PFUN-31318"),
    (
        UsageCategory.KIND,
        (
            "inputColumnReference",
            "outputColumnReference",
            "calculationResultColumnReference",
            "activeCalculationResultColumnReference",
            "previousCalculationResultColumnReference",
        ),
        VersionBounds("15.0.6"),
        "PFUN-31318",
    ),
    (UsageCategory.KIND, ("selectColumns",), VersionBounds("15.1.0"), "PFUN-30390"),
    (
        UsageCategory.FUNCTION,
        (
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
        ),
        VersionBounds("15.1.0"),
        "PFUN-31317",
    ),
    # 16.0.0
    (
        UsageCategory.TABLE,
        ("productCompetitions",),
        VersionBounds("15.3.8", "16.0.0"),
        "PFUN-33178",
    ),
    # 16.2.0
    (
        UsageCategory.TABLE,
        (
            "productBillOfMaterials",
            "companyParameterTables",
            "claimLineItems",
            "customForms",
            "jobStatusTrackers",
        ),
        VersionBounds("15.3.15", "16.1.1"),
        "PFUN-30357",
    ),
    # 16.3.0
    (
        UsageCategory.TABLE,
        ("productReferences",),
        VersionBounds("15.3.16", "16.2.1"),
        "PFUN-33923",
    ),
    (
        UsageCategory.TABLE,
        ("claims",),
        VersionBounds("15.3.17", "16.2.2"),
        "PFUN-34034",
    ),
    (
        UsageCategory.FUNCTION,
        ("greatest", "least"),
        VersionBounds("15.3.17", "16.2.3"),
        "PFUN-33886",
    ),
    # 17.0.0
    (
        UsageCategory.FIELD,
        ("companyParameterRows.targetDate",),
        VersionBounds("15.3.22", "16.3.6", "17.0.0"),
        "PFUN-34433",
    ),
    (
        UsageCategory.FUNCTION,
        ("dateOnlyToday", "dateTimeNow"),
        VersionBounds("16.3.7", "17.0.0"),
        "PFUN-33284",
    ),
    (
        UsageCategory.LITERAL_TYPE,
        ("DURATION", "PERIOD"),
        VersionBounds("16.3.7", "17.0.0"),
        "PFUN-33284",
    ),
    # 17.1.0
    (
        UsageCategory.FIELD,
        ("join.pipeline",),
        VersionBounds("16.3.10", "17.0.1"),
        "PFUN-30794",
    ),
    (
        UsageCategory.TABLE,
        ("historicalConditionRecords",),
        VersionBounds("16.3.12", "17.0.3"),
        "PFUN-34151",
    ),
    (
        UsageCategory.TABLE,
        ("users",),
        VersionBounds("16.3.16", "17.0.7"),
        "PFUN-27391",
    ),
    # 17.2.0. PFUN-35990 shipped these in 16.3.16, 17.0.7 and 17.0.8 before reverting them in
    # 16.3.17, 17.0.9 and 17.1.0: those few releases are rejected too, on the safe side, as
    # VersionBounds has no upper bound and the reverted shape may differ.
    (
        UsageCategory.FUNCTION,
        ("safeDiv", "percentRankOver", "cumeDistOver"),
        VersionBounds("17.2.0"),
        "PFUN-36206",
    ),
)


class UnsupportedQueryError(RuntimeError):
    """A query uses what the backend it is sent to does not support."""

    def __init__(self, version: BackendVersion, unsupported: Mapping[Usage, Requirement]) -> None:
        self.version = version
        self.unsupported = dict(unsupported)
        uses = "; ".join(
            f"{usage}, supported from {requirement.since} ({requirement.ticket})"
            for usage, requirement in self.unsupported.items()
        )
        super().__init__(f"Query not supported by backend {version}: it uses {uses}")


def _tables(node: Any) -> Iterator[dict[str, Any]]:
    """Yield every table of a query, at any depth, joined pipelines included.

    The DTO only takes a table under a `table` key, in source and join stages.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "table" and isinstance(value, dict):
                yield value
            else:
                yield from _tables(value)
    elif isinstance(node, list):
        for item in node:
            yield from _tables(item)


def _usages(node: Any, *, is_table: bool = False, is_window_call: bool = False) -> Iterator[Usage]:
    """Yield everything a query node uses that may need a minimum backend version.

    Each table, stage and expression yields its kind, and each of its fields set to a value:
    a field left null is not used, as a model dumped without `exclude_none` sends it. Each
    function yields its name, but the one a window function applies, and each literal its
    type.
    """
    if isinstance(node, list):
        for item in node:
            yield from _usages(item)
    elif isinstance(node, dict):
        kind = node.get("kind")
        for key, value in node.items():
            yield from _usages(
                value,
                is_table=key == "table" and isinstance(value, dict),
                is_window_call=kind == "windowFunction" and key == "function",
            )
        if isinstance(kind, str):
            yield Usage(UsageCategory.TABLE if is_table else UsageCategory.KIND, kind)
            for key, value in node.items():
                if key != "kind" and value is not None:
                    yield Usage(UsageCategory.FIELD, f"{kind}.{key}")
            if (
                kind == "function"
                and not is_window_call
                and isinstance(name := node.get("name"), str)
            ):
                yield Usage(UsageCategory.FUNCTION, name)
            if kind == "literal" and isinstance(type_ := node.get("type"), str):
                yield Usage(UsageCategory.LITERAL_TYPE, type_)


@dataclass(frozen=True)
class QueryCompatibility:
    """What the Query API of a backend accepts, as far as pyfxlib knows it."""

    version: BackendVersion
    # Only the requirements this backend does not meet: everything else it supports.
    unsupported: Mapping[Usage, Requirement]

    @classmethod
    def for_backend(cls, version: BackendVersion) -> "QueryCompatibility":
        """The compatibility of a backend at `version`."""
        unsupported = {
            usage: requirement
            for usage, requirement in _REQUIREMENTS.items()
            if not requirement.since.supports(version)
        }
        return cls(version, MappingProxyType(unsupported))

    def check(self, usages: Iterable[Usage]) -> None:
        """Raise `UnsupportedQueryError` listing every usage the backend does not support."""
        unsupported = {
            usage: self.unsupported[usage] for usage in usages if usage in self.unsupported
        }
        if unsupported:
            raise UnsupportedQueryError(self.version, unsupported)

    def adapt_table(self, table: dict[str, Any], rename: TableKindRename) -> None:
        """Rewrite the kind of `table` to the spelling the backend knows, logging it."""
        kind = table["kind"]
        new_supported = rename.since.supports(self.version)
        if kind == rename.new and not new_supported:
            table["kind"] = rename.old
            LOGGER.debug(
                "Query table kind %r rewritten to %r: backend %s predates it (%s), which is"
                " supported from %s.",
                rename.new,
                rename.old,
                self.version,
                rename.ticket,
                rename.since,
            )
        elif kind == rename.old and new_supported:
            if rename.old_accepted_after_rename:
                LOGGER.debug(
                    "Query table kind %r is deprecated on backend %s (%s): set the table kind"
                    " to %r to stop this message.",
                    rename.old,
                    self.version,
                    rename.ticket,
                    rename.new,
                )
            else:
                table["kind"] = rename.new
                LOGGER.debug(
                    "Query table kind %r rewritten to %r: backend %s renamed it (%s).",
                    rename.old,
                    rename.new,
                    self.version,
                    rename.ticket,
                )


QueryCompatibilityProvider = Callable[[], Awaitable[QueryCompatibility]]


async def adapt_query_to_backend(
    query: dict[str, Any], compatibility: QueryCompatibilityProvider
) -> dict[str, Any]:
    """Return a copy of `query` adapted to the backend it is about to be sent to.

    `query` itself is never modified. The backend compatibility is only asked for when the
    query uses an id that changed across versions, or something that needs a minimum version.

    Args:
        query: the Query API query, as sent on the wire.
        compatibility: coroutine function returning the compatibility of the backend.
    Raises:
        UnsupportedQueryError: if the query uses anything the backend does not support.
    """
    adapted = copy.deepcopy(query)
    renamed = [
        (table, _RENAMES_BY_KIND[kind])
        for table in _tables(adapted)
        if isinstance(kind := table.get("kind"), str) and kind in _RENAMES_BY_KIND
    ]
    # Renamed kinds carry no requirement, so renaming them leaves this unchanged.
    gated = {usage for usage in _usages(adapted) if usage in _REQUIREMENTS}
    if renamed or gated:
        backend = await compatibility()
        backend.check(gated)
        for table, rename in renamed:
            backend.adapt_table(table, rename)
    return adapted


async def adapt_table_to_backend(
    table: dict[str, Any], compatibility: QueryCompatibilityProvider
) -> dict[str, Any]:
    """Return a copy of `table` adapted to the backend it is about to be sent to.

    The table is adapted and checked as it would be in a query, for the `queryapi.meta`
    endpoints that take a table on its own. `table` itself is never modified.

    Args:
        table: the Query API table, as sent on the wire.
        compatibility: coroutine function returning the compatibility of the backend.
    Raises:
        UnsupportedQueryError: if the table uses anything the backend does not support.
    """
    adapted = await adapt_query_to_backend({"table": table}, compatibility)
    return adapted["table"]
