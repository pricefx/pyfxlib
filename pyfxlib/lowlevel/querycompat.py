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
declared from 15.0.0, the first release of the endpoint and pyfxlib's minimum backend: the
15.0.0 baseline itself has no entry.

What a backend accepts is captured once per connection as a `QueryCompatibility`, built from
the backend version on the first query that needs it, and kept for the connection lifetime: a
backend upgraded meanwhile is only seen by a new connection.

Only queries sent through a connection are adapted: a `Pipeline` dumped and posted by other
means is sent as is.

Each change declares the backend versions it is available from as `VersionBounds`.

TODO: once pyfxlib requires 16.3.11 / 17.0.2, drop both renames, and the `datafeed` and
`datasource` spellings from `PADataFeed` and `PADataSource`.
"""

from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
import copy
from dataclasses import dataclass
from enum import StrEnum, unique
import logging
from types import MappingProxyType
from typing import Any

from pyfxlib.schema import BackendVersion

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class VersionBounds:
    """Backend versions a change is available from: one lower bound per major.

    Changes are backported per major, so versions are not monotonic across majors (17.0.1 is
    numerically higher than 16.3.11 yet may predate a change backported to both), but they are
    within one. A backend on a major newer than every bound is assumed to have the change, as
    cut from a develop that had it, and one on an older major not to.
    """

    by_major: Mapping[int, tuple[int, int, int]]

    def __post_init__(self) -> None:
        if not self.by_major or any(bound[0] != major for major, bound in self.by_major.items()):
            raise ValueError(
                f"Expected one lower bound per major, keyed by it, got {dict(self.by_major)}"
            )
        # Read-only, as the dataclass is frozen, and lowest major first.
        object.__setattr__(self, "by_major", MappingProxyType(dict(sorted(self.by_major.items()))))

    def supports(self, version: BackendVersion) -> bool:
        """Whether a backend at `version` has the change."""
        bound = self.by_major.get(version.major)
        if bound is None:
            return version.major > max(self.by_major)
        return (version.major, version.minor or 0, version.patch or 0) >= bound

    def __str__(self) -> str:
        """The bounds as "X.Y.Z or A.B.C"."""
        return " or ".join(".".join(map(str, bound)) for bound in self.by_major.values())


# The data feed and data source kinds changed together.
_PFUN_35498 = VersionBounds({16: (16, 3, 11), 17: (17, 0, 2)})


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


_TABLE = UsageCategory.TABLE
_KIND = UsageCategory.KIND
_FIELD = UsageCategory.FIELD

# Every change since 15.0.0, under its first X.Y.0 release, bounded by its backports.
_REQUIREMENTS = _declare(
    # 15.1.0
    (
        _TABLE,
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
        VersionBounds({15: (15, 0, 3)}),
        "PFUN-30634",
    ),
    (_KIND, ("windowFunction",), VersionBounds({15: (15, 0, 5)}), "PFUN-31214"),
    (_TABLE, ("actionItems",), VersionBounds({15: (15, 0, 6)}), "PFUN-31318"),
    (
        _KIND,
        (
            "inputColumnReference",
            "outputColumnReference",
            "calculationResultColumnReference",
            "activeCalculationResultColumnReference",
            "previousCalculationResultColumnReference",
        ),
        VersionBounds({15: (15, 0, 6)}),
        "PFUN-31318",
    ),
    (_KIND, ("selectColumns",), VersionBounds({15: (15, 1, 0)}), "PFUN-30390"),
    # 16.0.0
    (
        _TABLE,
        ("productCompetitions",),
        VersionBounds({15: (15, 3, 8), 16: (16, 0, 0)}),
        "PFUN-33178",
    ),
    # 16.2.0
    (
        _TABLE,
        (
            "productBillOfMaterials",
            "companyParameterTables",
            "claimLineItems",
            "customForms",
            "jobStatusTrackers",
        ),
        VersionBounds({15: (15, 3, 15), 16: (16, 1, 1)}),
        "PFUN-30357",
    ),
    # 16.3.0
    (
        _TABLE,
        ("productReferences",),
        VersionBounds({15: (15, 3, 16), 16: (16, 2, 1)}),
        "PFUN-33923",
    ),
    (_TABLE, ("claims",), VersionBounds({15: (15, 3, 17), 16: (16, 2, 2)}), "PFUN-34034"),
    # 17.0.0
    (
        _FIELD,
        ("companyParameterRows.targetDate",),
        VersionBounds({15: (15, 3, 22), 16: (16, 3, 6), 17: (17, 0, 0)}),
        "PFUN-34433",
    ),
    # 17.1.0
    (_FIELD, ("join.pipeline",), VersionBounds({16: (16, 3, 10), 17: (17, 0, 1)}), "PFUN-30794"),
    (
        _TABLE,
        ("historicalConditionRecords",),
        VersionBounds({16: (16, 3, 12), 17: (17, 0, 3)}),
        "PFUN-34151",
    ),
    (_TABLE, ("users",), VersionBounds({16: (16, 3, 16), 17: (17, 0, 7)}), "PFUN-27391"),
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


def _usages(node: Any, *, is_table: bool = False) -> Iterator[Usage]:
    """Yield everything a query node uses that may need a minimum backend version.

    Each table, stage and expression yields its kind, and each of its fields set to a value:
    a field left null is not used, as a model dumped without `exclude_none` sends it.
    """
    if isinstance(node, list):
        for item in node:
            yield from _usages(item)
    elif isinstance(node, dict):
        for key, value in node.items():
            yield from _usages(value, is_table=key == "table" and isinstance(value, dict))
        if isinstance(kind := node.get("kind"), str):
            yield Usage(UsageCategory.TABLE if is_table else UsageCategory.KIND, kind)
            for key, value in node.items():
                if key != "kind" and value is not None:
                    yield Usage(UsageCategory.FIELD, f"{kind}.{key}")


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
