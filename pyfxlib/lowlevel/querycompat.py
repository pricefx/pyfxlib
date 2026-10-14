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

Only queries sent through a connection are adapted: a `Pipeline` dumped and posted by other
means is sent as is.

Each change declares the backend versions it is available from as `VersionBounds`.

TODO: once pyfxlib requires 16.3.11 / 17.0.2, drop both renames, and the `datafeed` and
`datasource` spellings from `PADataFeed` and `PADataSource`. Before that, `PADataSource` can
move to sending `dataSource` by changing its default only: its rename already covers older
backends.
"""

from collections.abc import Awaitable, Callable, Iterator, Mapping
import copy
from dataclasses import dataclass
import logging
from types import MappingProxyType
from typing import Any

from pyfxlib.schema import BackendVersion

LOGGER = logging.getLogger(__name__)

BackendVersionProvider = Callable[[], Awaitable[BackendVersion]]


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


def _adapt_table(table: dict[str, Any], rename: TableKindRename, version: BackendVersion) -> None:
    """Rewrite the kind of `table` to the spelling the backend knows, logging it."""
    kind = table["kind"]
    new_supported = rename.since.supports(version)
    if kind == rename.new and not new_supported:
        table["kind"] = rename.old
        LOGGER.debug(
            "Query table kind %r rewritten to %r: backend %s predates it (%s), which is"
            " supported from %s.",
            rename.new,
            rename.old,
            version,
            rename.ticket,
            rename.since,
        )
    elif kind == rename.old and new_supported:
        if rename.old_accepted_after_rename:
            LOGGER.debug(
                "Query table kind %r is deprecated on backend %s (%s): set the table kind to %r"
                " to stop this message.",
                rename.old,
                version,
                rename.ticket,
                rename.new,
            )
        else:
            table["kind"] = rename.new
            LOGGER.debug(
                "Query table kind %r rewritten to %r: backend %s renamed it (%s).",
                rename.old,
                rename.new,
                version,
                rename.ticket,
            )


async def adapt_query_to_backend(
    query: dict[str, Any], fetch_backend_version: BackendVersionProvider
) -> dict[str, Any]:
    """Return a copy of `query` adapted to the backend it is about to be sent to.

    `query` itself is never modified. The backend version is fetched only when the query
    uses an id that changed across versions, and at most once.

    Args:
        query: the Query API query, as sent on the wire.
        fetch_backend_version: coroutine function returning the backend version.
    """
    adapted = copy.deepcopy(query)
    renamed = [
        (table, _RENAMES_BY_KIND[kind])
        for table in _tables(adapted)
        if isinstance(kind := table.get("kind"), str) and kind in _RENAMES_BY_KIND
    ]
    if renamed:
        version = await fetch_backend_version()
        for table, rename in renamed:
            _adapt_table(table, rename, version)
    return adapted
