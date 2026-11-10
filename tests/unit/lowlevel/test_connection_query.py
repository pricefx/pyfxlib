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

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
import copy
import logging
from typing import Any

from httpx import Response
import pytest

from tests.unit.backend_versions import supported_by_every_backend_from

from pyfxlib.lowlevel.connection import (
    ConnectionRemote,
    NOTIFICATIONS_SINCE,
    QUERY_META_TABLES_SINCE,
)
from pyfxlib.lowlevel.constants import _DEFAULT_STREAM_CHUNK_SIZE
from pyfxlib.lowlevel.querycompat import QueryCompatibility, UnsupportedQueryError
from pyfxlib.lowlevel.session import PfxSession
from pyfxlib.schema import (
    BackendVersion,
    MIN_DEPRECATED_BACKEND_VERSION,
    MIN_SUPPORTED_BACKEND_VERSION,
    Notification,
    NotificationStatus,
    NotificationTopic,
)

_ENDPOINT = "https://pfx.test/pricefx/partition"
_TABLE_ENTRY = {"table": {"kind": "datafeed", "dataFeedUniqueName": "DF"}, "description": "Feed"}
_COLUMN_ENTRY = {"name": "sku", "type": "STRING"}


class _FakeSession(PfxSession):
    """Session recording the requests posted, and answering as a backend at `release`.

    The `queryapi.meta` table endpoints answer, as the backend does, with a single block, here
    holding `meta_entries` entries.
    """

    def __init__(self, release: str, meta_entries: int = 2) -> None:
        self.release = release
        self.meta_entries = meta_entries
        self.posted: list[tuple[str, Any]] = []

    async def post(self, url: str, **kwargs: Any) -> Response:
        self.posted.append((url.removeprefix(_ENDPOINT), kwargs.get("json")))
        # Yields to the event loop, as a real request does, so concurrent calls interleave.
        await asyncio.sleep(0)
        if url.endswith("/login/extended"):
            data: list[Any] = [{"extendedData": {"Release": self.release}}]
        elif url.endswith("/queryapi.meta"):
            data = [{"columns": []}]
        elif url.endswith("/queryapi.meta/tables"):
            data = [{"tables": [_TABLE_ENTRY] * self.meta_entries}]
        elif url.endswith("/queryapi.meta/table"):
            data = [{"columns": [_COLUMN_ENTRY] * self.meta_entries}]
        else:
            data = []
        return Response(200, json={"response": {"data": data}})

    def sent_queries(self) -> list[Any]:
        return [
            body["data"]["query"]
            for url, body in self.posted
            if url in ("/queryapi.meta", "/queryapi.execute")
        ]

    def sent_data(self, url: str) -> list[Any]:
        return [body["data"] for posted_url, body in self.posted if posted_url == url]

    def version_fetches(self) -> int:
        return sum(url == "/login/extended" for url, _ in self.posted)

    # Not used by queries.

    async def get(self, url: str, **kwargs: Any) -> Response:
        raise NotImplementedError

    async def post_simple(self, url: str, **kwargs: Any) -> Response:
        raise NotImplementedError

    def add_request_hook(self, hook: Callable[[str, str, dict[str, Any]], None]) -> None:
        raise NotImplementedError

    def add_response_hook(self, hook: Callable[[Response], None]) -> None:
        raise NotImplementedError

    async def get_stream(
        self, url: str, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError
        yield b""  # makes it an async generator, as the abstract method is

    def has_header(self, key: str) -> bool:
        raise NotImplementedError

    def set_header(self, key: str, value: str) -> None:
        raise NotImplementedError

    def remove_header(self, key: str) -> None:
        raise NotImplementedError


def _query(table: dict[str, Any]) -> dict[str, Any]:
    return {"kind": "pipeline", "stages": [{"kind": "source", "table": table}]}


_DATA_FEED = {"kind": "dataFeed", "dataFeedUniqueName": "DF"}

QueryMethod = Callable[[ConnectionRemote, dict[str, Any]], Awaitable[Any]]
_QUERY_METHODS = pytest.mark.parametrize(
    "method", [ConnectionRemote.query_meta, ConnectionRemote.query_execute], ids=["meta", "execute"]
)


@pytest.mark.asyncio
@_QUERY_METHODS
@pytest.mark.parametrize("release,kind", [("16.2.0", "datafeed"), ("17.0.2", "dataFeed")])
async def test_queries_are_sent_with_the_kinds_the_backend_knows(
    method: QueryMethod, release: str, kind: str
) -> None:
    session = _FakeSession(release)
    connection = ConnectionRemote(_ENDPOINT, session)

    await method(connection, _query(_DATA_FEED))

    assert [query["stages"][0]["table"]["kind"] for query in session.sent_queries()] == [kind]


@pytest.mark.asyncio
@_QUERY_METHODS
async def test_the_caller_query_is_left_untouched(method: QueryMethod) -> None:
    connection = ConnectionRemote(_ENDPOINT, _FakeSession("16.2.0"))
    query = _query(_DATA_FEED)
    original = copy.deepcopy(query)

    await method(connection, query)

    assert query == original


@pytest.mark.asyncio
@_QUERY_METHODS
async def test_a_query_without_renamed_tables_does_not_fetch_the_version(
    method: QueryMethod,
) -> None:
    session = _FakeSession("16.2.0")
    connection = ConnectionRemote(_ENDPOINT, session)
    query = _query({"kind": "products"})

    await method(connection, query)

    assert session.version_fetches() == 0
    assert session.sent_queries() == [query]


@pytest.mark.asyncio
async def test_the_version_is_fetched_once_per_connection() -> None:
    session = _FakeSession("16.2.0")
    connection = ConnectionRemote(_ENDPOINT, session)

    await connection.query_meta(_query(_DATA_FEED))
    await connection.query_execute(_query(_DATA_FEED))
    await connection.query_execute(_query(_DATA_FEED))

    assert session.version_fetches() == 1
    assert len(session.sent_queries()) == 3


@pytest.mark.asyncio
async def test_the_query_compatibility_is_built_once_per_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builds: list[BackendVersion] = []
    for_backend = QueryCompatibility.for_backend

    def counting_for_backend(version: BackendVersion) -> QueryCompatibility:
        builds.append(version)
        return for_backend(version)

    monkeypatch.setattr(QueryCompatibility, "for_backend", counting_for_backend)
    connection = ConnectionRemote(_ENDPOINT, _FakeSession("17.0.7"))

    await connection.query_meta(_query(_DATA_FEED))
    await connection.query_execute(_query(_DATA_FEED))
    await connection.query_meta_tables("dataFeed")
    await connection.query_meta_table(dict(_DATA_FEED))

    assert builds == [BackendVersion.parse("17.0.7")]


@pytest.mark.asyncio
async def test_connections_to_different_backends_each_adapt_to_their_own() -> None:
    old_session, new_session = _FakeSession("16.2.0"), _FakeSession("17.0.2")
    old_backend = ConnectionRemote(_ENDPOINT, old_session)
    new_backend = ConnectionRemote(_ENDPOINT, new_session)

    for connection in (old_backend, new_backend, old_backend, new_backend):
        await connection.query_execute(_query(_DATA_FEED))

    old_kinds = [query["stages"][0]["table"]["kind"] for query in old_session.sent_queries()]
    new_kinds = [query["stages"][0]["table"]["kind"] for query in new_session.sent_queries()]
    assert old_kinds == ["datafeed", "datafeed"]
    assert new_kinds == ["dataFeed", "dataFeed"]
    assert (old_session.version_fetches(), new_session.version_fetches()) == (1, 1)


@pytest.mark.asyncio
async def test_every_call_needing_the_backend_version_shares_one_fetch() -> None:
    session = _FakeSession("17.0.2")
    connection = ConnectionRemote(_ENDPOINT, session)

    first = await connection.backend_version()
    await connection.query_execute(_query(_DATA_FEED))
    await connection.send_notification(_NOTIFICATION)
    await connection.send_notification(_NOTIFICATION)

    assert await connection.backend_version() == first == {"major": 17, "minor": 0, "patch": 2}
    assert session.version_fetches() == 1


@pytest.mark.asyncio
async def test_concurrent_first_calls_share_one_fetch_and_one_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    release = f"{MIN_SUPPORTED_BACKEND_VERSION.major - 1}.0.0"
    session = _FakeSession(release)
    connection = ConnectionRemote(_ENDPOINT, session)

    with caplog.at_level(logging.WARNING, logger="pyfxlib.lowlevel.connection"):
        await asyncio.gather(
            connection.backend_version(),
            connection.query_execute(_query(_DATA_FEED)),
            connection.query_meta(_query(_DATA_FEED)),
        )

    assert session.version_fetches() == 1
    assert len(caplog.messages) == 1


@pytest.mark.asyncio
@_QUERY_METHODS
async def test_an_unsupported_query_is_rejected_before_being_sent(method: QueryMethod) -> None:
    session = _FakeSession("16.3.15")
    connection = ConnectionRemote(_ENDPOINT, session)

    with pytest.raises(UnsupportedQueryError, match="table 'users'"):
        await method(connection, _query({"kind": "users"}))

    assert session.sent_queries() == []


_SUPPORTED = MIN_SUPPORTED_BACKEND_VERSION.major


@pytest.mark.asyncio
@pytest.mark.parametrize("release", [f"{_SUPPORTED - 1}.4.9", f"{_SUPPORTED - 1}.0.0"])
async def test_a_deprecated_backend_is_warned_about_once_per_connection(
    release: str, caplog: pytest.LogCaptureFixture
) -> None:
    connection = ConnectionRemote(_ENDPOINT, _FakeSession(release))

    with caplog.at_level(logging.WARNING, logger="pyfxlib.lowlevel.connection"):
        await connection.backend_version()
        await connection.backend_version()

    assert caplog.messages == [
        f"Backend {release} at {_ENDPOINT} is deprecated: pyfxlib no longer supports backends"
        f" older than {MIN_SUPPORTED_BACKEND_VERSION}, and will drop the code for it in a"
        " future release."
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("release", [f"{_SUPPORTED - 2}.4.9", f"{_SUPPORTED - 2}.0.0"])
async def test_an_older_backend_is_warned_about_once_per_connection(
    release: str, caplog: pytest.LogCaptureFixture
) -> None:
    connection = ConnectionRemote(_ENDPOINT, _FakeSession(release))

    with caplog.at_level(logging.WARNING, logger="pyfxlib.lowlevel.connection"):
        await connection.backend_version()
        await connection.backend_version()

    assert caplog.messages == [
        f"Backend {release} at {_ENDPOINT} is not supported: pyfxlib supports backends from"
        f" {MIN_SUPPORTED_BACKEND_VERSION}, and some calls may fail."
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "release",
    [
        f"{_SUPPORTED}.0.0",
        f"{_SUPPORTED}.0.0-SNAPSHOT",
        f"{_SUPPORTED}.3.2",
        f"{_SUPPORTED + 2}.0.2",
        f"{_SUPPORTED + 3}.0.0",
    ],
)
async def test_a_supported_backend_is_not_warned_about(
    release: str, caplog: pytest.LogCaptureFixture
) -> None:
    connection = ConnectionRemote(_ENDPOINT, _FakeSession(release))

    with caplog.at_level(logging.WARNING, logger="pyfxlib.lowlevel.connection"):
        await connection.backend_version()

    assert caplog.messages == []


_NOTIFICATION = Notification(
    title="Title",
    message="Message",
    source="notificationBanner",
    status=NotificationStatus.INFO,
    topic=NotificationTopic.SYSTEM_NOTIFICATION,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("release", ["15.2.0", "15.3.1", "16.0.0", "17.0.2"])
async def test_a_notification_is_sent_to_a_backend_supporting_it(release: str) -> None:
    session = _FakeSession(release)

    await ConnectionRemote(_ENDPOINT, session).send_notification(_NOTIFICATION)

    assert [url for url, _ in session.posted] == ["/login/extended", "/notification.send"]


@pytest.mark.asyncio
@pytest.mark.parametrize("release", ["15.1.9", "15.0.0", "14.3.0"])
async def test_a_notification_is_rejected_by_older_backends_before_being_sent(
    release: str,
) -> None:
    session = _FakeSession(release)

    with pytest.raises(RuntimeError, match=f"below 15.2.0. Current backend version: {release}"):
        await ConnectionRemote(_ENDPOINT, session).send_notification(_NOTIFICATION)

    assert [url for url, _ in session.posted] == ["/login/extended"]


def test_the_notification_gate_is_not_met_by_every_handled_backend() -> None:
    """Fails once the oldest handled backend rises past the gate: see CONTRIBUTING.md."""
    oldest = MIN_DEPRECATED_BACKEND_VERSION

    assert not supported_by_every_backend_from(oldest, NOTIFICATIONS_SINCE), (
        f"Every backend from {oldest} supports notifications: drop `NOTIFICATIONS_SINCE` and"
        " its check from `ConnectionRemote.send_notification`."
    )


def test_the_query_meta_tables_gate_is_not_met_by_every_handled_backend() -> None:
    """Fails once the oldest handled backend rises past the gate: see CONTRIBUTING.md."""
    oldest = MIN_DEPRECATED_BACKEND_VERSION

    assert not supported_by_every_backend_from(oldest, QUERY_META_TABLES_SINCE), (
        f"Every backend from {oldest} supports the queryapi.meta table endpoints: drop"
        " `QUERY_META_TABLES_SINCE` and its check from `ConnectionRemote.query_meta_tables`"
        " and `ConnectionRemote.query_meta_table`."
    )


# Every backend with the queryapi.meta table endpoints knows `dataFeed`: the kind is sent as
# is. Its adaptation to older backends is tested on `adapt_table_to_backend`.
_META_TABLES_RELEASES = pytest.mark.parametrize("release", ["16.3.16", "17.0.7"])


@pytest.mark.asyncio
@_META_TABLES_RELEASES
async def test_tables_are_listed(release: str) -> None:
    session = _FakeSession(release)
    connection = ConnectionRemote(_ENDPOINT, session)

    entries = await connection.query_meta_tables("dataFeed")

    assert session.sent_data("/queryapi.meta/tables") == [{"kind": "dataFeed"}]
    assert entries == [_TABLE_ENTRY, _TABLE_ENTRY]


@pytest.mark.asyncio
@_META_TABLES_RELEASES
async def test_table_columns_are_listed(release: str) -> None:
    session = _FakeSession(release)
    connection = ConnectionRemote(_ENDPOINT, session)
    table = dict(_DATA_FEED)

    columns = await connection.query_meta_table(table)

    assert session.sent_data("/queryapi.meta/table") == [{"table": _DATA_FEED}]
    assert columns == [_COLUMN_ENTRY, _COLUMN_ENTRY]
    assert table == _DATA_FEED


@pytest.mark.asyncio
@_META_TABLES_RELEASES
async def test_an_answer_without_entries_lists_nothing(release: str) -> None:
    connection = ConnectionRemote(_ENDPOINT, _FakeSession(release, meta_entries=0))

    assert await connection.query_meta_tables("dataFeed") == []
    assert await connection.query_meta_table(dict(_DATA_FEED)) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("release", ["16.3.15", "17.0.6"])
async def test_a_backend_without_the_meta_table_endpoints_is_rejected_before_sending(
    release: str,
) -> None:
    session = _FakeSession(release)
    connection = ConnectionRemote(_ENDPOINT, session)

    with pytest.raises(RuntimeError, match="queryapi.meta/tables is not supported"):
        await connection.query_meta_tables("dataFeed")
    with pytest.raises(RuntimeError, match="queryapi.meta/table is not supported"):
        await connection.query_meta_table(dict(_DATA_FEED))

    assert session.sent_data("/queryapi.meta/tables") == []
    assert session.sent_data("/queryapi.meta/table") == []
