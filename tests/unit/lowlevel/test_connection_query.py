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

from collections.abc import AsyncIterator, Awaitable, Callable
import copy
import logging
from typing import Any

from httpx import Response
import pytest

from tests.unit.backend_versions import supported_by_every_backend_from

from pyfxlib.lowlevel.connection import _NOTIFICATIONS_SINCE, ConnectionRemote
from pyfxlib.lowlevel.constants import _DEFAULT_STREAM_CHUNK_SIZE
from pyfxlib.lowlevel.querycompat import UnsupportedQueryError
from pyfxlib.lowlevel.session import PfxSession
from pyfxlib.schema import (
    MIN_DEPRECATED_BACKEND_VERSION,
    MIN_SUPPORTED_BACKEND_VERSION,
    Notification,
    NotificationStatus,
    NotificationTopic,
)

_ENDPOINT = "https://pfx.test/pricefx/partition"


class _FakeSession(PfxSession):
    """Session recording the requests posted, and answering as a backend at `release`."""

    def __init__(self, release: str) -> None:
        self.release = release
        self.posted: list[tuple[str, Any]] = []

    async def post(self, url: str, **kwargs: Any) -> Response:
        self.posted.append((url.removeprefix(_ENDPOINT), kwargs.get("json")))
        if url.endswith("/login/extended"):
            data: list[Any] = [{"extendedData": {"Release": self.release}}]
        elif url.endswith("/queryapi.meta"):
            data = [{"columns": []}]
        else:
            data = []
        return Response(200, json={"response": {"data": data}})

    def sent_queries(self) -> list[Any]:
        return [body["data"]["query"] for url, body in self.posted if url.startswith("/queryapi.")]

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

    assert not supported_by_every_backend_from(oldest, _NOTIFICATIONS_SINCE), (
        f"Every backend from {oldest} supports notifications: drop `_NOTIFICATIONS_SINCE` and"
        " its check from `ConnectionRemote.send_notification`."
    )
