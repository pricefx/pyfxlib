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
from typing import Any

from httpx import Response
import pytest

from pyfxlib.lowlevel.connection import ConnectionRemote
from pyfxlib.lowlevel.constants import _DEFAULT_STREAM_CHUNK_SIZE
from pyfxlib.lowlevel.querycompat import UnsupportedQueryError
from pyfxlib.lowlevel.session import PfxSession

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
@_QUERY_METHODS
async def test_an_unsupported_query_is_rejected_before_being_sent(method: QueryMethod) -> None:
    session = _FakeSession("16.3.15")
    connection = ConnectionRemote(_ENDPOINT, session)

    with pytest.raises(UnsupportedQueryError, match="table 'users'"):
        await method(connection, _query({"kind": "users"}))

    assert session.sent_queries() == []
