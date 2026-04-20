import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable
import os
from typing import Any
from urllib.parse import ParseResult, urlparse

from _pytest.fixtures import fixture
from httpx import HTTPStatusError, Response, TimeoutException
import pytest_asyncio

from pyfxlib._testtooling.helpers import _IntegrationRemote
from pyfxlib.api.domain import Partition
from pyfxlib.lowlevel import _DEFAULT_STREAM_CHUNK_SIZE
from pyfxlib.lowlevel.connection import ConnectionAsync, ConnectionSync
from pyfxlib.lowlevel.session import (
    pfx_session,
    PfxAuthUserPass,
    PfxSession,
    RetryPfxSession,
    SimplePfxSession,
)


async def retry_on_http_error(
    request_callable: Callable, max_try: int = 3, delay_between_try: int = 5
):
    """Retry on HTTP error."""
    nb_try = 0
    while nb_try <= max_try:
        nb_try += 1
        try:
            return await request_callable()
        except HTTPStatusError as err:
            if nb_try >= max_try:
                raise err
            await asyncio.sleep(delay_between_try)


class _RaisingExceptionSession(RetryPfxSession):
    def __init__(
        self,
        wrapped: PfxSession,
        retries: int,
        exception_to_raise: Exception | None = TimeoutException("timeout"),
        endpoints_to_fail: list | None = None,
    ) -> None:
        super().__init__(wrapped)
        self._wrapped: PfxSession = wrapped
        self._retries: int = retries
        self._counter: int = 0
        self._exception_to_raise: Exception = exception_to_raise
        if endpoints_to_fail is None:
            endpoints_to_fail = ["datamart.createfc", "datamart.loadfc"]
        self._endpoints_to_fail: list = endpoints_to_fail

    def reset_counter(self) -> None:
        self._counter = 0

    async def post(self, url: str, **kwargs: Any) -> Response:
        if (
            any(endpoint in url for endpoint in self._endpoints_to_fail)
            and self._counter < self._retries
        ):
            print("Raising exception on", url)
            for file_tuple in kwargs["files"].values():
                if hasattr(file_tuple[1], "read"):
                    file_tuple[1].read(4096)  # deplete the file-like object
            self._counter += 1
            raise self._exception_to_raise
        return await self._wrapped.post(url, **kwargs)

    async def get(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.get`."""
        return await self._wrapped.get(url, **kwargs)

    def add_request_hook(self, hook: Callable[[str, str, dict[str, Any]], None]) -> None:
        """Add a hook to be executed before sending request."""
        self._wrapped.add_request_hook(hook)

    def add_response_hook(self, hook: Callable[[Response], None]) -> None:
        """Add a hook to be executed after receiving a response."""
        self._wrapped.add_response_hook(hook)

    async def get_stream(
        self, url: str, chunk_size: int = _DEFAULT_STREAM_CHUNK_SIZE, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from the given URL. See `httpx.AsyncClient.stream`."""
        async for chunk in self._wrapped.get_stream(url, chunk_size, **kwargs):
            yield chunk


@fixture(scope="session")
def _pfx_base_url() -> ParseResult:
    return urlparse(os.getenv("PFX_BASE_URL", "http://localhost:2000"))


@fixture(scope="function")
def _session(_auth: PfxAuthUserPass) -> PfxSession:
    return pfx_session(_auth)


@fixture(scope="session")
def _auth(_pfx_base_url: ParseResult) -> PfxAuthUserPass:
    return PfxAuthUserPass(
        _pfx_base_url.netloc,
        "system",
        "root",
        lambda: "root",
        protocol=_pfx_base_url.scheme,
    )


@pytest_asyncio.fixture(scope="function")
async def _remote(
    _session: PfxSession, _auth: PfxAuthUserPass, _pfx_base_url: ParseResult
) -> AsyncGenerator[_IntegrationRemote, None]:
    remote = _IntegrationRemote(_session, _auth, _pfx_base_url)
    _session.set_header("Connection", "close")
    try:
        await retry_on_http_error(
            lambda: _session.post(
                _pfx_base_url._replace(
                    path="/pricefx/system/remoteintegrationtestmanager/reset"
                ).geturl()
            )
        )
    finally:
        _session.remove_header("Connection")

    yield remote

    _session.set_header("Connection", "close")
    try:
        await retry_on_http_error(
            lambda: _session.post(
                _pfx_base_url._replace(
                    path="/pricefx/system/remoteintegrationtestmanager/cleanup"
                ).geturl()
            )
        )
    finally:
        _session.remove_header("Connection")


@fixture(scope="function")
def _async_conn(_remote: _IntegrationRemote) -> ConnectionAsync:
    return _remote.connection()


@fixture(scope="function")
def _conn(_remote: _IntegrationRemote) -> ConnectionSync:
    return ConnectionSync(_remote.connection())


@fixture(scope="function")
def _partition(_async_conn: ConnectionAsync) -> Partition:
    return Partition(_async_conn)


@pytest_asyncio.fixture(scope="function")
async def _model_object(_remote: _IntegrationRemote) -> dict[str, Any]:
    return (await _remote.new_model_object("aModelObjectName"))[1]


@pytest_asyncio.fixture(scope="function")
async def _job_jst(_remote: _IntegrationRemote, _model_object: dict[str, Any]) -> dict[str, Any]:
    await _remote.trigger_job(_model_object)
    return await _remote.job(_model_object["typedId"])


@fixture(scope="session")
def _raising_auth(_pfx_base_url: ParseResult) -> PfxAuthUserPass:
    return PfxAuthUserPass(
        _pfx_base_url.netloc,
        "system",
        "root",
        lambda: "root",
        protocol=_pfx_base_url.scheme,
    )


@fixture(scope="session")
def _retry_and_raising_session(
    _raising_auth, _pfx_base_url
) -> tuple[PfxSession, _RaisingExceptionSession]:
    raising = _RaisingExceptionSession(SimplePfxSession(_raising_auth), 2)
    retry = RetryPfxSession(raising, retry_delays=[1, 1, 1])
    return retry, raising


@pytest_asyncio.fixture(scope="function")
async def _raising_remote(
    _retry_and_raising_session: tuple[PfxSession, _RaisingExceptionSession],
    _raising_auth: PfxAuthUserPass,
    _pfx_base_url: ParseResult,
) -> AsyncGenerator[_IntegrationRemote, None]:

    _session, _ = _retry_and_raising_session
    _IntegrationRemote(_session, _raising_auth, _pfx_base_url)
    _session.set_header("Connection", "close")
    try:
        await retry_on_http_error(
            lambda: _session.post(
                _pfx_base_url._replace(
                    path="/pricefx/system/remoteintegrationtestmanager/reset"
                ).geturl()
            )
        )
    finally:
        _session.remove_header("Connection")
    yield _IntegrationRemote(_session, _raising_auth, _pfx_base_url)

    _session.set_header("Connection", "close")
    try:
        await retry_on_http_error(
            lambda: _session.post(
                _pfx_base_url._replace(
                    path="/pricefx/system/remoteintegrationtestmanager/cleanup"
                ).geturl()
            )
        )
    finally:
        _session.remove_header("Connection")


@fixture(scope="function")
def _connection_with_raising_session(
    _retry_and_raising_session, _raising_remote
) -> tuple[ConnectionSync, _RaisingExceptionSession]:
    retry, raising = _retry_and_raising_session

    connection = _raising_remote.connection()
    return ConnectionSync(connection), raising


@pytest_asyncio.fixture(scope="function")
async def _setup_datamart(
    _remote: _IntegrationRemote, _partition: Partition
) -> tuple[Partition, str, list[str]]:
    dm_name = "aDatamartName"
    col_names = ["column1", "column2"]
    await _remote.new_empty_datamart(
        dm_name,
        [
            {"name": col_names[0], "type": "TEXT", "key": True},
            {"name": col_names[1], "type": "NUMBER"},
        ],
    )
    return _partition, dm_name, col_names
