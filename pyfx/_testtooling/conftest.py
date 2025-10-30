import os
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import ParseResult, urlparse

from _pytest.fixtures import fixture

from requests import HTTPError, Response
from requests.exceptions import Timeout

from pyfx._testtooling.helpers import _IntegrationRemote
from pyfx.api.domain import Instance
from pyfx.lowlevel.connection import Connection
from pyfx.lowlevel.session import (
    PfxAuthUserPass,
    PfxSession,
    RetryPfxSession,
    SimplePfxSession,
    pfx_session,
)


class _RaisingExceptionSession(RetryPfxSession):
    def __init__(
        self,
        wrapped: PfxSession,
        retries: int,
        exception_to_raise: Optional[Exception] = Timeout,
        endpoints_to_fail: Optional[list] = ["datamart.createfc", "datamart.loadfc"],
    ) -> None:
        self._wrapped: PfxSession = wrapped
        self._retries: int = retries
        self._counter: int = 0
        self._exception_to_raise: Exception = exception_to_raise
        self._endpoints_to_fail: list = endpoints_to_fail

    def reset_counter(self) -> None:
        self._counter = 0

    def post(self, url: str, **kwargs: Any) -> Response:
        if (
            any(endpoint in url for endpoint in self._endpoints_to_fail)
            and self._counter < self._retries
        ):
            print("Raising exception on", url)
            for data in kwargs["data"]:  # deplete the generator
                pass
            self._counter += 1
            raise self._exception_to_raise
        return self._wrapped.post(url, **kwargs)

    def get(self, url: str, **kwargs: Any) -> Response:
        """See `requests.Session.get`."""
        return self._wrapped.get(url, **kwargs)

    def add_request_hook(self, hook: Callable[[str, str, Dict[str, Any]], None]) -> None:
        """Add a hook to be executed before sending request."""
        self._wrapped.add_request_hook(hook)

    def add_response_hook(self, hook: Callable[[Response], None]) -> None:
        """Add a hook to be executed after receiving a response."""
        self._wrapped.add_response_hook(hook)


@fixture(scope="session")
def _pfx_base_url() -> ParseResult:
    return urlparse(os.getenv("PFX_BASE_URL", "http://localhost:2000"))


@fixture(scope="session")
def _session(_auth: ParseResult) -> PfxSession:
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


@fixture(scope="function")
def _remote(
    _session: PfxSession, _auth: PfxAuthUserPass, _pfx_base_url: ParseResult
) -> _IntegrationRemote:
    def retry_on_http_error(
        request_callable: Callable, max_try: int = 3, delay_between_try: int = 5
    ):
        nb_try = 0
        while nb_try <= max_try:
            nb_try += 1
            try:
                return request_callable()
            except HTTPError as err:
                if nb_try >= max_try:
                    raise err
                time.sleep(delay_between_try)

    retry_on_http_error(
        lambda: _session.post(
            _pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/reset"
            ).geturl()
        )
    )
    yield _IntegrationRemote(_session, _auth, _pfx_base_url)
    retry_on_http_error(
        lambda: _session.post(
            _pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/cleanup"
            ).geturl()
        )
    )


@fixture(scope="function")
def _conn(_remote: _IntegrationRemote) -> Connection:
    return _remote.connection()


@fixture(scope="function")
def _instance(_conn: Connection) -> Instance:
    return Instance(_conn)


@fixture(scope="function")
def _model_object(_remote: _IntegrationRemote) -> Dict[str, Any]:
    return _remote.new_model_object("aModelObjectName")[1]


@fixture(scope="function")
def _job_jst(_remote: _IntegrationRemote, _model_object: Dict[str, Any]) -> Dict[str, Any]:
    _remote.trigger_job(_model_object)
    return _remote.job(_model_object["typedId"])


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


@fixture(scope="function")
def _raising_remote(
    _retry_and_raising_session: tuple[PfxSession, _RaisingExceptionSession],
    _raising_auth: PfxAuthUserPass,
    _pfx_base_url: ParseResult,
) -> _IntegrationRemote:
    def retry_on_http_error(
        request_callable: Callable, max_try: int = 3, delay_between_try: int = 5
    ):
        nb_try = 0
        while nb_try <= max_try:
            nb_try += 1
            try:
                return request_callable()
            except HTTPError as err:
                if nb_try >= max_try:
                    raise err
                time.sleep(delay_between_try)

    _session, _ = _retry_and_raising_session
    retry_on_http_error(
        lambda: _session.post(
            _pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/reset"
            ).geturl()
        )
    )
    yield _IntegrationRemote(_session, _raising_auth, _pfx_base_url)
    retry_on_http_error(
        lambda: _session.post(
            _pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/cleanup"
            ).geturl()
        )
    )


@fixture(scope="function")
def _connection_with_raising_session(
    _retry_and_raising_session, _raising_remote
) -> tuple[Connection, _RaisingExceptionSession]:
    retry, raising = _retry_and_raising_session

    connection = _raising_remote.connection()
    return connection, raising
