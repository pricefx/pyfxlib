import os
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import ParseResult, urlparse

from _pytest.fixtures import fixture

from pyfx2.api.domain import Instance
from pyfx2.lowlevel.connection import Connection
from pyfx2.lowlevel.session import (
    PfxAuthUserPass,
    PfxSession,
    RetryPfxSession,
    SimplePfxSession,
    pfx_session,
)

from requests import HTTPError, Response
from requests.exceptions import Timeout

from tests.helpers import IntegrationRemote


class RaisingExceptionSession(RetryPfxSession):
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
def pfx_base_url() -> ParseResult:
    return urlparse(os.getenv("PFX_BASE_URL", "http://localhost:2000"))


@fixture(scope="session")
def session(auth: ParseResult) -> PfxSession:
    return pfx_session(auth)


@fixture(scope="session")
def auth(pfx_base_url: ParseResult) -> PfxAuthUserPass:
    return PfxAuthUserPass(
        pfx_base_url.netloc,
        "system",
        "root",
        lambda: "root",
        protocol=pfx_base_url.scheme,
    )


@fixture(scope="function")
def remote(
    session: PfxSession, auth: PfxAuthUserPass, pfx_base_url: ParseResult
) -> IntegrationRemote:
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
        lambda: session.post(
            pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/reset"
            ).geturl()
        )
    )
    yield IntegrationRemote(session, auth, pfx_base_url)
    retry_on_http_error(
        lambda: session.post(
            pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/cleanup"
            ).geturl()
        )
    )


@fixture(scope="function")
def conn(remote: IntegrationRemote) -> Connection:
    return remote.connection()


@fixture(scope="function")
def instance(conn: Connection) -> Instance:
    return Instance(conn)


@fixture(scope="function")
def model_object(remote: IntegrationRemote) -> Dict[str, Any]:
    return remote.new_model_object("aModelObjectName")[1]


@fixture(scope="function")
def job_jst(remote: IntegrationRemote, model_object: Dict[str, Any]) -> Dict[str, Any]:
    remote.trigger_job(model_object)
    return remote.job(model_object["typedId"])


@fixture(scope="session")
def raising_auth(pfx_base_url: ParseResult) -> PfxAuthUserPass:
    return PfxAuthUserPass(
        pfx_base_url.netloc,
        "system",
        "root",
        lambda: "root",
        protocol=pfx_base_url.scheme,
    )


@fixture(scope="session")
def retry_and_raising_session(
    raising_auth, pfx_base_url
) -> tuple[PfxSession, RaisingExceptionSession]:
    raising = RaisingExceptionSession(SimplePfxSession(raising_auth), 2)
    retry = RetryPfxSession(raising, retry_delays=[1, 1, 1])
    return retry, raising


@fixture(scope="function")
def raising_remote(
    retry_and_raising_session: tuple[PfxSession, RaisingExceptionSession],
    raising_auth: PfxAuthUserPass,
    pfx_base_url: ParseResult,
) -> IntegrationRemote:
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

    session, _ = retry_and_raising_session
    retry_on_http_error(
        lambda: session.post(
            pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/reset"
            ).geturl()
        )
    )
    yield IntegrationRemote(session, raising_auth, pfx_base_url)
    retry_on_http_error(
        lambda: session.post(
            pfx_base_url._replace(
                path="/pricefx/system/remoteintegrationtestmanager/cleanup"
            ).geturl()
        )
    )


@fixture(scope="function")
def connection_with_raising_session(
    retry_and_raising_session, raising_remote
) -> tuple[Connection, RaisingExceptionSession]:
    retry, raising = retry_and_raising_session

    connection = raising_remote.connection()
    return connection, raising
