import math
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Any, List

from pyfx2.lowlevel.session import (
    PfxAuthStaticToken,
    RetryPfxSession,
    SimplePfxSession,
    pfx_session_from_token_file,
)

import pytest

from requests import HTTPError, RequestException, Response, Timeout


class MockResponse(Response):
    def raise_for_status(self) -> Any:
        return None


class MockSession:
    def __init__(self):
        self.headers = {}

    def get(self, url: str, **kwargs: Any) -> Response:
        return MockResponse()

    def post(self, url: str, **kwargs: Any) -> Response:
        return MockResponse()


def test_pfxsession_tokenfile_update():
    with NamedTemporaryFile(mode="w", delete_on_close=False) as tokenfile:
        tokenfile.write("token1")
        tokenfile.flush()

        raw_session = MockSession()
        pfxsession = pfx_session_from_token_file(tokenfile.name, session=raw_session)
        assert "Cookie" in raw_session.headers
        assert raw_session.headers["Cookie"] == "X-PriceFx-jwt=token1"

        tokenfile.seek(0)
        tokenfile.write("token2")
        tokenfile.flush()

        pfxsession.get("someurl")

        assert raw_session.headers["Cookie"] == "X-PriceFx-jwt=token2"

        tokenfile.seek(0)
        tokenfile.write("token3")
        tokenfile.flush()

        pfxsession.post("someurl")

        assert raw_session.headers["Cookie"] == "X-PriceFx-jwt=token3"


class RaisingExceptionSession:
    def __init__(self, exceptions: List[RequestException]) -> None:
        self.headers = {}
        self._exceptions = exceptions.copy()
        self.post_timestamps = []
        self.get_timestamps = []

    def _next_response(self) -> Response:
        if len(self._exceptions) > 0:
            exc, self._exceptions = self._exceptions[0], self._exceptions[1:]
            raise exc
        return MockResponse()

    def get(self, url: str, **kwargs: Any) -> Response:
        self.get_timestamps.append(datetime.now().timestamp())
        return self._next_response()

    def post(self, url: str, **kwargs: Any) -> Response:
        self.post_timestamps.append(datetime.now().timestamp())
        return self._next_response()


def http_error(status_code: int) -> HTTPError:
    response = Response()
    response.status_code = status_code
    return HTTPError(response=response)


@pytest.mark.parametrize(
    "exception",
    [
        (Timeout()),
        (http_error(500)),
        (http_error(409)),
    ],
)
def test_pfxsession_should_retry_on_timout_and_500_or_409_http_errors(exception, caplog):
    raw_session = RaisingExceptionSession([exception] * 3)
    delays = [1, 2, 1]
    pfx_session = RetryPfxSession(
        SimplePfxSession(PfxAuthStaticToken("a_dummy_token"), session=raw_session),
        retry_delays=delays,
    )

    resp = pfx_session.post("dummy_url")

    assert resp is not None
    assert len(raw_session.post_timestamps) == 4
    assert [
        int(math.floor(end - start))
        for start, end in zip(raw_session.post_timestamps, raw_session.post_timestamps[1:])
    ] == delays
    expected_log_messages = [
        f"Caught a retryable error: {repr(exception)}",
        "Will retry in 1 seconds (1/3 retries)...",
        "Will retry in 2 seconds (2/3 retries)...",
        "Will retry in 1 seconds (3/3 retries)...",
    ]
    effective_log_messages = [rec.message for rec in caplog.records]
    assert all(m in effective_log_messages for m in expected_log_messages), effective_log_messages


@pytest.mark.parametrize(
    "exception",
    [
        (Timeout()),
        (http_error(409)),
    ]
    + [(http_error(code)) for code in range(500, 600)],
)
def test_pfxsession_should_fail_after_too_much_timout_and_5xx_409_http_errors(exception, caplog):
    raw_session = RaisingExceptionSession([exception] * 2)
    delays = [0]
    pfx_session = RetryPfxSession(
        SimplePfxSession(PfxAuthStaticToken("a_dummy_token"), session=raw_session),
        retry_delays=delays,
    )

    with pytest.raises(type(exception)):
        pfx_session.post("dummy_url")

    assert len(raw_session.post_timestamps) == 2
    assert [
        int(math.floor(end - start))
        for start, end in zip(raw_session.post_timestamps, raw_session.post_timestamps[1:])
    ] == delays
    expected_log_messages = [
        f"Caught a retryable error: {repr(exception)}",
        "Will retry in 0 seconds (1/1 retries)...",
        "Aborting after 1 retries",
    ]
    effective_log_messages = [rec.message for rec in caplog.records]
    assert all(m in effective_log_messages for m in expected_log_messages), effective_log_messages


def test_pfxsession_should_fail_directly_on_non_elligible_exception(caplog):
    raw_session = RaisingExceptionSession([http_error(404)])
    delays = [1, 2]
    pfx_session = RetryPfxSession(
        SimplePfxSession(PfxAuthStaticToken("a_dummy_token"), session=raw_session),
        retry_delays=delays,
    )

    with pytest.raises(HTTPError):
        pfx_session.post("dummy_url")

    assert len(raw_session.post_timestamps) == 1
    assert "Caught a non retryable error: HTTPError()" in [rec.message for rec in caplog.records]
