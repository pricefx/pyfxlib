from typing import Any, Callable, Dict, Optional

from requests import Response
from requests.exceptions import Timeout

from pyfx2.lowlevel.session import (
    PfxSession,
    RetryPfxSession,
)


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
