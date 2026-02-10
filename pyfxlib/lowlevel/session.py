"""Handling a session with the Pricefx platform.

A session is used to manager persistent authentication with the platform.
"""

from abc import ABC, abstractmethod
import asyncio
import base64
from collections.abc import AsyncIterator
import getpass
import json
import logging
import os
import subprocess
from typing import Any, Awaitable, Callable, cast, Dict, List, Optional, overload

from httpx import AsyncClient, HTTPError, HTTPStatusError, Response, TimeoutException

LOGGER = logging.getLogger(__name__)


class PfxSession(ABC):
    """Formal interface of a PfxSession."""

    @abstractmethod
    async def get(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.get`."""
        raise NotImplementedError

    @abstractmethod
    async def post(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.post`."""
        raise NotImplementedError

    @abstractmethod
    async def post_simple(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.post`."""
        raise NotImplementedError

    @abstractmethod
    def add_request_hook(self, hook: Callable[[str, str, Dict[str, Any]], None]) -> None:
        """Add a hook to be executed before sending request."""
        raise NotImplementedError

    @abstractmethod
    def add_response_hook(self, hook: Callable[[Response], None]) -> None:
        """Add a hook to be executed after receiving a response."""
        raise NotImplementedError

    @abstractmethod
    async def get_stream(
        self, url: str, chunk_size: int = 128, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Get a stream from the given URL."""
        yield b""


class PfxAuthMethod(ABC):
    """Formal interface of a PfxAuthMethod."""

    @abstractmethod
    async def before_request(self, session: AsyncClient) -> None:
        """
        Method called before each request.

        The AuthMethod can then alter the session to register credentials properly

        Args:
            session: the session object
        """
        raise NotImplementedError

    async def after_response(self, session: AsyncClient, response: Response) -> None:
        """
        Method called after each response.

        The AuthMethod can then alter the session to register credentials properly

        Args:
            session: the session object
            response: the response obtained from the request
        """
        raise NotImplementedError


def pfx_session(auth: PfxAuthMethod, session: Optional[AsyncClient] = None) -> PfxSession:
    """
    Default PfxSession constructor.

    Args:
            auth: the authentication method to be used
            session: the httpx AsyncClient to use, if not set a new one will be created.
    """
    return RetryPfxSession(SimplePfxSession(auth, session))


def pfx_session_from_token_file(
    token_file_path: str, session: Optional[AsyncClient] = None
) -> PfxSession:
    """
    Default PfxSession constructor using a token file as auth method.

    Args:
            token_file_path: the token file path
            session: the httpx AsyncClient to use, if not set a new one will be created.
    """
    return pfx_session(PfxAuthTokenFile(token_file_path), session)


def pfx_session_from_token(token: str, session: Optional[AsyncClient] = None) -> PfxSession:
    """
    Default PfxSession constructor using a static token.

    Args:
            token: the token to use
            session: the httpx AsyncClient to use, if not set a new one will be created.
    """
    return pfx_session(PfxAuthStaticToken(token), session)


def pfx_session_from_user_pass(
    instance: str,
    partition: str,
    user: str,
    passwd_provider: Callable[[], str],
    protocol: str = "https",
) -> PfxSession:
    """
    Default PfxSession constructor using a user password authentication method.

    Args:
            instance: the base address of the instance
            partition: the partition name
            user: the user account
            passwd_provider: a way to retrieve the user password
            protocol: the protocol to use to define the URL
    """
    return pfx_session(PfxAuthUserPass(instance, partition, user, passwd_provider, protocol))


class RetryPfxSession(PfxSession):
    """
    Session that retries requests a given number of time before failing.

    Args:
            wrapped: the used PfxSession to execute the requests
            retry_predicate: a predicate which define a request should be retried when the given
                             HTTPError happens
            retry_delays: the sequence of delays in seconds to apply between each trial. The first
                          value is the first delay to wait before the second retry. The next value
                          will be waited after a second failure and so on. Consequently, the number
                          of retry done before aborting is equal to the size of this list
                          (excluding the initial request).
    """

    def __init__(
        self,
        wrapped: PfxSession,
        retry_predicate: Optional[Callable[[HTTPError], bool]] = None,
        retry_delays: Optional[List[int]] = None,
    ) -> None:
        self._wrapped: PfxSession = wrapped
        self._retry_predicate: Callable[[HTTPError], bool] = (
            retry_predicate if retry_predicate is not None else _default_retry_predicate
        )
        # by default 3 retries with respectively 3s, 10s and 30s between retries
        self._retry_delays: List[int] = retry_delays if retry_delays is not None else [3, 10, 30]

    async def _try(self, method: Callable[[], Awaitable[Response]]) -> Response:
        return await retry(method, 0, self._retry_delays, self._retry_predicate)

    async def post(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.post`."""
        return await self._try(lambda: self._wrapped.post(url, **kwargs))

    async def post_simple(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.post`."""
        return await self._wrapped.post(url, **kwargs)

    async def get(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.get`."""
        return await self._try(lambda: self._wrapped.get(url, **kwargs))

    def add_request_hook(self, hook: Callable[[str, str, Dict[str, Any]], None]) -> None:
        """Add a hook to be executed before sending request."""
        self._wrapped.add_request_hook(hook)

    def add_response_hook(self, hook: Callable[[Response], None]) -> None:
        """Add a hook to be executed after receiving a response."""
        self._wrapped.add_response_hook(hook)

    async def get_stream(
        self, url: str, chunk_size: int = 128, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from the given URL. See `httpx.AsyncClient.stream`."""
        async for chunk in self._wrapped.get_stream(url, chunk_size=chunk_size, **kwargs):
            yield chunk


def _default_retry_predicate(exception: HTTPError) -> bool:
    return isinstance(exception, TimeoutException) or (
        isinstance(exception, HTTPStatusError)
        and (
            (exception.response.status_code >= 500 and exception.response.status_code < 600)
            or exception.response.status_code == 409
        )
    )


@overload
async def retry(  # noqa: E704
    method: Callable[[], Awaitable[Response]],
    nb_tries: int,
    retry_delays: List[int],
    retry_predicate: Callable[[HTTPError], bool],
) -> Response: ...


@overload
async def retry(  # noqa: E704
    method: Callable[[], Awaitable[None]],
    nb_tries: int,
    retry_delays: List[int] = ...,
    retry_predicate: Callable[[HTTPError], bool] = ...,
) -> None: ...


async def retry(
    method: Callable[[], Awaitable[Response]] | Callable[[], Awaitable[None]],
    nb_tries: int,
    retry_delays: List[int] = [3, 10, 30],
    retry_predicate: Callable[[HTTPError], bool] = _default_retry_predicate,
) -> Response | None:
    """
    Wrapper function that retries requests a given number of time before failing.

    Args:
            method: the executed request function
            nb_tries: number of current try
            retry_predicate: a predicate which define a request should be retried when the given
                HTTPError happens
            retry_delays: the sequence of delays in seconds to apply between each trial. The first
                value is the first delay to wait before the second retry. The next value
                will be waited after a second failure and so on. Consequently, the number
                of retry done before aborting is equal to the size of this list
                (excluding the initial request).

    Returns:
        Returns either Response or None, depending on what's `method` returning.
    """
    try:
        return await method()
    except HTTPError as exception:
        if retry_predicate(exception):
            exception.add_note(
                "Exception occurred during transfer of data from/to backend.\n"
                "For more details, see partition BE logs and Python logs (Job trigger calculation"
                " logs, where pricefx_job_trigger_jst is set to this jobs ID).\n"
                "If calculation failed due to timeout, adjusting partition settings for"
                " `datamart.query.externalMaxTimeout` might help."
            )
            LOGGER.error("Caught a retryable error: %s", repr(exception))
            if len(retry_delays) > nb_tries:
                delay = retry_delays[nb_tries]
                LOGGER.error(
                    "Will retry in %d seconds (%d/%d retries)...",
                    delay,
                    nb_tries + 1,
                    len(retry_delays),
                )
                await asyncio.sleep(delay)
                return await retry(method, nb_tries + 1, retry_delays, retry_predicate)
            else:
                LOGGER.error("Aborting after %d retries", nb_tries)
                raise exception
        else:
            LOGGER.error("Caught a non retryable error: %s", repr(exception))
            raise exception


class SimplePfxSession(PfxSession):
    """Specialized Session to handle Pfx specificities.

    By default, it raises an error for client error or server error responses.
    """

    def __init__(self, auth: PfxAuthMethod, session: Optional[AsyncClient] = None) -> None:
        """
        Constructor of SimplePfxSession.

        Args:
            auth: handler used to set up the session for authentication purpose.
            session: the httpx AsyncClient to use, if not set a new one will be created.
        """
        if not session:
            session = AsyncClient(timeout=None)
        self._session: AsyncClient = session
        self._auth = auth
        # disable keep-alive, this makes the connection pool a bit useless,
        #  but it's impossible to disable it, cf https://github.com/urllib3/urllib3/issues/383
        self._session.headers.update({"Connection": "close"})
        # initialize authentication method
        self._before_request_hooks: List[Callable[[str, str, Dict[str, Any]], None]] = []
        self._after_response_hooks: List[Callable[[Response], None]] = []

    async def get(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.get`."""
        await self._auth.before_request(self._session)
        try:
            for hook_before in self._before_request_hooks:
                hook_before("get", url, kwargs)
            response = await self._session.get(url, **kwargs)
            for hook_after in self._after_response_hooks:
                hook_after(response)
            _check_for_pfx_error(response)
            response.raise_for_status()
            await self._auth.after_response(self._session, response)
            return response
        except HTTPStatusError as err:
            if (body := _error_response_body(err)) is not None:
                LOGGER.error("Error response body: %s", body)
            raise err

    async def post(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.post`."""
        await self._auth.before_request(self._session)
        try:
            for hook_before in self._before_request_hooks:
                hook_before("post", url, kwargs)
            response = await self._session.post(url, **kwargs)
            for hook_after in self._after_response_hooks:
                hook_after(response)
            _check_for_pfx_error(response)
            response.raise_for_status()
            await self._auth.after_response(self._session, response)
            return response
        except HTTPStatusError as err:
            if (body := _error_response_body(err)) is not None:
                LOGGER.error("Error response body: %s", body)
            raise err

    async def post_simple(self, url: str, **kwargs: Any) -> Response:
        """See `httpx.AsyncClient.post`."""
        return await self.post(url, **kwargs)

    def add_request_hook(self, hook: Callable[[str, str, Dict[str, Any]], None]) -> None:
        """Add a hook to be executed before sending request."""
        self._before_request_hooks.append(hook)

    def add_response_hook(self, hook: Callable[[Response], None]) -> None:
        """Add a hook to be executed after receiving a response."""
        self._after_response_hooks.append(hook)

    async def get_stream(
        self, url: str, chunk_size: int = 128, **kwargs: Any
    ) -> AsyncIterator[bytes]:
        """Stream bytes from the given URL. See `httpx.AsyncClient.stream`."""
        await self._auth.before_request(self._session)
        async with self._session.stream("GET", url, **kwargs) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                yield chunk


def _error_response_body(err: HTTPStatusError) -> Optional[str]:
    if err.response is not None and err.response.text is not None:
        return err.response.text
    return None


def _check_for_pfx_error(response: Response) -> None:
    try:
        response_json = response.json()
    except json.JSONDecodeError:
        # Some messages just don't have any content. That's Ok.
        return

    if "Content-Disposition" in response.headers and response.headers[
        "Content-Disposition"
    ].startswith("attachment;"):
        # result is a raw file
        return

    if "response" not in response_json:
        message = (
            f'The following Response does not contain a "response" key : {response} {response_json}'
        )
        LOGGER.error(message)
        raise HTTPStatusError(message=message, request=response.request, response=response)

    if "status" not in response_json["response"]:
        message = (
            f'The following Response does not contain a "status" key : {response} {response_json}'
        )
        LOGGER.error(message)
        raise HTTPStatusError(message=message, request=response.request, response=response)

    status = response_json["response"]["status"]
    # Note: for more info about app status code, see
    # https://qa.pricefx.eu/pricefx-api/json/master.html "Status Code"
    if status not in [
        0,  # `STATUS_SUCCESS`
        -8,  # `LOGIN_SUCCESS`
    ]:
        message = f"Application error in response : {response} {response_json}"
        LOGGER.error(message)
        raise HTTPStatusError(message=message, request=response.request, response=response)


class PfxAuthStaticToken(PfxAuthMethod):
    """A PfxAuthHandler using a static token."""

    def __init__(self, token: str) -> None:
        self.pfxtoken = token

    async def before_request(self, session: AsyncClient) -> None:
        """See `PfxAuthMethod.before_request`."""
        session.headers.update({"Cookie": f"X-PriceFx-jwt={self.pfxtoken}"})

    async def after_response(self, session: AsyncClient, response: Response) -> None:
        """See `PfxAuthMethod.after_response`."""
        pass


class PfxAuthTokenFile(PfxAuthMethod):
    """A PfxAuthHandler that uses a token stored in a file.

    The token file is updated by re-reading the file before each request.
    """

    def __init__(self, token_file_path: str) -> None:
        self.pfxtoken_file_path = token_file_path

    async def before_request(self, session: AsyncClient) -> None:
        """See `PfxAuthMethod.before_request`."""
        with open(self.pfxtoken_file_path, "r") as token_file:
            token = token_file.read().replace("\n", "")
            session.headers.update({"Cookie": f"X-PriceFx-jwt={token}"})

    async def after_response(self, session: AsyncClient, response: Response) -> None:
        """See `PfxAuthMethod.after_response`."""
        pass


class PfxAuthUserPass(PfxAuthMethod):
    """A PfxSession that authenticate using user/pass on first connexion.

    After authenticating the user, the session switch to the provided JWT token.
    """

    def __init__(
        self,
        instance: str,
        partition: str,
        user: str,
        passwd_provider: Callable[[], str],
        protocol: str = "https",
    ) -> None:
        """Create a new session by authenticating with a user/pass.

        Password cannot be provided directly, it must be given by the `passwd_provider`.

        Args:
            instance: the base address of the instance
            partition: the partition name
            user: the user account
            passwd_provider: a way to retrieve the user password
        """
        self._auth_url = f"{protocol}://{instance}/pricefx/{partition}/login"
        self._credential = base64.urlsafe_b64encode(
            bytes(f"{partition}/{user}:{passwd_provider()}", "utf-8")
        )
        self.pfxtoken: Optional[str] = None

    def _refresh_token(self, session: AsyncClient, response: Response) -> None:
        if "X-PriceFx-jwt" in response.cookies:
            self.pfxtoken = response.cookies["X-PriceFx-jwt"]
            session.headers.update({"Cookie": f"X-PriceFx-jwt={self.pfxtoken}"})

    async def before_request(self, session: AsyncClient) -> None:
        """See `PfxAuthMethod.before_request`."""
        if self.pfxtoken is None:
            try:
                response = await session.post(
                    self._auth_url,
                    headers={"Authorization": "Basic " + self._credential.decode()},
                )
                _check_for_pfx_error(response)
                self._refresh_token(session, response)
                del self._credential
            except HTTPStatusError as err:
                if (body := _error_response_body(err)) is not None:
                    LOGGER.error("Error response body: %s", body)
                raise err

    async def after_response(self, session: AsyncClient, response: Response) -> None:
        """See `PfxAuthMethod.after_response`."""
        self._refresh_token(session, response)


class PasswordProviderPrompt:
    """A platform provider prompting user for password."""

    def __init__(self, prompt: str) -> None:
        """Get the password by prompting on stdin.

        Args:
            prompt: the message to display when prompting for the password
        """
        self.prompt = prompt

    def __call__(self) -> str:
        """Get the password."""
        return getpass.getpass(self.prompt)


class PasswordProviderLinuxSecretStore:
    """A platform provider baked by Linux secretstore service."""

    def __init__(self, instance: str, partition: str, account: str) -> None:
        """Get the password from Linux secret storage.

        Args:
            instance: the instance to connect to
            partition: the partition to connect to
            account: the "account" key for the password

        """
        self.instance = instance
        self.partition = partition
        self.account = account

    def __call__(self) -> str:
        """Get the password."""
        from contextlib import closing

        import secretstorage  # type:ignore # Linux-specific module

        with closing(secretstorage.dbus_init()) as conn:
            secretstore = secretstorage.get_default_collection(conn)
            if secretstore.is_locked():
                secretstore.unlock()
            # Try to find matching storage with some variants
            res = []
            for variant in [
                f"https://{self.instance} ({self.partition})",
                f"https://{self.instance}/ ({self.partition})",
                f"{self.instance} ({self.partition})",
                f"{self.instance}/ ({self.partition})",
            ]:
                res = list(
                    secretstore.search_items(
                        {
                            "service": variant,
                            "account": self.account,
                            "xdg:schema": "com.intellij.credentialStore.Credential",
                        }
                    )
                )
                if len(res) != 0:
                    break
            if len(res) == 0:
                raise RuntimeError(
                    f"No password found for {self.instance=}, {self.partition=}, {self.account=}"
                )
            if len(res) > 1:
                raise RuntimeError(
                    "More than one password found for "
                    f"{self.instance=}, {self.partition=}, {self.account=}"
                )

            # we need to remove the username, but it can be an email
            # in which case the secret will be user\\@pricefx.com@PASSWORD
            # (of course, the password too can contain the '@' character)
            encoded_user = self.account.replace("@", "\\@")
            # note: +1 to remove the '@' separator also
            return res[0].get_secret().decode()[len(encoded_user) + 1 :]


class PasswordProviderCommand:
    """A platform provider getting the password from an external command."""

    def __init__(self, command: str) -> None:
        """Get the password by running a command and using its output."""
        self.command = command

    def __call__(self) -> str:
        """Get the password."""
        proc = subprocess.Popen(self.command.split(" "), stdout=subprocess.PIPE, text=True)
        if not proc or not proc.stdout:
            raise RuntimeError(f"unknown error running {self.command}")
        proc.wait()
        return proc.stdout.read().rstrip()  # drop carriage return


class PasswordProviderEnvironment:
    """A platform provider getting the password directly from an environment variable.

    Please don't use this outside of testing.
    """

    def __init__(self, env: str = "PFX_PASSWORD") -> None:
        """Get the password directly from env variables."""
        self.pwd = os.environ.get(env)
        if self.pwd is None:
            raise ValueError("The environmental value PFX_PASSWORD is not set")

    def __call__(self) -> str:
        """Get the password."""
        return cast(str, self.pwd)  # cast to str because mypy doesn't understand the env var is set
