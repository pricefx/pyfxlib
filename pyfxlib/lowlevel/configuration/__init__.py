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

"""Job context instantiation from a configuration file."""

from collections.abc import Callable
from configparser import ConfigParser
import json
import logging
from pathlib import Path
from typing import Any, Optional
import uuid

from pyfxlib.api.domain import DMModel, Model, ModelObject, PlatformJob
from pyfxlib.lowlevel.configuration import spec
from pyfxlib.lowlevel.connection import (
    ConnectionAsync,
    ConnectionComposed,
    ConnectionLocal,
    ConnectionRemote,
    ConnectionSync,
)
from pyfxlib.lowlevel.session import (
    PasswordProviderCommand,
    PasswordProviderLinuxSecretStore,
    PasswordProviderPrompt,
    pfx_session_from_token,
    pfx_session_from_user_pass,
    PfxSession,
)

REMOTECONNECTION_CONFIG_SECTION = "connection"
LOCALCONNECTION_CONFIG_SECTION = "connection.local"
CONNECTIONDISPATCH_CONFIG_SECTION = "connection.dispatch"
USER_CONFIG_SECTION = "user"
LOGS_CONFIG_SECTION = "logging"
JOB_CONFIG_SECTION = "job"


def _remote_conn_from_config(config: ConfigParser) -> Optional[ConnectionAsync]:
    """Create a remote Connection from config."""
    auth_values = ["token", "login"]
    pass_values = ["prompt", "keyring", "command"]

    my_spec = (
        spec.optional_section(REMOTECONNECTION_CONFIG_SECTION)
        .must_contains("endpoint")
        .must_contains("auth", possible_values=auth_values)
    )

    my_spec.when_key_value_is("auth", "token").must_contains("jwttoken")

    (
        my_spec.when_key_value_is("auth", "login")
        .must_contains("instance")
        .must_contains("partition")
        .must_contains("account")
        .must_contains("passwordmethod", possible_values=pass_values)
    )

    my_spec.when_key_value_is("passwordmethod", "command").must_contains("passcmd")

    my_spec.validate(config)

    if not config.has_section(REMOTECONNECTION_CONFIG_SECTION):
        return None

    config_conn = config[REMOTECONNECTION_CONFIG_SECTION]

    auth_type = config_conn["auth"]

    session: PfxSession
    if auth_type == "token":
        session = pfx_session_from_token(config_conn["jwttoken"])
    elif auth_type == "login":
        instance = config_conn["instance"]
        partition = config_conn["partition"]
        user = config_conn["account"]
        pass_prov = config_conn["passwordmethod"]
        pass_method: Callable[[], str]
        if pass_prov == "prompt":
            pass_method = PasswordProviderPrompt(f"password for {user}@{instance}/{partition}: ")
        elif pass_prov == "keyring":
            pass_method = PasswordProviderLinuxSecretStore(
                instance,
                partition,
                user,
            )
        elif pass_prov == "command":
            pass_cmd = config_conn["passcmd"]
            pass_method = PasswordProviderCommand(pass_cmd)
        else:
            raise RuntimeError(f"invalid password method {pass_prov}")
        session = pfx_session_from_user_pass(instance, partition, user, pass_method)
    else:
        raise RuntimeError(f"invalid session auth type {auth_type}")

    return ConnectionRemote(
        config_conn["endpoint"],
        session,
    )


def _local_conn_from_config(config: ConfigParser) -> Optional[ConnectionAsync]:
    """Create a local Connection from config."""
    my_spec = (
        spec.optional_section(LOCALCONNECTION_CONFIG_SECTION)
        .must_contains("path")
        .may_contains("logformat")
    )

    my_spec.validate(config)

    if LOCALCONNECTION_CONFIG_SECTION not in config:
        return None
    return ConnectionLocal(
        Path(config[LOCALCONNECTION_CONFIG_SECTION]["path"]),
        config[LOCALCONNECTION_CONFIG_SECTION]["logformat"],
    )


def conn_from_config(config: ConfigParser) -> ConnectionAsync:
    """Create a Connection from config."""
    remote_conn = _remote_conn_from_config(config)
    local_conn = _local_conn_from_config(config)

    if local_conn is not None and remote_conn is not None:
        expected_keys = [
            "model_attachments",
            "model_parameters",
            "model_tables",
            "pa_tables",
            "job_updates",
        ]
        possible_values = ["partition", "local"]

        my_spec = spec.required_section(CONNECTIONDISPATCH_CONFIG_SECTION)
        for key in expected_keys:
            my_spec.must_contains(key, possible_values=possible_values)

        my_spec.validate(config)

        dispatch_config = config[CONNECTIONDISPATCH_CONFIG_SECTION]

        dispatch = {}
        for k, v in dispatch_config.items():
            if v == "partition":
                dispatch[k] = remote_conn
            else:
                dispatch[k] = local_conn

        return ConnectionComposed(dispatch, remote_conn)

    if remote_conn is not None:
        return remote_conn

    if local_conn is not None:
        return local_conn

    raise RuntimeError(
        f"Must specify either {REMOTECONNECTION_CONFIG_SECTION}"
        f"or {LOCALCONNECTION_CONFIG_SECTION}"
    )


def sync_conn_from_config(config: ConfigParser) -> ConnectionSync:
    """Create a synchronous connection (ConnectionSync) from config."""
    return ConnectionSync(conn_from_config(config))


def user_params_from_config(config: ConfigParser) -> dict[str, Any]:
    """Create user params dict from config."""
    my_spec = spec.required_section("user").must_contains("parameters")
    my_spec.validate(config)

    return json.loads(config.get("user", "parameters", fallback="{}"))


def logging_from_config(
    config: ConfigParser,
) -> Optional[Callable[[Optional[str], Optional[int]], None]]:
    """Create logging facility from config."""
    loglevels = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }
    my_spec = (
        spec.optional_section(LOGS_CONFIG_SECTION)
        .may_contains("format")
        .may_contains(
            "level",
            [
                *list(loglevels.keys()),
                *[level.upper() for level in loglevels.keys()],
            ],
        )
    )

    my_spec.validate(config)

    if LOGS_CONFIG_SECTION not in config:
        return None

    log_config = config[LOGS_CONFIG_SECTION]

    # get log format or default
    logformat = log_config.get("format", "%(asctime)s %(progress)s %(message)s")

    # get log level config
    loglevel = logging.INFO
    if "level" in log_config:
        levelstring = log_config["level"].lower()
        loglevel = loglevels[levelstring]

    # set logger
    # Note, we generate an uuid, as we want to avoid having multiple instances
    # of contexts to share the same logger.
    # (can happens, e.g. during tests, when creating multiple contexts in sequence)
    loghandler = logging.StreamHandler()
    loghandler.setFormatter(logging.Formatter(logformat))
    logger = logging.getLogger(f"context_{uuid.uuid4()}")
    logger.addHandler(loghandler)
    logger.setLevel(logging.DEBUG)

    def _log(msg: Optional[str], progress: Optional[int]) -> None:
        logger.log(
            loglevel,
            msg if msg is not None else "",
            extra={
                "progress": str(progress) + "%" if progress is not None else "",
            },
        )

    return _log


def model_from_config(config: ConfigParser, conn: ConnectionSync) -> Optional[Model]:
    """Create PO model from config."""
    modeltypedid = config.get(JOB_CONFIG_SECTION, "modeltypedid", fallback=None)
    if modeltypedid is None:
        return None
    if modeltypedid.endswith(".MO"):
        return ModelObject.from_conn(conn, modeltypedid)
    return DMModel.from_conn(conn, modeltypedid)


def job_from_config(config: ConfigParser, conn: ConnectionSync) -> Optional[PlatformJob]:
    """Create Job from config."""
    jst_id = config.getint(JOB_CONFIG_SECTION, "jst_id", fallback=None)
    if jst_id is None:
        return None
    return PlatformJob(conn, jst_id)
