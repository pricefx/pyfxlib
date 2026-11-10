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

"""Core Pricefx domain objects: backend, users, notifications, jobs."""

from dataclasses import dataclass
from enum import Enum, StrEnum, unique
from functools import total_ordering
from typing import Any, Final, Optional

from pydantic import ConfigDict, Field, field_validator, model_validator
from typing_extensions import override, Self

from pyfxlib.schema.base import PfxCamelCaseModel


@unique
class JobStatus(Enum):
    """Possible status of a job for the Pricefx REST API."""

    PROCESSING = "PROCESSING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


@total_ordering
class BackendVersion(PfxCamelCaseModel):
    """Backend version information.

    Versions compare by major, minor then patch. A missing or null minor or patch is 0: 15 and
    15.0.0 are the same version. Versions are immutable, so hashable.
    """

    model_config = ConfigDict(frozen=True)

    major: int
    minor: int = 0
    patch: int = 0

    @field_validator("minor", "patch", mode="before")
    @classmethod
    def _none_is_0(cls, value: Any) -> Any:
        return 0 if value is None else value

    @classmethod
    def parse(cls, version: str) -> Self:
        """Parse a Pricefx backend version string, e.g. '15.2.0' or '15.2.0-SNAPSHOT'."""
        version = version.removesuffix("-SNAPSHOT")
        try:
            parts = [int(part) for part in version.split(".")]
        except ValueError as err:
            raise ValueError(
                f"Invalid version format: {version}. Expected format is 'major.minor.patch'"
                " or 'major.minor' or 'major' with int values.",
            ) from err
        return cls.model_validate(dict(zip(["major", "minor", "patch"], parts)))

    @override
    def __str__(self) -> str:
        """Return a string representation of the version."""
        return f"{self.major}.{self.minor}.{self.patch}"

    # With the pydantic equality, the ordering derives `<=`, `>` and `>=` from it.
    def __lt__(self, other: object) -> bool:
        if not isinstance(other, BackendVersion):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)


@dataclass(frozen=True, init=False)
class VersionBounds:
    """Backend versions a change is available from: one lower bound per major.

    Changes are backported per major, so versions are not monotonic across majors (17.0.1 is
    numerically higher than 16.3.11 yet may predate a change backported to both), but they are
    within one. A backend on a major newer than every bound is assumed to have the change, as
    cut from a develop that had it, and one on an older major not to.
    """

    # One per major, lowest first. A tuple, so that bounds are hashable as well as equal
    # whatever the order they are given in.
    bounds: tuple[BackendVersion, ...]

    def __init__(self, *bounds: str) -> None:
        """Bounds from one version per major, e.g. `VersionBounds("16.3.11", "17.0.2")`."""
        versions = tuple(sorted(BackendVersion.parse(bound) for bound in bounds))
        majors = [version.major for version in versions]
        if not versions or len(set(majors)) != len(majors):
            raise ValueError(f"Expected one lower bound per major, got {bounds}")
        object.__setattr__(self, "bounds", versions)

    def supports(self, version: BackendVersion) -> bool:
        """Whether a backend at `version` has the change."""
        for bound in self.bounds:
            if bound.major == version.major:
                return version >= bound
        return version.major > self.bounds[-1].major

    def __str__(self) -> str:
        """The bounds as "X.Y.Z or A.B.C"."""
        return " or ".join(map(str, self.bounds))


# The oldest backend pyfxlib supports: the current major minus 2. See CONTRIBUTING.md to raise it.
MIN_SUPPORTED_BACKEND_VERSION: Final = BackendVersion(major=15, minor=0, patch=0)
# The oldest backend pyfxlib keeps the code for, without supporting it: the major before the
# oldest supported one. Older backends have none left, and may fail.
MIN_DEPRECATED_BACKEND_VERSION: Final = BackendVersion(
    major=MIN_SUPPORTED_BACKEND_VERSION.major - 1, minor=0, patch=0
)


class UserInfo(PfxCamelCaseModel):
    """User information."""

    login_name: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    typed_id: str
    activated: bool

    @override
    def __str__(self) -> str:
        """Format user information into a string."""
        names = [
            self.first_name,
            self.last_name,
        ]
        user_info = " ".join(filter(None, names))
        if user_info:
            user_info += f" ({self.email})"
        else:
            user_info = self.email
        return user_info


@unique
class NotificationStatus(StrEnum):
    """Possible status of a notification for the Pricefx REST API.

    More info in https://pricefx.atlassian.net/wiki/spaces/KB/pages/6878068738
    """

    INFO = "INFO"
    ERROR = "ERROR"
    WARNING = "WARNING"
    SUCCESS = "SUCCESS"


@unique
class NotificationTopic(StrEnum):
    """Possible topics of a notification for the Pricefx REST API.

    More info in https://pricefx.atlassian.net/wiki/spaces/KB/pages/6878068738
    """

    CALCULATION = "CALCULATION"
    DATA_DOWNLOAD = "DATA_DOWNLOAD"
    MESSAGE = "MESSAGE"
    GROOVY = "GROOVY"
    VALIDATION = "VALIDATION"
    ACTION = "ACTION"
    SUBMIT_FOR_APPROVAL = "SUBMIT_FOR_APPROVAL"
    IMPORT_MANAGER = "IMPORT_MANAGER"
    SYSTEM_NOTIFICATION = "SYSTEM_NOTIFICATION"
    COPILOT = "COPILOT"


@unique
class NotificationActionType(StrEnum):
    """Possible action types of a notification for the Pricefx REST API.

    More info in https://pricefx.atlassian.net/wiki/spaces/KB/pages/6878068738
    """

    INFO_MESSAGE = "INFO_MESSAGE"
    CTX_LINK = "CTX_LINK"
    LINK = "LINK"
    DOWNLOAD = "DOWNLOAD"
    MESSAGE_LINK = "MESSAGE_LINK"
    CTX_LINK_AND_TOAST = "CTX_LINK_AND_TOAST"


class Notification(PfxCamelCaseModel):
    """Notification representation.

    More info in https://pricefx.atlassian.net/wiki/spaces/KB/pages/6878068738
    """

    title: str
    message: str
    source: str
    status: NotificationStatus
    topic: NotificationTopic
    action_type: NotificationActionType = Field(default=NotificationActionType.INFO_MESSAGE)
    action: Optional[str] = Field(default=None, max_length=2000)
    action_label: Optional[str] = Field(default=None, max_length=255)
    recipients: Optional[list[dict[str, str]]] = None
    valid_from: Optional[str] = None
    valid_until: Optional[str] = None
    due_date: Optional[str] = None
    dismissible: Optional[bool] = None

    @model_validator(mode="after")
    def check_action(self) -> Self:
        """Validate the action fields per API rules."""
        if self.action_label is not None and self.action is None:
            raise ValueError("`action` must be set when `actionLabel` is provided")
        return self
