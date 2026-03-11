"""Core Pricefx domain objects: backend, users, notifications, jobs."""

from enum import Enum, StrEnum, unique

from pydantic import alias_generators, BaseModel, ConfigDict, Field, model_validator
from typing_extensions import override, Self


@unique
class JobStatus(Enum):
    """Possible status of a job for the Pricefx REST API."""

    PROCESSING = "PROCESSING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class BackendVersion(BaseModel):
    """Backend version information."""

    major: int
    minor: int | None = None
    patch: int | None = None

    @override
    def __str__(self) -> str:
        """Return a string representation of the version."""
        return f"{self.major}.{self.minor or 0}.{self.patch or 0}"


class UserInfo(BaseModel):
    """User information."""

    model_config = ConfigDict(
        alias_generator=alias_generators.to_camel, populate_by_name=True, serialize_by_alias=True
    )

    login_name: str
    email: str
    first_name: str | None = None
    last_name: str | None = None
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


class Notification(BaseModel):
    """Notification representation.

    More info in https://pricefx.atlassian.net/wiki/spaces/KB/pages/6878068738
    """

    model_config = ConfigDict(
        serialize_by_alias=True, populate_by_name=True, alias_generator=alias_generators.to_camel
    )

    title: str
    message: str
    source: str
    status: NotificationStatus
    topic: NotificationTopic
    action_type: NotificationActionType = Field(default=NotificationActionType.INFO_MESSAGE)
    action: str | None = Field(default=None, max_length=2000)
    action_label: str | None = Field(default=None, max_length=255)
    recipients: list[dict[str, str]] | None = None
    valid_from: str | None = None
    valid_until: str | None = None
    due_date: str | None = None
    dismissible: bool | None = None

    @model_validator(mode="after")
    def check_action(self) -> Self:
        """Validate the action fields per API rules."""
        if self.action_label is not None and self.action is None:
            raise ValueError("`action` must be set when `actionLabel` is provided")
        return self
