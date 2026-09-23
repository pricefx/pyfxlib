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

"""Pricefx List Price Grid (LPG) domain objects."""

from enum import StrEnum, unique
from typing import Any, Optional

from pydantic import AliasChoices, ConfigDict, Field
from typing_extensions import override

from pyfxlib.schema.base import PfxCamelCaseModel


@unique
class LPGType(StrEnum):
    """LPG types."""

    SIMPLE = "SIMPLE"
    MATRIX = "MATRIX"


class LPG(PfxCamelCaseModel):
    """Representation of a Pricefx LPG."""

    # This makes the class hashable and enable comparison
    model_config = ConfigDict(frozen=True)

    version: int
    typed_id: str
    label: str
    size: int = Field(validation_alias=AliasChoices("numberOfItems", "size"))
    type: LPGType = Field(validation_alias=AliasChoices("priceGridType", "type"))
    id: int

    @override
    def __str__(self) -> str:
        return f'"{self.label}" ({self.id})'


@unique
class LPGProductWorkflowStatus(StrEnum):
    """Status of workflow for an LPG product."""

    DRAFT = "DRAFT"
    SUBMITTED = "SUBMITTED"
    DENIED = "DENIED"
    APPROVED = "APPROVED"
    NO_APPROVAL_REQUIRED = "NO_APPROVAL_REQUIRED"
    WITHDRAWN = "WITHDRAWN"
    INVALIDATED = "INVALIDATED"


@unique
class LPGProductApprovalState(StrEnum):
    """Status of approval for an LPG product."""

    NOT_APPROVED = "NOT_APPROVED"
    PARTIALLY_APPROVED = "PARTIALLY_APPROVED"
    APPROVED = "APPROVED"
    AUTO_APPROVED = "AUTO_APPROVED"
    DENIED = "DENIED"


class LPGProduct(PfxCamelCaseModel):
    """Representation of a Pricefx LPG product."""

    model_config = ConfigDict(frozen=True, extra="allow")

    version: int
    typed_id: str
    sku: str
    label: str
    key2: Optional[str] = None
    workflow_status: Optional[LPGProductWorkflowStatus] = None
    approval_state: Optional[LPGProductApprovalState] = None
    allowed_overrides: str = ""

    def extra(self) -> dict[str, Any]:
        """Return extra fields as a dictionary."""
        return self.model_extra if self.model_extra is not None else {}

    @override
    def __str__(self) -> str:
        """Return a string representation of the LPG product."""
        if self.key2 is not None:
            return f'"{self.label}" ({self.sku} - {self.key2})'
        return f'"{self.label}" ({self.sku})'
