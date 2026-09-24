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

"""Pricefx optimization objects: model objects and their logics."""

from typing import Any, Optional

from pydantic import Field

from pyfxlib.schema.base import PfxCamelCaseModel


class LogicInput(PfxCamelCaseModel):
    """An input a logic expects, as the logic declares it."""

    name: str
    label: Optional[str] = None
    type: Optional[str] = None


class LogicOutput(PfxCamelCaseModel):
    """An element a logic returns."""

    name: str = Field(alias="elementName")
    label: Optional[str] = Field(default=None, alias="elementLabel")
    format: Optional[str] = Field(default=None, alias="formatType")


class LogicParameters(PfxCamelCaseModel):
    """What a logic expects, and what it will return."""

    logic_inputs: list[LogicInput] = Field(default_factory=list, alias="contextParameters")
    logic_outputs: list[LogicOutput] = Field(
        default_factory=list, alias="formulaParameterReference"
    )


class CalculationResult(PfxCamelCaseModel):
    """The result of one element of a logic, as returned by the backend.

    Only the fields describing the result are kept: the display settings the
    backend sends along (css, overridability, grid layout...) are ignored.
    """

    name: str = Field(alias="resultName")
    label: Optional[str] = Field(default=None, alias="resultLabel")
    type: Optional[str] = Field(default=None, alias="resultType")
    format: Optional[str] = Field(default=None, alias="formatType")
    value: Any = Field(default=None, alias="result")
    group: Optional[str] = Field(default=None, alias="resultGroup")
    warnings: Optional[list[str]] = None
    alert_type: Optional[str] = None
    alert_message: Optional[str] = None
