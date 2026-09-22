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

"""Base class shared by the Pricefx domain objects."""

from pydantic import alias_generators, BaseModel, ConfigDict


class PfxCamelCaseModel(BaseModel):
    """Base model for wiring Pricefx REST/Analytics format in python."""

    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_name=True, alias_generator=alias_generators.to_camel
    )


class PfxPascalCaseModel(BaseModel):
    """Base model for wiring Picefx model-object step/input layer in python."""

    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_name=True, alias_generator=alias_generators.to_pascal
    )
