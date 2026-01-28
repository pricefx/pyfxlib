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

"""Interacting with a Pricefx platform using a high-level API.

The purpose of this module is to provide a API to interact with the platform.

Pricefx Python Client can be used outside a Pricefx logic. One can use the class method
`domain.Partition.connect` to manually connect to a Pricefx partition and use the
returned `domain.Partition` object methods to navigate the partition objects.
"""

from pyfxlib.api.domain import Partition

from . import domain

__all__ = ["domain", "Partition"]
