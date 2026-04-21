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

import pytest

from pyfxlib.lowlevel.connection import ConnectionAsync


@pytest.mark.parametrize(
    "invalid_typedid",
    [
        ("12"),
        ("MO"),
        ("12."),
        (".MO"),
        ("MO.12"),
        ("12.mo"),
        ("."),
        ("12.MO foo bar"),
        ("foo bar 12.MO"),
    ],
)
def test_typedid_parsing_should_raise_value_error_if_invalid(
    invalid_typedid,
):
    with pytest.raises(ValueError) as err:
        ConnectionAsync._split_typedid(invalid_typedid)
    assert err.value.args[0] == f"'{invalid_typedid}' is not a valid typedId"


def test_typedid_should_be_parsed_correctly():
    id = 12
    type_code = "MO"
    assert ConnectionAsync._split_typedid(f"{id}.{type_code}") == (id, type_code)
