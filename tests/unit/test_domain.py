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

import logging
from unittest.mock import Mock

import pandas as pd
import pytest

from pyfxlib.api.domain import TableMutable
from pyfxlib.lowlevel import pandasutil


def _table(fields: list[dict[str, str]]) -> TableMutable:
    return TableMutable(Mock(), "12345.DMDS", fields=fields)


@pytest.fixture(autouse=True)
def _assume_truncation_advisory_already_shown(monkeypatch):
    monkeypatch.setattr(pandasutil, "_TRUNCATION_RISK_ADVISED", True)


def test_update_pandas_does_not_warn_for_a_column_absent_from_declared_fields(caplog):
    # a column the table doesn't know about (e.g. not yet created) is skipped entirely: the
    # backend rejects an avro column absent from the existing FieldCollection spec outright,
    # so there's no point guessing a type (and no risk of mistaking a NUMBER/DATE for text).
    caplog.set_level(logging.WARNING, logger="pyfxlib.lowlevel.pandasutil")
    table = _table([])
    dataframe = pd.DataFrame({"key": ["k1"], "bigtext": ["x" * 2000]}).set_index("key")

    table.update_pandas(dataframe, check_oversized_values=True)

    assert caplog.text == ""


def test_update_pandas_does_not_check_oversized_values_by_default(caplog):
    caplog.set_level(logging.WARNING, logger="pyfxlib.lowlevel.pandasutil")
    table = _table([{"name": "bigtext", "type": "TEXT"}])
    dataframe = pd.DataFrame({"key": ["k1"], "bigtext": ["x" * 2000]}).set_index("key")

    table.update_pandas(dataframe)

    assert caplog.text == ""


def test_first_push_with_a_text_or_lob_field_gets_a_one_time_advisory(monkeypatch, caplog):
    monkeypatch.setattr(pandasutil, "_TRUNCATION_RISK_ADVISED", False)
    caplog.set_level(logging.WARNING, logger="pyfxlib.lowlevel.pandasutil")
    table = _table([{"name": "bigtext", "type": "TEXT"}])
    dataframe = pd.DataFrame({"key": ["k1"], "bigtext": ["short"]}).set_index("key")

    table.update_pandas(dataframe)

    assert "silently truncates" in caplog.text
    assert "check_oversized_values" in caplog.text

    caplog.clear()
    table.update_pandas(dataframe)

    assert caplog.text == ""


def test_truncation_advisory_does_not_fire_without_a_text_or_lob_field(monkeypatch, caplog):
    monkeypatch.setattr(pandasutil, "_TRUNCATION_RISK_ADVISED", False)
    caplog.set_level(logging.WARNING, logger="pyfxlib.lowlevel.pandasutil")
    table = _table([{"name": "amount", "type": "NUMBER"}])
    dataframe = pd.DataFrame({"key": ["k1"], "amount": [1]}).set_index("key")

    table.update_pandas(dataframe)

    assert caplog.text == ""


@pytest.mark.parametrize(
    "field_type, value_length, expected_warning_substring, expect_lob_suggestion",
    [
        # under the LOB limit: must NOT be warned about (auto-inference alone would have
        # seen this as TEXT, over its 255-char limit, and warned wrongly)
        ("LOB", 2000, None, False),
        # over the LOB limit: still warned about, using the LOB threshold
        ("LOB", 12000, "10000", False),
        # over the TEXT limit: warned about, using the TEXT threshold, and told that LOB is
        # an option.
        ("TEXT", 2000, "255", True),
    ],
)
def test_update_pandas_checks_oversized_values_against_the_declared_field_type(
    caplog, field_type, value_length, expected_warning_substring, expect_lob_suggestion
):
    caplog.set_level(logging.WARNING, logger="pyfxlib.lowlevel.pandasutil")
    table = _table([{"name": "bigtext", "type": field_type}])
    dataframe = pd.DataFrame({"key": ["k1"], "bigtext": ["x" * value_length]}).set_index("key")

    table.update_pandas(dataframe, check_oversized_values=True)

    if expected_warning_substring is None:
        assert caplog.text == ""
    else:
        assert expected_warning_substring in caplog.text
        assert ("Consider declaring this field as LOB" in caplog.text) == expect_lob_suggestion
