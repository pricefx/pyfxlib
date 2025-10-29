from pyfx2.lowlevel.connection import _split_typedid

import pytest


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
        _split_typedid(invalid_typedid)
    assert err.value.args[0] == f"'{invalid_typedid}' is not a valid typedId"


def test_typedid_should_be_parsed_correctly():
    id = 12
    type_code = "MO"
    assert _split_typedid(f"{id}.{type_code}") == (id, type_code)
