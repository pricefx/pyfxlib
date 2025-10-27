# pylint: disable=redefined-outer-name

import json
from configparser import ConfigParser
from tempfile import TemporaryDirectory
from typing import List

import pytest

from pyfx2.lowlevel.configuration import (
    CONNECTIONDISPATCH_CONFIG_SECTION,
    LOCALCONNECTION_CONFIG_SECTION,
    spec,
)


@pytest.fixture
def config():
    user_params = {"something": "I am something"}

    with TemporaryDirectory() as conn_dir:
        config = ConfigParser(interpolation=None)

        config.add_section("job")
        config.set("job", "modeltypedid", "modeltest")
        config.set("job", "jst", "1234")

        config.add_section("user")
        config.set("user", "parameters", json.dumps(user_params))

        config.add_section(LOCALCONNECTION_CONFIG_SECTION)
        config.set(LOCALCONNECTION_CONFIG_SECTION, "path", conn_dir)

        config.set(LOCALCONNECTION_CONFIG_SECTION, "logformat", "%(progress)s %(message)s")

        config.add_section(CONNECTIONDISPATCH_CONFIG_SECTION)

        yield config


def test_unit_spec_valid(config: ConfigParser) -> None:
    def diag(config: ConfigParser) -> List[str]:
        return []

    my_spec = spec.UnitSpec(diag)

    assert my_spec.is_valid(config)
    assert my_spec.diagnostic(config) == []
    try:
        my_spec.validate(config)
    except spec.SpecError:
        pytest.fail("validation should succeed")


def test_unit_spec_invalid(config: ConfigParser) -> None:
    def diag(config: ConfigParser) -> List[str]:
        return ["failure"]

    my_spec = spec.UnitSpec(diag)

    assert not my_spec.is_valid(config)
    assert my_spec.diagnostic(config) == ["failure"]
    with pytest.raises(spec.SpecError):
        my_spec.validate(config)


def test_optional_spec(config: ConfigParser) -> None:
    def diag(config: ConfigParser) -> List[str]:
        return ["failure"]

    my_spec = spec.OptionalSpec(spec.UnitSpec(diag))

    assert my_spec.is_valid(config)
    assert my_spec.diagnostic(config) == ["[Optional] failure"]


def test_conditional_spec_inactive(config: ConfigParser) -> None:
    def diag(config: ConfigParser) -> List[str]:
        return ["failure"]

    def cond(config: ConfigParser) -> bool:
        return False

    my_spec = spec.ConditionalSpec(spec.UnitSpec(diag), cond)

    assert my_spec.is_valid(config)
    assert my_spec.diagnostic(config) == []


def test_conditional_spec_active(config: ConfigParser) -> None:
    def diag(config: ConfigParser) -> List[str]:
        return ["failure"]

    def cond(config: ConfigParser) -> bool:
        return True

    my_spec = spec.ConditionalSpec(spec.UnitSpec(diag), cond)

    assert not my_spec.is_valid(config)
    assert my_spec.diagnostic(config) == ["failure"]


def test_composed_spec(config: ConfigParser) -> None:
    def diag1(config: ConfigParser) -> List[str]:
        return ["failure1"]

    def diag2(config: ConfigParser) -> List[str]:
        return ["failure2"]

    my_spec = spec.ComposedSpec(
        [
            spec.UnitSpec(diag1),
            spec.UnitSpec(diag2),
        ]
    )

    assert not my_spec.is_valid(config)
    assert my_spec.diagnostic(config) == ["failure1", "failure2"]


def test_section_must_exist_spec_success(config: ConfigParser) -> None:
    my_spec = spec.SectionMustExistSpec("job")
    assert my_spec.is_valid(config)


def test_section_must_exist_spec_failure(config: ConfigParser) -> None:
    my_spec = spec.SectionMustExistSpec("nonexistant")
    assert not my_spec.is_valid(config)


def test_section_must_contains_key_spec_success_without_section(
    config: ConfigParser,
) -> None:
    my_spec = spec.SectionMustContainKeySpec("nonexistant", "nonexistant")
    assert my_spec.is_valid(config)


def test_section_must_contains_key_spec_success_with_key(config: ConfigParser) -> None:
    my_spec = spec.SectionMustContainKeySpec("job", "modeltypedid")
    assert my_spec.is_valid(config)


def test_section_must_contains_key_spec_failure(config: ConfigParser) -> None:
    my_spec = spec.SectionMustContainKeySpec("job", "nonexistant")
    assert not my_spec.is_valid(config)


def test_possible_values_spec_success_without_section(config: ConfigParser) -> None:
    my_spec = spec.PossibleValuesSpec("nonexistant", "nonexistant", [])
    assert my_spec.is_valid(config)


def test_possible_values_spec_success_without_key(config: ConfigParser) -> None:
    my_spec = spec.PossibleValuesSpec("job", "nonexistant", [])
    assert my_spec.is_valid(config)


def test_possible_values_spec_success_with_key(config: ConfigParser) -> None:
    my_spec = spec.PossibleValuesSpec("job", "jst", ["1234"])
    assert my_spec.is_valid(config)


def test_possible_values_spec_fail(config: ConfigParser) -> None:
    my_spec = spec.PossibleValuesSpec("job", "jst", ["invalidvalue"])
    assert not my_spec.is_valid(config)
