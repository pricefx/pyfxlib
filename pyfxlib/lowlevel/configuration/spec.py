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

"""Specification for valid configurations.

This module defines a DSL for checking a configuration is valid.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from configparser import ConfigParser
from typing import Any


class SpecError(Exception):
    """Exception raised when a config does not validate a Spec."""

    pass


class Spec(ABC):
    """Spec that must be followed by a config to build an object."""

    @abstractmethod
    def diagnostic(self, config: ConfigParser) -> list[str]:
        """Returns issues with the config regarding the spec."""
        pass

    def is_valid(self, config: ConfigParser) -> bool:
        """Validate a spec is satisfied.

        By default, a spec is validated if there is no diagnostic.
        """
        return len(self.diagnostic(config)) == 0

    def validate(self, config: ConfigParser) -> None:
        """Will raise an Error if config does not validate spec."""
        if not self.is_valid(config):
            diag = self.diagnostic(config)

            err_msg = "\n"
            err_msg += "\n".join(diag)
            raise SpecError(err_msg)


class UnitSpec(Spec):
    """Spec of an unitary item that must be validated."""

    def __init__(self, diagnostic: Callable[[ConfigParser], list[str]]) -> None:
        self._diagnostic = diagnostic

    def diagnostic(self, config: ConfigParser) -> list[str]:
        """See `Spec.diagnostic`."""
        return self._diagnostic(config)


class OptionalSpec(Spec):
    """Spec of an unitary item that may optionnally be validated.

    Non-requirement spec, can be used for documentation purpose
    """

    def __init__(self, spec: Spec) -> None:
        self._spec = spec

    def diagnostic(self, config: ConfigParser) -> list[str]:
        """See `Spec.diagnostic`."""
        diag = self._spec.diagnostic(config)
        return [f"[Optional] {msg}" for msg in diag]

    def is_valid(self, config: ConfigParser) -> bool:
        """See `Spec.is_valid`.

        This spec will always return True.
        """
        return True


class ConditionalSpec(Spec):
    """Spec that must be validated only if some other condition is met."""

    def __init__(
        self,
        spec: Spec,
        condition: Callable[[ConfigParser], bool],
    ) -> None:
        self._spec = spec
        self._condition = condition

    def diagnostic(self, config: ConfigParser) -> list[str]:
        """See `Spec.diagnostic`."""
        return self._spec.diagnostic(config) if self._condition(config) else []


class FunctionSpec(Spec):
    """Spec that must be validated only function returns true."""

    def __init__(self, condition: Callable[[ConfigParser], bool], diag_string: str) -> None:
        self._condition = condition
        self._diag_string = diag_string

    def diagnostic(self, config: ConfigParser) -> list[str]:
        """See `Spec.diagnostic`."""
        return [self._diag_string] if not self._condition(config) else []


class ComposedSpec(Spec):
    """Spec composed of multiple specs."""

    def __init__(self, specs: list[Spec]) -> None:
        self._specs = specs

    def diagnostic(self, config: ConfigParser) -> list[str]:
        """See `Spec.diagnostic`."""
        res: list[str] = []
        for spec in self._specs:
            res = [*res, *spec.diagnostic(config)]
        return res

    def is_valid(self, config: ConfigParser) -> bool:
        """See `Spec.is_valid`."""
        res = True
        for spec in self._specs:
            res = res and spec.is_valid(config)
        return res


class SectionMustExistSpec(UnitSpec):
    """Spec validating a section is present."""

    def __init__(self, section: str) -> None:
        def _diagnostic(config: ConfigParser) -> list[str]:
            return [f"Required section `{section}` is missing"] if section not in config else []

        super().__init__(_diagnostic)


class SectionMustContainKeySpec(UnitSpec):
    """Spec validating a section key is present if section exists.

    Note that this spec will allow the section to be missing
    """

    def __init__(self, section: str, key: str) -> None:
        def _diagnostic(config: ConfigParser) -> list[str]:
            if section not in config:
                return []

            if key in config[section]:
                return []

            return [f"Section `{section}` is missing required key `{key}`"]

        super().__init__(_diagnostic)


class PossibleValuesSpec(UnitSpec):
    """Spec validating a section key value is in a given list of possible values if present.

    Note that this spec will allow the section or the section key to be missing.
    """

    def __init__(
        self,
        section: str,
        key: str,
        values: list[Any],
    ) -> None:
        def _diagnostic(config: ConfigParser) -> list[str]:
            if section not in config:
                return []
            if key not in config[section]:
                return []
            if config[section][key] in values:
                return []

            return [
                f"`{section}`.`{key}` value should be one of {values}, is {config[section][key]}"
            ]

        super().__init__(_diagnostic)


class SectionSpec(ComposedSpec):
    """Spec for a section."""

    def __init__(
        self,
        section: str,
        required: bool = True,
        specs: list[Spec] | None = None,
    ) -> None:
        self._section = section
        self._required = required
        if specs is None:
            specs = []
        if required:
            specs.append(SectionMustExistSpec(section))
        super().__init__(specs)

    def with_spec(self, spec: Spec) -> "SectionSpec":
        """Add a spec for the section."""
        self._specs.append(spec)
        return self

    def with_check(self, cond: Callable[[ConfigParser], bool], diag_msg: str) -> "SectionSpec":
        """Add a spec from a function that must be checked."""
        return self.with_spec(FunctionSpec(cond, diag_msg))

    def must_contains(self, key: str, possible_values: list[Any] | None = None) -> "SectionSpec":
        """Add required key to section spec."""
        self.with_spec(SectionMustContainKeySpec(self._section, key))
        if possible_values is not None:
            self.with_spec(PossibleValuesSpec(self._section, key, possible_values))
        return self

    def may_contains(self, key: str, possible_values: list[Any] | None = None) -> "SectionSpec":
        """Add optional key to section spec."""
        self.with_spec(OptionalSpec(SectionMustContainKeySpec(self._section, key)))
        if possible_values is not None:
            self.with_spec(PossibleValuesSpec(self._section, key, possible_values))
        return self

    def when(self, condition: Callable[[ConfigParser], bool]) -> "SectionSpec":
        """Add conditional spec for section."""
        cond = ConditionalSectionSpec(self._section, self._required, condition)
        self.with_spec(cond)
        return cond

    def when_key_value_is(self, key: str, value: Any) -> "SectionSpec":
        """Add spec for section for when a key is at a specific value."""

        def _cond(config: ConfigParser) -> bool:
            if self._section not in config:
                return False
            if key not in config[self._section]:
                return False
            return config[self._section][key] == value

        return self.when(_cond)


class ConditionalSectionSpec(SectionSpec):
    """Spec for a section that must be validated only when a condition is true."""

    def __init__(
        self, section: str, required: bool, condition: Callable[[ConfigParser], bool]
    ) -> None:
        super().__init__(section, required)
        self._condition = condition

    def diagnostic(self, config: ConfigParser) -> list[str]:
        """See `Spec.diagnostic`."""
        return super().diagnostic(config) if self._condition(config) else []

    def is_valid(self, config: ConfigParser) -> bool:
        """See `Spec.is_valid`."""
        return super().is_valid(config) if self._condition(config) else True


def required_section(section: str) -> SectionSpec:
    """Create a spec for a required section."""
    return SectionSpec(section, required=True)


def optional_section(section: str) -> SectionSpec:
    """Create a spec for an optional section."""
    return SectionSpec(section, required=False)
