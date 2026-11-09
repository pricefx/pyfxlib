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

from dataclasses import FrozenInstanceError

import pytest
from pydantic import ValidationError

from tests.unit.backend_versions import supported_by_every_backend_from

from pyfxlib.schema import (
    BackendVersion,
    MIN_DEPRECATED_BACKEND_VERSION,
    MIN_SUPPORTED_BACKEND_VERSION,
    VersionBounds,
)


@pytest.mark.parametrize(
    "older,newer",
    [
        ("15.0.0", "15.0.1"),
        ("15.0.9", "15.1.0"),
        ("15.9.9", "16.0.0"),
        ("16.3.11", "17.0.0"),
        ("15", "15.0.1"),
        ("15.2", "15.2.1"),
    ],
)
def test_versions_compare_by_major_minor_then_patch(older: str, newer: str) -> None:
    old, new = BackendVersion.parse(older), BackendVersion.parse(newer)

    assert old < new and old <= new and new > old and new >= old
    assert not (old > new or old >= new or new < old or new <= old)
    assert old != new


@pytest.mark.parametrize("short", ["15", "15.0"])
def test_a_missing_minor_or_patch_counts_as_0(short: str) -> None:
    version, full = BackendVersion.parse(short), BackendVersion.parse("15.0.0")

    assert version == full and version <= full and version >= full
    assert not (version < full or version > full)


def test_a_version_equals_no_other_type() -> None:
    version = BackendVersion.parse("15.0.0")

    assert version != "15.0.0"
    assert version != {"major": 15, "minor": 0, "patch": 0}


@pytest.mark.parametrize("other", ["15.0.0", (15, 0, 0), None])
def test_a_version_orders_against_no_other_type(other: object) -> None:
    version = BackendVersion.parse("15.0.0")

    with pytest.raises(TypeError):
        _ = version < other  # type: ignore[operator]
    with pytest.raises(TypeError):
        _ = version >= other  # type: ignore[operator]


def test_a_version_is_immutable() -> None:
    version = BackendVersion.parse("15.0.0")

    with pytest.raises(ValidationError):
        version.major = 16  # type: ignore[misc]
    assert version == BackendVersion.parse("15.0.0")


def test_equal_versions_hash_alike() -> None:
    assert {BackendVersion.parse("15"), BackendVersion.parse("15.0.0")} == {
        BackendVersion(major=15)
    }


def test_a_null_minor_or_patch_is_0() -> None:
    version = BackendVersion.model_validate({"major": 15, "minor": None, "patch": None})

    assert (version.minor, version.patch) == (0, 0)
    assert version.model_dump() == BackendVersion(major=15).model_dump()


def test_versions_sort() -> None:
    versions = [BackendVersion.parse(v) for v in ["17.0.2", "15", "16.3.11", "15.2"]]

    assert [str(version) for version in sorted(versions)] == [
        "15.0.0",
        "15.2.0",
        "16.3.11",
        "17.0.2",
    ]
    assert str(max(versions)) == "17.0.2"


def test_the_deprecated_major_is_the_one_before_the_oldest_supported() -> None:
    assert MIN_DEPRECATED_BACKEND_VERSION == BackendVersion(
        major=MIN_SUPPORTED_BACKEND_VERSION.major - 1
    )


_BOUNDS = VersionBounds("17.0.2", "16.3.11")


@pytest.mark.parametrize(
    "version,supported",
    [
        ("15.9.9", False),  # older major than every bound
        ("16.3.10", False),
        ("16.3.11", True),
        ("16.4", True),
        # numerically higher than 16.3.11, but on the 17 major
        ("17.0.1", False),
        ("17.0.2", True),
        ("18.0.0", True),  # newer major than every bound
    ],
)
def test_version_bounds_compare_within_the_major_only(version: str, supported: bool) -> None:
    assert _BOUNDS.supports(BackendVersion.parse(version)) is supported


def test_version_bounds_are_kept_lowest_major_first() -> None:
    assert list(_BOUNDS.by_major) == [16, 17]
    assert str(_BOUNDS) == "16.3.11 or 17.0.2"


@pytest.mark.parametrize(
    "bounds", [(), ("16.3.11", "16.0.0")], ids=["no bound", "two bounds on one major"]
)
def test_version_bounds_require_one_bound_per_major(bounds: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="one lower bound per major"):
        VersionBounds(*bounds)


def test_version_bounds_are_read_only() -> None:
    with pytest.raises(FrozenInstanceError):
        _BOUNDS.by_major = {}  # type: ignore[misc]
    with pytest.raises(TypeError):
        _BOUNDS.by_major[16] = BackendVersion(major=16)  # type: ignore[index]


def test_version_bounds_equal_whatever_the_order_they_are_given_in() -> None:
    assert VersionBounds("16.3.11", "17.0.2") == _BOUNDS
    assert VersionBounds("16.3.11") != _BOUNDS


@pytest.mark.parametrize(
    "bounds,oldest,supported",
    [
        (("15.0.3",), "15.0.0", False),
        (("15.0.3",), "15.0.3", True),
        (("15.0.3",), "16.0.0", True),
        (("16.3.11", "17.0.2"), "16.3.11", False),
        (("16.3.11", "17.0.2"), "17.0.2", True),
        (("15.3.8", "16.0.0"), "15.3.8", True),
        (("17.2.0",), "16.0.0", False),
    ],
)
def test_supported_by_every_backend_from_checks_each_major_from_the_oldest(
    bounds: tuple[str, ...], oldest: str, supported: bool
) -> None:
    since = VersionBounds(*bounds)

    assert supported_by_every_backend_from(BackendVersion.parse(oldest), since) is supported
