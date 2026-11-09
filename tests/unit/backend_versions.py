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

"""Test helpers about backend versions."""

from pyfxlib.schema import BackendVersion, VersionBounds


def supported_by_every_backend_from(oldest: BackendVersion, since: VersionBounds) -> bool:
    """Whether every backend from `oldest` has a change.

    Versions are monotonic within a major, so `oldest` and the first release of each newer
    major up to the last bound are enough to check.
    """
    newer_majors = range(oldest.major + 1, since.bounds[-1].major + 1)
    return since.supports(oldest) and all(
        since.supports(BackendVersion(major=major, minor=0, patch=0)) for major in newer_majors
    )
