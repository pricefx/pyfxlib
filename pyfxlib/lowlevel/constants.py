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

"""Constants coming from Pricefx constraints."""

# The default chunk size for streaming downloads. The previous value of 128 bytes caused
# incomplete downloads on large tables after the migration from requests (sync) to httpx (async),
# due to the overhead of multiple async generator layers per chunk.
_DEFAULT_STREAM_CHUNK_SIZE = 8192
# Default page size for the large datasets
_DEFAULT_PAGE_SIZE = 100_000
