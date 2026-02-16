"""Low level functions of the pyfxlib library."""

from typing import List

__all__: List[str] = []

# The default chunk size for streaming downloads. The previous value of 128 bytes caused
# incomplete downloads on large tables after the migration from requests (sync) to httpx (async),
# due to the overhead of multiple async generator layers per chunk.
_DEFAULT_STREAM_CHUNK_SIZE = 8192
