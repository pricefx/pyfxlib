# pyfxlib

A set of utilities to be able to use the Pricefx API from a Python package.
The package is available on [PyPI](https://pypi.org/project/pyfxlib/) and can be installed with pip or Poetry.

## Installation

```bash
pip install pyfxlib
```

Or with Poetry:

```bash
poetry add pyfxlib
```

## Supported Pricefx versions

pyfxlib supports Pricefx backends from **15.0.0**: the current major version (17) and the two
before it.

Backends of the major before them (14.x) are deprecated: pyfxlib keeps working with them as it
did, but no longer supports them, and will drop the code for them when the next major is
released. They have no Query API. Older backends are not supported, and some calls may fail. In
both cases, a connection logs a warning the first time it fetches their version.

Some features need a more recent backend:
- notifications, from 15.2.0;
- pushing `LOB` fields, from 16.3.13 or 17.0.4;
- some tables, functions and other features of the Query API (`query_meta`, `query_execute`),
  whose use raises `UnsupportedQueryError` on an older backend.

## Quick Start

To connect to  a Pricefx partition:

```python
from pyfxlib.lowlevel import Session
session = Session.from_partition_url(
  "https://your-partition.pricefx.eu",
  username="your-username",
  password="your-password"
)
```

And then use the session to interact with the platform (see [documentation](https://developer.pricefx.eu/pricefx-api/pyfxlib/index.html) for full API details)

## Documentation

Full API documentation is available at: https://developer.pricefx.eu/pricefx-api/pyfxlib/index.html

See [CHANGELOG](https://knowledge.pricefx.com/configuration-engineer-knowledge-base/reference/optimization-reference/engines/pyfxlib-python-sdk-for-pricefx/pyfxlib-changelog) for release notes.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
