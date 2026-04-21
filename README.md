# pyfxlib

A set of utilities to be able to use the Pricefx API from a Python package.

## Installation

```bash
pip install pyfxlib
```

Or with Poetry:

```bash
poetry add pyfxlib
```

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

## Contributing

See `CONTRIBUTING.md` for development setup, testing, and release process.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
