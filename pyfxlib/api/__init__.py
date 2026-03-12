"""Interacting with a Pricefx platform using a high-level API.

The purpose of this module is to provide a API to interact with the platform.

Pricefx Python Client can be used outside a Pricefx logic. One can use the class method
`domain.Partition.connect` to manually connect to a Pricefx partition and use the
returned `domain.Partition` object methods to navigate the partition objects.
"""

from pyfxlib.api.domain import Partition

from . import domain

__all__ = ["domain", "Partition"]
