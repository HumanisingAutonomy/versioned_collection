from importlib.metadata import PackageNotFoundError, version

from versioned_collection.collection.versioned_collection import (
    VersionedCollection,
)

try:
    __version__ = version("versioned_collection")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ['__version__', 'VersionedCollection']
