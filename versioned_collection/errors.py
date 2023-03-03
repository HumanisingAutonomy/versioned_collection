""" Custom errors used in `versioned_collection` """
from typing import List

from bson import ObjectId


class CollectionAlreadyInitialised(Exception):
    """ Raised when an initialised collection is tried to be reinitialised. """

    __DEFAULT_MESSAGE = (
        "You tried to call `init()` on an already initialised versioned "
        "collection."
    )

    def __init__(self, message: str = __DEFAULT_MESSAGE) -> None:
        super().__init__(message)


class InvalidOperation(Exception):
    """ Raised when an invalid operation is attempted on the collection. """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InvalidCollectionVersion(Exception):
    """ Raised when trying to check out to a wrong collection version. """

    __DEFAULT_MESSAGE = (
        "You tried to checkout to an nonexistent version of the target tracked "
        "collection. The version identified by (version_id: {}, branch: {}) "
        "does not exist."
    )

    def __init__(self,
                 version: int,
                 branch: str,
                 message: str = __DEFAULT_MESSAGE
                 ) -> None:
        message = message.format(version, branch)
        super().__init__(message)


class InvalidCollectionState(Exception):
    """ Raised when something terrible happens. """
    pass


class BranchNotFound(Exception):
    """ Raised when a branch could not be found  """

    def __init__(self, branch_name: str) -> None:
        super().__init__(f"Branch '{branch_name}' does not exists!")


class AutoMergeFailedError(Exception):
    """ Raised when automatically merging two branches produces conflicts. """

    def __init__(self, branch: str) -> None:
        super().__init__(
            f"Automatic merge failed. Fix conflicts for branch "
            f"{branch} and then register a new version. \n"
            f"For manually solving conflicts call `resolve_conflicts()`"
        )
