from pymongo.collection import Collection
from pymongo.database import Database


class _BaseTrackerCollection(Collection):
    """Base class for all the helper tracking collection."""

    _NAME_TEMPLATE = None

    def __init__(self, database: Database, name: str, **kwargs) -> None:
        super().__init__(database, self.format_name(name), **kwargs)
        self._target_collection_name = name
        self.__kwargs = kwargs

    def __eq__(self, other) -> bool:
        return self.name == other.name

    def __hash__(self) -> int:
        # dummy
        return 0  # pragma: nocover

    @classmethod
    def format_name(cls, collection_name: str) -> str:
        """Return this collection's name.

        Formats and returns this collection's name by appending the name of
        the target tracked collection to it.

        :param collection_name: The name of the target tracked collection.
        :return: The name of this collection
        """
        return cls._NAME_TEMPLATE.format(collection_name)

    def exists(self) -> bool:
        """Check whether this collection exists in the database."""
        return self.name in self.database.list_collection_names()

    def build(self, *args, **kwargs) -> bool:
        """Create the collection on the database."""
        if self.exists():
            self.drop()
        self.database.create_collection(self.name)
        return True

    def rename(self, parent_collection_name: str, *args, **kwargs) -> bool:
        """Rename this collection.

        See the :meth:`rename` method of the collection superclass
        :class:`pymongo.collection.Collection` for more info.

        The collection is renamed only if it physically exists in the database.

        :param parent_collection_name: The parent's collection name.
        :param args: the rest of the `args`.
        :param kwargs: the rest of the `kwargs`.
        :return: Whether the collection has been renamed.
        """
        if self.exists():
            name = self.format_name(parent_collection_name)
            super().rename(*args, new_name=name, **kwargs)
            return True
        return False
