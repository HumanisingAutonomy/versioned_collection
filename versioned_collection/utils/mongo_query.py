from typing import List, Dict, Any, Optional, Tuple, Callable, Union

from bson import ObjectId
from pymongo.cursor import Cursor

from versioned_collection.utils.data_structures import hashabledict


def group_documents_by_id(
    documents: Union[List[Dict[str, Any]], Cursor[Dict[str, Any]]]
) -> Dict[Any, Dict[str, Any]]:
    """Group a collection of documents by id."""
    return dict(
        [
            (doc['_id'], doc)
            if not isinstance(doc['_id'], dict)
            else (hashabledict(doc['_id']), doc)
            for doc in documents
        ]
    )


def generate_pagination_query(
    query: Dict[str, Any],
    sort: Optional[Tuple[str, int]] = None,
    last_key: Optional[Dict[str, ObjectId]] = None,
) -> Tuple[
    Dict[str, Any],
    Callable[[List[Dict[str, Any]]], Optional[Dict[str, ObjectId]]],
]:
    """Generate a pagination query.

    :param query: The base query.
    :param sort: The field use to sort and the direction of sorting.
    :param last_key: The id of the last document from the previous page.
    :return: The query used to get the new page and the function used to get
        the id of the last entry from the previous page.
    """
    # Source:
    # https://medium.com/swlh/mongodb-pagination-fast-consistent-ece2a97070f3

    sort_field = None if sort is None else sort[0]

    def last_key_fn(items):
        if len(items) == 0:
            return None
        item = items[-1]
        if sort_field is None:
            return {'_id': item['_id']}
        else:
            return {'_id': item['_id'], sort_field: item[sort_field]}

    if last_key is None:
        return query, last_key_fn

    paginated_query = query.copy()

    if sort is None:
        paginated_query['_id'] = {'$gt': last_key['_id']}
        return paginated_query, last_key_fn

    sort_operator = '$gt' if sort[1] == 1 else '$lt'

    # fmt: off
    pagination_query = [
        {sort_field: {sort_operator: last_key[sort_field]}},
        {'$and': [
            {sort_field: last_key[sort_field]},
            {'_id': {sort_operator: last_key['_id']}},
        ]},
    ]
    # fmt: on

    if '$or' not in paginated_query:
        paginated_query['$or'] = pagination_query
    else:
        paginated_query = {'$and': [query, {'$or': pagination_query}]}

    return paginated_query, last_key_fn
