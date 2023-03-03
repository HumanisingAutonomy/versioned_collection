from __future__ import annotations

from enum import Enum
from functools import reduce
from typing import List, Optional


def reduce_event_sequence(events: List[str]) -> Optional[str]:
    """ Reduces a list of events to a single event.

    .. warning::
        'Upserts' should be fed in as inserts, so 'update' events have to
        be split into 'pure updates' and inserts.

    Raises :class:`ValueError` if invalid events are given. If the first
    event in the sequence is an insertion and the last one is a deletion,
    then the correctness of the events in between is ignored,

    Reducing a sequence of events that modified a document to a single event
    means finding the event type that logically can summarise the sequence,
    i.e., the single operation that can be applied to the document to bring
    it into the same state obtained by sequentially applying the events in the
    sequence.


    The event reduction rules are the following:

    ::

        1.
            insert -> ... -> delete
          --------------------------- (no-op shortcut)
                    no-op
        2.
               insert -> delete
            --------------------- (no-op)
                    no-op
        3.
               insert -> update
            --------------------- (insert)
                    insert
        4.
               delete -> insert
            --------------------- (update 1)
                    update
        5.
               update -> update
            --------------------- (update 2)
                    update
        6.
               update -> delete
            --------------------- (delete)
                    delete

    In addition to the above rules, there is the following reduction rule
    applied only to intermediary results:

    ::

        7^.
                 no-op -> any
            --------------------- (no-op elimination)
                      any

    where 'any' can be either of insert, update, delete or no-op.

    :param events: The list of events to be reduced. This sequence
        should consist of pure events. The permitted events and their
        representation are inserts ('i'), (pure) updates ('u') and
        deletions ('d').
    :return: The event to which the sequence reduces to. If the sequence
        reduces to a no-op event, ``None`` is returned.
    """

    class Event(Enum):
        INSERT = 'i'
        UPDATE = 'u'
        DELETE = 'd'
        NOOP = None

        @staticmethod
        def get(e: str) -> Event:
            if not isinstance(e, str):
                raise ValueError(
                    f"Events should be strings, found {type(e)}, for {e}"
                )
            if e == 'u':
                ret = Event.UPDATE
            elif e == 'i':
                ret = Event.INSERT
            elif e == 'd':
                ret = Event.DELETE
            else:
                raise ValueError(
                    f"Invalid value for event '{e}'. "
                    f"The allowed values are one of ['i', 'u', 'd']"
                )
            return ret

    def _reduce_fn(e1: Event, e2: Event) -> Event:
        if e1 == Event.NOOP:
            # Rule 7^
            ret = e2
        elif e1 == Event.INSERT and e2 == Event.DELETE:
            # Rule 2
            ret = Event.NOOP
        elif e1 == Event.INSERT and e2 == Event.UPDATE:
            # Rule 3
            ret = Event.INSERT
        elif e1 == Event.DELETE and e2 == Event.INSERT:
            # Rule 4
            ret = Event.UPDATE
        elif e1 == Event.UPDATE == e2:
            # Rule 5
            ret = Event.UPDATE
        elif e1 == Event.UPDATE and e2 == Event.DELETE:
            # Rule 6
            ret = Event.DELETE
        else:
            raise ValueError(
                f"Invalid sequence of events '{e1.value} -> {e2.value}'."
            )
        return ret

    if events is None or len(events) == 0:
        raise ValueError(
            f"Invalid input events sequence. "
            f"Expected a non-empty list, but got '{events}'.")
    if len(events) == 1:
        # check if valid
        Event.get(events[0])
        return events[0]
    if events[0] == 'i' and events[-1] == 'd':
        # Rule 1
        return None

    return reduce(_reduce_fn, [Event.get(e) for e in events]).value
