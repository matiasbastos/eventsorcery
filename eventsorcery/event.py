# -*- coding: utf-8 -*-


class Event(object):
    """
    Generic event container
    """
    aggregate_id = None  # aggregate_id
    sequence = 0  # incremental sequence number
    _is_dirty = False  # used to mark the event to be persisted

    def __init__(self, **kwargs):
        """
        Init event using kwargs to create properties.

        :param kwargs: dict containing event values.
        """
        if 'aggregate_id' not in kwargs:
            raise ValueError('aggregate_id not in initialization of Event',
                             kwargs)
        self.__dict__.update(kwargs)

    def __repr__(self):
        return str(self.__dict__)

    def _clean(self) -> dict:
        """
        Return clean dict with current object properies.

        :return: dict
        """
        return {k: v
                for k, v
                in self.__dict__.items()
                if not k.startswith('_')}