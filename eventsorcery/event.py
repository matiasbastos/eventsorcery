# -*- coding: utf-8 -*-

class Event(object):
    """
    Generic event container
    """
    sequence = 0
    _is_dirty = False

    def __init__(self, **kwargs):
        """
        Init event using kwargs to create properies.

        :param kwargs: dict containing event values.
        """
        self.__dict__.update(kwargs)

    def _clean(self) -> dict:
        """
        Return clean dict with current object properies.

        :return: dict
        """
        return {k:v for k, v in self.__dict__.items() if not k.startswith('_')}



