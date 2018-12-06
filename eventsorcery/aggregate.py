# -*- coding: utf-8 -*-
from collections import deque
from eventsorcery.fields import BaseField, Field, SetField
from eventsorcery.event import Event

MODEL_BASE = '_metaclass_helper_'


def with_metaclass(meta, base=object):
    """
    Function desc

    :param param_name: short desc
    :return: type
    """
    return meta(MODEL_BASE, (base,), {})


class AggregateMeta(type):
    SCHEMA = '_schema'

    def __new__(cls, name, bases, attrs):
        # add fields to schema
        schema = {}
        for base in bases:
            if cls.SCHEMA in base.__dict__:
                schema.update(getattr(base, cls.SCHEMA))
        # get current cls Fields and add them to schema
        fields = {k: v for k, v in attrs.items() if isinstance(v, BaseField)}
        attrs['_schema'] = {**schema, **fields}
        # replace fields
        for k, v in fields.items():
            attrs[k] = None
        # return new class
        return super().__new__(cls, name, bases, attrs)


class Aggregate(with_metaclass(AggregateMeta)):
    Meta = type
    _events = deque()
    _snapshot = {}
    aggregate_id = Field(column='aggregate_id')
    sequence = SetField(column='sequence')

    def __init__(self, *args, **kwargs):
        # check if `Meta.backend` is set and is instance of `BaseBackend`
        if not hasattr(self.Meta, 'backend') and isinstance(getattr(self.Meta, 'backend'), BaseBackend):
            raise NotImplementedError(
                '`Meta.backend` in Aggregate %s is missing or is not `BaseBackend` type.' % self)
        # check if `Meta.model` is set
        if not hasattr(self.Meta, 'event_model'):
            raise NotImplementedError(
                '`Meta.event_model` in Aggregate %s is missing.' % self)
        # initialize aggregate_id value
        if args:
            self.aggregate_id = args[0]
        elif 'aggregate_id' in kwargs:
            self.aggregate_id = kwargs['aggregate_id']
        else:
            raise NotImplementedError('fuck off, you need an aggregate_id!')
        # get data from backend
        self._get_events()
        # process all events
        self._process_all_events()

    def _get_events(self):
        event = Event()
        self._events = deque(self.Meta.backend.get_events(self.aggregate_id,
                                                          model=self.Meta.event_model))

    def _process_all_events(self):
	# iterate data
        for event in self._events:
            self._process_event(event)

    def _process_event(self, event: dict):
	# iterate fields
        for field_name, field in self._schema.items():
            # get previous value
            previous_value = getattr(self, field_name)
            # get new value
            new_value = field.calculate(previous_value=previous_value,
                                        current_value=getattr(event, field.column, None))  # TODO Validate
            # set new value
            if new_value:
                  setattr(self, field_name, new_value)

    def append(self, event: object):
        # convert object to dict
        new_event = self.Meta.backend.to_event(event)
        # get latest sequence
        latest_sequence = 0
        if self._events:
            latest_sequence = self._events[-1].sequence
        # asign sequence order
        new_event.sequence = latest_sequence + 1
        new_event._is_dirty = True
        self._events.append(new_event)
	# calculate aggregate fields
        self._process_event(new_event)
	
    def commit(self):
        # iterate events
        [self.Meta.backend.save_event(event, model=self.Meta.event_model)
         for event
         in self._events
         if event._is_dirty]
