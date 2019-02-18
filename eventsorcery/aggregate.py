# -*- coding: utf-8 -*-
from collections import deque
from eventsorcery.fields import BaseField, SetField
from eventsorcery.event import Event
from eventsorcery.base_backend import BaseBackend

MODEL_BASE = '_metaclass_helper_'


def with_metaclass(meta, base=object):
    return meta(MODEL_BASE, (base,), {})


class AggregateMeta(type):
    SCHEMA = '_schema'

    def __new__(mcs, name, bases, attrs):
        # add fields to schema
        schema = {}
        for base in bases:
            if mcs.SCHEMA in base.__dict__:
                schema.update(getattr(base, mcs.SCHEMA))
        # get current cls Fields and add them to schema
        fields = {k: v for k, v in attrs.items() if isinstance(v, BaseField)}
        attrs['_schema'] = {**schema, **fields}
        # replace fields
        for k, v in fields.items():
            attrs[k] = None
        # return new class
        return super().__new__(mcs, name, bases, attrs)


class Aggregate(with_metaclass(AggregateMeta)):
    class Meta:  # meta class for the aggregate setup
        backend = None
        snapshot_model = None
        event_model = None

    _sequence_offset = 0  # used to mark the sequence offset
    _events = deque()  # current list of events in the aggregate
    _snapshot = {}  # holds the latest snapshot
    aggregate_id = SetField(column='aggregate_id')

    def __init__(self, *args, **kwargs):
        # check if `Meta.backend` is set and is instance of `BaseBackend`
        if not hasattr(self.Meta, 'backend') \
                and isinstance(getattr(self.Meta, 'backend'), BaseBackend):
            raise NotImplementedError(
                '`Meta.backend` in Aggregate %s is missing or is '
                'not `BaseBackend` type.' % self)
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
        snapshot = self.Meta \
            .backend \
            .get_latest_snapshot(self.aggregate_id,
                                 model=self.Meta.snapshot_model)
        if snapshot:
            self._snapshot = snapshot
            self._sequence_offset = snapshot.sequence
            # apply snapshot values to current aggregate
            self._process_event(snapshot, is_snapshot=True)

        self._events = deque(self.Meta
                             .backend
                             .get_events(self.aggregate_id,
                                         model=self.Meta.event_model,
                                         sequence=self._sequence_offset))

    def _process_all_events(self):
        # iterate data
        for event in self._events:
            self._process_event(event)

    def _process_event(self, event: dict, is_snapshot: bool = False):
        # iterate fields
        for field_name, field in self._schema.items():
            # get previous value
            previous_value = getattr(self, field_name)
            current_value = getattr(event,
                                    field.column
                                    if not is_snapshot else field_name,
                                    None)
            # get new value
            new_value = field.calculate(previous_value=previous_value,
                                        current_value=current_value)
            # TODO Validate
            # set new value
            if new_value:
                setattr(self, field_name, new_value)

    def append(self, event: object):
        event.aggregate_id = self.aggregate_id
        # convert object to event object
        new_event = self.Meta.backend.to_event(event)
        # get latest sequence
        latest_sequence = self._sequence_offset + len(self._events)
        # asign sequence
        new_event.sequence = latest_sequence + 1
        new_event.is_dirty = True
        self._events.append(new_event)
        # calculate aggregate fields
        self._process_event(new_event)

    def commit(self):
        # iterate events and save them using the current backend
        [self.Meta.backend.save_event(event, model=self.Meta.event_model)
         for event
         in self._events
         if event.is_dirty]

    def create_snapshot(self):
        fields = {}
        # iterate fields
        for field_name, field in self._schema.items():
            # get previous value
            fields[field_name] = getattr(self, field_name)
        # get latest sequence
        fields['sequence'] = self._sequence_offset + len(self._events)
        # create snapshot event
        snapshot = Event(**fields)
        # persist it
        self.Meta.backend.save_snapshot(snapshot,
                                        model=self.Meta.snapshot_model)
