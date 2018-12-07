# -*- coding: utf-8 -*-
from typing import Any, List, Optional
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import UniqueConstraint, Index

from eventsorcery.base_backend import BaseBackend
from eventsorcery.event import Event


class SQLAlchemyBackend(BaseBackend):
    session = None

    def __init__(self, session: None):
        self.session = session

    @staticmethod
    def to_event(obj: object, **kwargs) -> Event:
        return Event(**{k: v
                        for k, v
                        in obj.__dict__.items()
                        if not k.startswith('_')})

    @staticmethod
    def to_object(event: Event, **kwargs)->object:
        model = kwargs.get('model')
        if not model or not isinstance(model, DeclarativeMeta):
            raise ValueError('No SQLAlchemy model provided')
        return model(**event._clean())

    def get_events(self,
                   aggregate_id: Any,
                   sequence: Any = 0,
                   **kwargs)->List[dict]:
        # update session object
        if 'session' in kwargs:
            self.session = kwargs['session']
        # get event model
        model = kwargs.get('model')
        if not model or not isinstance(model, DeclarativeMeta):
            raise ValueError('No SQLAlchemy model provided')
        # get data
        if sequence:
            query = self.session.query(model) \
                                .filter(model.aggregate_id == aggregate_id,
                                        model.sequence > sequence) \
                                .order_by(model.sequence)
        else:
            query = self.session.query(model) \
                                .filter(model.aggregate_id == aggregate_id) \
                                .order_by(model.sequence)
        return [self.to_event(event) for event in query]

    def get_latest_snapshot(self,
                            aggregate_id: Any,
                            **kwargs)->Optional[Event]:
        # update session object
        if 'session' in kwargs:
            self.session = kwargs['session']
        # get event model
        model = kwargs.get('model')
        if not model or not isinstance(model, DeclarativeMeta):
            raise ValueError('No SQLAlchemy model provided')
        # get data
        snapshot = self.session.query(model) \
                               .filter(model.aggregate_id == aggregate_id) \
                               .order_by(model.sequence.desc()) \
                               .first()
        if not snapshot:
            return None
        return self.to_event(snapshot)

    def save_event(self, event, **kwargs)->None:
        # update session object
        if 'session' in kwargs:
            self.session = kwargs['session']
        # get event model
        model = kwargs.get('model')
        if not model or not isinstance(model, DeclarativeMeta):
            raise ValueError('No SQLAlchemy model provided')
        event_row = self.to_object(event, model=model)
        self.session.add(event_row)
        self.session.commit()

    def save_snapshot(self, event, **kwargs)->None:
        # update session object
        if 'session' in kwargs:
            self.session = kwargs['session']
        # get event model
        model = kwargs.get('model')
        if not model or not isinstance(model, DeclarativeMeta):
            raise ValueError('No SQLAlchemy model provided')
        event_row = self.to_object(event, model=model)
        self.session.add(event_row)
        self.session.commit()
