# -*- coding: utf-8 -*-
from unittest.mock import MagicMock
import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Column
from sqlalchemy.types import String, Integer, BigInteger
from eventsorcery.backends.sqlalchemy_backend import SQLAlchemyBackend
from eventsorcery.event import Event


Base = declarative_base()


class TestEvent(Base):
    __tablename__ = 'test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    aggregate_id = Column(String)
    sequence = Column(BigInteger)
    field1 = Column(BigInteger)
    field2 = Column(String)


def test_to_event():
    model_event = TestEvent(aggregate_id=1, sequence=2, field1=3, field2='4')
    event = SQLAlchemyBackend.to_event(model_event)
    assert isinstance(event, Event)
    assert hasattr(event, 'aggregate_id')
    assert hasattr(event, 'sequence')
    assert hasattr(event, 'field1')
    assert hasattr(event, 'field2')
    assert getattr(event, 'aggregate_id') == 1
    assert getattr(event, 'sequence') == 2
    assert getattr(event, 'field1') == 3
    assert getattr(event, 'field2') == '4'


def test_to_object():
    event_dict = {'aggregate_id': True,
                  'sequence': 0,
                  'field1': 1,
                  'field2': '2'}
    event = Event(**event_dict)

    with pytest.raises(ValueError):
        event_model = SQLAlchemyBackend.to_object(event)

    event_model = SQLAlchemyBackend.to_object(event, model=TestEvent)
    assert isinstance(event_model, TestEvent)
    for k in event_dict.keys():
        assert hasattr(event_model, k)
        assert getattr(event_model, k) == event_dict[k]


def test_get_events():
    session_mock = MagicMock()
    session_mock.query.filter.order_by.__iter__.return_value = []
    be = SQLAlchemyBackend(session_mock)
    with pytest.raises(ValueError):
        events = be.get_events(1, 1)
    events = be.get_events(1, 1, model=TestEvent)
    assert events == []


def test_get_latest_snapshot():
    model_event = TestEvent(aggregate_id=1, sequence=2, field1=3, field2='4')
    session_mock = MagicMock()

    session_mock.query().filter().order_by().first.return_value = model_event

    be = SQLAlchemyBackend(session_mock)
    with pytest.raises(ValueError):
        events = be.get_latest_snapshot(1)
    event = be.get_latest_snapshot(1, model=TestEvent)
    assert isinstance(event, Event)


def save_event():
    pass


def save_snapshot():
    pass
