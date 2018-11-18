# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from collections import deque
from copy import deepcopy
from typing import Any, List


from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.schema import MetaData
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy import UniqueConstraint, Index
from sqlalchemy.schema import Column
from sqlalchemy.types import String, Integer, Text, BigInteger
from sqlalchemy.orm import relationship, foreign, remote, validates

engine = create_engine('sqlite://')

maker = sessionmaker(autocommit=False, autoflush=True, bind=engine)
Session = scoped_session(maker)
metadata = MetaData()


@as_declarative()
class Base:
    query = Session.query_property()


MODEL_BASE = '_metaclass_helper_'


def with_metaclass(meta, base=object):
    return meta(MODEL_BASE, (base,), {})

class BaseField(ABC):
    def __init__(self, column=None):
        self.column = column

    @abstractmethod
    def calculate(self, event: Any)->Any:
        pass

class Field(BaseField):
    def calculate(self, previous_value: Any, current_value: Any)->Any:
        return None

class SumField(BaseField):
    def calculate(self, previous_value: Any, current_value: Any)->Any:
        if not previous_value:
            previous_value = 0
        return previous_value + current_value

class SetField(BaseField):
    def calculate(self, previous_value: Any, current_value: Any)->Any:
        return current_value


class BaseBackend(ABC):
    @staticmethod
    @abstractmethod
    def to_dict(obj: object, **kwargs)->dict:
        pass

    @staticmethod
    @abstractmethod
    def to_object(event: dict, **kwargs)->object:
        pass

    @abstractmethod
    def get_events(self, aggregate_id: Any, **kwargs)->List[dict]:
        pass

    @abstractmethod
    def save_event(self, event, **kwargs)->None:
        pass

class SQLAlchemyBackend(BaseBackend):
    session = None

    def __init__(self, session: None):
        self.session = session

    @staticmethod
    def to_dict(obj: object, **kwargs)->dict:
        return {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}

    @staticmethod
    def to_object(event: dict, **kwargs)->object:
        model = kwargs.get('model')
        if not model or not isinstance(model, DeclarativeMeta):
            raise ValueError('No SQLAlchemy model provided')
        return model(**event)
        

    def get_events(self, aggregate_id: Any, **kwargs)->List[dict]:
        # update session object
        if 'session' in kwargs:
            self.session = kwargs['session']
        # get event model
        model = kwargs.get('model')
        if not model or not isinstance(model, DeclarativeMeta):
            raise ValueError('No SQLAlchemy model provided')
        # get data
        return [self.to_dict(event)
	 	for event 
		in self.session.query(model).order_by(model.sequence)] # TODO check this order bullshit
		
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
                                        current_value=event.get(field.column))  # TODO Validate
            # set new value
            if new_value:
                  setattr(self, field_name, new_value)

    def append(self, event: object):
        # convert object to dict
        new_event = self.Meta.backend.to_dict(event)
        # get latest sequence
        latest_sequence = 0
        if self._events:
            latest_sequence = self._events[-1]['sequence']
        # asign sequence order
        new_event['sequence'] = latest_sequence + 1
        self._events.append(new_event)
	# calculate aggregate fields
        self._process_event(new_event)
	
    def commit(self):
        # iterate events
        [self.Meta.backend.save_event(event, model=self.Meta.event_model)
         for event
         in self._events]

##################### Usage #########################
class WalletEvent(Base):
    __tablename__ = "wallet_event"

    id = Column(Integer, primary_key=True, autoincrement=True)
    aggregate_id = Column(String(length=20)) # unique together with sequence
    sequence = Column(BigInteger)
    amount = Column(BigInteger)
    status = Column(String(length=10))


class WalletSnapshot(Base):
    __tablename__ = "wallet_snapshot"

    id = Column(Integer, primary_key=True, autoincrement=True)
    aggregate_id = Column(String(length=20))
    sequence = Column(BigInteger)
    amount = Column(BigInteger)
    status = Column(String(length=10))

Base.metadata.create_all(engine)

class WalletAggregate(Aggregate):
    class Meta:
        backend = SQLAlchemyBackend(session=Session)
        event_model = WalletEvent
        snapshot_model = WalletSnapshot

    balance = SumField('amount')
    status = SetField('status')


wallet = WalletAggregate(1)
wallet.append(WalletEvent(amount=10, status='fuck'))
assert wallet.balance == 10
assert wallet.status == 'fuck'
wallet.append(WalletEvent(amount=10, status='shit'))
assert wallet.balance == 20
assert wallet.status == 'shit'
wallet.commit()
wallet = WalletAggregate(1)
assert wallet.balance == 20
assert wallet.status == 'shit'
