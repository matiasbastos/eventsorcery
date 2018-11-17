# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from copy import deepcopy


from sqlalchemy.ext.declarative import as_declarative
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


class Field:
    def __init__(self, column=None):
        self.column = column


class SumField(Field):
    pass


class SetField(Field):
    pass


class AggregateMeta(type):
    def __new__(cls, name, bases, attrs):
        import pudb; pu.db
        # get Fields and send them to 
        fields = {k: v for k, v in attrs.items() if isinstance(v, Field)}
        if not '_schema' in attrs:
            attrs['_schema'] = {}
        attrs['_schema'].update(fields)
        if hasattr(bases[0], '_schema'):
            attrs['_schema'].update(getattr(bases[0], '_schema'))
        for k, v in fields.items():
            attrs[k] = 666
        if name == MODEL_BASE or bases[0].__name__ == MODEL_BASE:
            # is from the declaration
            return super().__new__(cls, name, bases, attrs)
        return type(MODEL_BASE, bases, attrs)


class Aggregate(with_metaclass(AggregateMeta)):
    _events = {}
    _snapshot = {}
    _schema = {}
    aggregate_id = Field(column='aggregate_id')
    sequence = Field(column='sequence')

    def __init__(self, *args, **kwargs):
        pass


class BaseBackend(ABC):
    @abstractmethod
    def new_event(self, *args, **kwargs):
        pass

    @abstractmethod
    def load_events(self, *args, **kwargs):
        pass

class SQLAlchemyBackend(BaseBackend):
    def new_event(self, *args, **kwargs):
        pass

    def load_events(self, *args, **kwargs):
        pass    
    
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
    
class WalletAggregate(Aggregate):
    class Meta:
        backend = SQLAlchemyBackend
        model = WalletEvent
        snapshot_model = WalletSnapshot

    balance = SumField('amount')
    status = SetField('status')


import pudb; pu.db
wallet = WalletAggregate(1)
wallet.append(WalletEvent(aggregate_id=1, amount=10))
wallet.commit()
