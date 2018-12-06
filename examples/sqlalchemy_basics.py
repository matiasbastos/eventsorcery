# -*- coding: utf-8 -*-
import pudb
# eventsorcery imports
from eventsorcery.aggregate import Aggregate
from eventsorcery.backends.sqlalchemy_backend import SQLAlchemyBackend
from eventsorcery.fields import SumField, SetField
# sqlalchemy imports
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Column
from sqlalchemy.types import String, Integer, BigInteger


# sqlalchemy setup
engine = create_engine('sqlite://')
# engine = create_engine('sqlite:///test.sql')
maker = sessionmaker(autocommit=False, autoflush=True, bind=engine)
Session = scoped_session(maker)
metadata = MetaData()


@as_declarative()
class Base:
    query = Session.query_property()


class WalletEvent(Base):
    __tablename__ = "wallet_event"

    id = Column(Integer, primary_key=True, autoincrement=True)
    aggregate_id = Column(String(length=20))  # unique together with sequence
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


pu.db  # DEBUG!
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
