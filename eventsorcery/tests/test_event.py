# -*- coding: utf-8 -*-
import pytest
from eventsorcery.event import Event


def test_init_ok():
    event = Event(**{'aggregate_id': True, 'var1': 1, 'var2': '2'})
    assert event.aggregate_id
    assert event.sequence == 0
    assert not event._is_dirty
    assert event.var1 == 1
    assert event.var2 == '2'


def test_init_error():
    with pytest.raises(ValueError):
        event = Event(**{'var1': 1, 'var2': '2'})


def test_clean():
    event_dict = {'aggregate_id': True, 'sequence': 0, 'var1': 1, '_var2': 2}
    event = Event(**event_dict)
    assert event._clean() == {'aggregate_id': True, 'sequence': 0, 'var1': 1}
