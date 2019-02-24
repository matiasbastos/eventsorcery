# -*- coding: utf-8 -*-
import pytest
from eventsorcery.fields import SumField, SetField


def test_sum_field_calculate():
    sf = SumField('test')
    assert sf.column == 'test'
    assert sf.calculate(None, None) == 0
    assert sf.calculate(1, None) == 1
    assert sf.calculate(None, 1) == 1
    assert sf.calculate(10, 10) == 20


def test_set_field_calculate():
    sf = SetField('test')
    assert sf.column == 'test'
    assert sf.calculate(None, None) is None
    assert sf.calculate(1, None) is None
    assert sf.calculate(1, 2) == 2
