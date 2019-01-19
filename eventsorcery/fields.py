# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any


class BaseField(ABC):
    def __init__(self, column=None):
        self.column = column

    @abstractmethod
    def calculate(self, event: Any)->Any:
        pass


class SumField(BaseField):
    default_value = 0

    def calculate(self, previous_value: Any, current_value: Any)->Any:
        return (previous_value or self.default_value) \
               + (current_value or self.default_value)


class SetField(BaseField):
    def calculate(self, previous_value: Any, current_value: Any)->Any:
        return current_value
