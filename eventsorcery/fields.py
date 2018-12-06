# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any


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
