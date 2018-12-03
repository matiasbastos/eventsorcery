# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any, List

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

