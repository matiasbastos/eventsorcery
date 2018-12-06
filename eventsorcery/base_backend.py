# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any, List
from eventsorcery.event import Event


class BaseBackend(ABC):
    @staticmethod
    @abstractmethod
    def to_event(aggregate_id: Any, obj: object, **kwargs) -> Event:
        pass

    @staticmethod
    @abstractmethod
    def to_object(event: Event, **kwargs)->object:
        pass

    @abstractmethod
    def get_events(self, aggregate_id: Any, **kwargs)->List[dict]:
        pass

    @abstractmethod
    def save_event(self, event, **kwargs)->None:
        pass
