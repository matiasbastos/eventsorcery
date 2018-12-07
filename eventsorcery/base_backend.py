# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any, List, Optional
from eventsorcery.event import Event


class BaseBackend(ABC):
    @staticmethod
    @abstractmethod
    def to_event(obj: object, **kwargs) -> Event:
        pass

    @staticmethod
    @abstractmethod
    def to_object(event: Event, **kwargs)->object:
        pass

    @abstractmethod
    def get_events(self,
                   aggregate_id: Any,
                   sequence: Any = 0,
                   **kwargs)->List[dict]:
        pass

    @abstractmethod
    def get_latest_snapshot(self,
                            aggregate_id: Any,
                            **kwargs)->Optional[Event]:
        pass

    @abstractmethod
    def save_event(self, event, **kwargs)->None:
        pass

    @abstractmethod
    def save_snapshot(self, event, **kwargs)->None:
        pass
