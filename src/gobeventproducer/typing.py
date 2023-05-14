"""Typing for GOB EventProducer."""


from typing import Any, NewType

EventData = NewType("EventData", dict[str, Any])
