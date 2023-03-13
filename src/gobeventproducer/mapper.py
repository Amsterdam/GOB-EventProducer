from abc import abstractmethod
from typing import Any

from gobeventproducer.mapping import FieldMappingTypes, MappedObjectDefinition, MappingDefinition

EventData = dict[str, Any]


class BaseEventDataMapper:
    """BaseEventDataMapper."""

    @abstractmethod
    def map(self, eventdata: dict):
        """Map the eventdata to the desired format."""
        pass


class PassThroughEventDataMapper(BaseEventDataMapper):
    """EventDataMapper that performs no transformation. Used for simplicity in calling code only."""

    def map(self, eventdata: dict):
        """Map the eventdata to the desired format."""
        return eventdata


class EventDataMapper(BaseEventDataMapper):
    """Map the internal GOB data to an event for a specific Amsterdam Schema version."""

    def __init__(self, mapping_definition: MappingDefinition):
        self.mapping_definition = mapping_definition

    def map(self, eventdata: EventData) -> EventData:
        """Map the eventdata to the desired format."""

        def get_value(eventdata: dict, fieldmapping: FieldMappingTypes):
            if isinstance(fieldmapping, MappedObjectDefinition):
                return {
                    newkey: get_value(eventdata, oldkey_or_definition)
                    for newkey, oldkey_or_definition in fieldmapping.mapping.items()
                }
            elif isinstance(fieldmapping, str):
                return eventdata.get(fieldmapping)
            else:  # pragma: nocover
                raise NotImplementedError("Fieldmapping of unexpected type. Please implement.")

        return {newkey: get_value(eventdata, mapping) for newkey, mapping in self.mapping_definition.mapping.items()}
