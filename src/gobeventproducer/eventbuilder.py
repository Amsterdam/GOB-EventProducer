from gobcore.typesystem import get_gob_type_from_info

from gobeventproducer import gob_model


class EventDataBuilder:
    """Helper class that generates external event data."""

    def __init__(self, catalogue_name: str, collection_name: str):
        self.collection = gob_model[catalogue_name]["collections"][collection_name]

    def build_event(self, obj: object) -> dict:
        """Build event data for SQLAlchemy object."""
        result = {}
        for attr_name, attr in self.collection["attributes"].items():
            if "Reference" not in attr["type"]:
                # Skip relations
                gob_type = get_gob_type_from_info(attr)
                type_instance = gob_type.from_value(getattr(obj, attr_name))
                result[attr_name] = type_instance.to_value
        result["_gobid"] = getattr(obj, "_gobid")
        return result
