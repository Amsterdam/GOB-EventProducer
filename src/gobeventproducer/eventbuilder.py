from gobcore.typesystem import get_gob_type_from_info

from gobeventproducer import gob_model


class EventDataBuilder:
    """Helper class that generates external event data."""

    def __init__(self, gob_db_session, gob_db_base, catalogue: str, collection_name: str):
        self.db_session = gob_db_session
        base = gob_db_base

        self.collection = gob_model[catalogue]["collections"][collection_name]
        tablename = gob_model.get_table_name(catalogue, collection_name)
        self.basetable = getattr(base.classes, tablename)

    def build_event(self, tid: str) -> dict:
        """Build event data for object with given tid."""
        query = self.db_session.query(self.basetable).filter(self.basetable._tid == tid)
        obj = query.one()

        result = {}
        for attr_name, attr in self.collection["attributes"].items():
            if "Reference" not in attr["type"]:
                # Skip relations
                gob_type = get_gob_type_from_info(attr)
                type_instance = gob_type.from_value(getattr(obj, attr_name))
                result[attr_name] = type_instance.to_value
        result["_gobid"] = getattr(obj, "_gobid")
        return result
