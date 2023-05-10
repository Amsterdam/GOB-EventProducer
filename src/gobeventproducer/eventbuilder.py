from gobcore.typesystem import get_gob_type_from_info

from gobeventproducer import gob_model
from gobeventproducer.utils.relations import RelationInfoBuilder


class EventDataBuilder:
    """Helper class that generates external event data."""

    def __init__(self, catalogue_name: str, collection_name: str):
        self.collection = gob_model[catalogue_name]["collections"][collection_name]
        self.relations = RelationInfoBuilder.build(catalogue_name, collection_name)

    def build_event(self, obj: object) -> dict:
        """Build event data for SQLAlchemy object."""
        result = {}
        for attr_name, attr in self.collection["fields"].items():
            if "Reference" in attr["type"]:
                relation = self.relations[attr_name]
                relation_table_rows = getattr(obj, f"{relation.relation_table_name}_collection")
                relation_obj = []
                for row in relation_table_rows:
                    dst_table = getattr(row, relation.dst_table_name)
                    rel = {
                        "tid": dst_table._tid,
                        "id": dst_table._id,
                        "begin_geldigheid": str(row.begin_geldigheid) if row.begin_geldigheid else None,
                        "eind_geldigheid": str(row.eind_geldigheid) if row.eind_geldigheid else None,
                    }
                    if hasattr(dst_table, "volgnummer"):
                        rel["volgnummer"] = dst_table.volgnummer

                    relation_obj.append(rel)

                if attr["type"] == "GOB.Reference":
                    result[attr_name] = relation_obj[0] if len(relation_obj) > 0 else {}
                else:
                    result[attr_name] = relation_obj
            else:
                # Skip relations
                gob_type = get_gob_type_from_info(attr)
                type_instance = gob_type.from_value(getattr(obj, attr_name))
                result[attr_name] = type_instance.to_value
        result["_gobid"] = getattr(obj, "_gobid")
        return result
