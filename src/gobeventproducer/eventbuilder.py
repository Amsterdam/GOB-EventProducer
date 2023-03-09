from gobcore.model.relations import get_relations_for_collection, split_relation_table_name
from gobcore.typesystem import get_gob_type_from_info

from gobeventproducer import gob_model


class EventDataBuilder:
    """Helper class that generates external event data."""

    def __init__(self, gob_db_session, gob_db_base, catalogue: str, collection_name: str):
        self.db_session = gob_db_session
        self.base = gob_db_base

        self.collection = gob_model[catalogue]["collections"][collection_name]
        self.tablename = gob_model.get_table_name(catalogue, collection_name)
        self.basetable = getattr(self.base.classes, self.tablename)

        self._init_relations(catalogue, collection_name)

    def _init_relations(self, catalogue: str, collection_name: str):
        self.relations = {}

        for attr_name, relname in get_relations_for_collection(gob_model, catalogue, collection_name).items():
            rel_table_name = gob_model.get_table_name("rel", relname)
            self.relations[attr_name] = {
                "relation_table_name": rel_table_name,
                "dst_table_name": self._get_rel_dst_tablename(rel_table_name),
            }

    def _get_rel_dst_tablename(self, rel_table_name: str):
        info = split_relation_table_name(rel_table_name)
        reference = gob_model.get_reference_by_abbreviations(info["dst_cat_abbr"], info["dst_col_abbr"])
        return gob_model.get_table_name_from_ref(reference)

    def build_event(self, tid: str) -> dict:
        """Build event data for object with given tid."""
        query = self.db_session.query(self.basetable).filter(self.basetable._tid == tid)
        obj = query.one()

        result = {}
        for attr_name, attr in self.collection["attributes"].items():
            if "Reference" in attr["type"]:
                relation = self.relations[attr_name]
                relation_table_rows = getattr(obj, f"{relation['relation_table_name']}_collection")
                relation_obj = []
                for row in relation_table_rows:
                    dst_table = getattr(row, relation["dst_table_name"])
                    rel = {
                        "tid": dst_table._tid,
                        "id": dst_table._id,
                        "begin_geldigheid": str(row.begin_geldigheid) if row.begin_geldigheid else None,
                        "eind_geldigheid": str(row.eind_geldigheid) if row.eind_geldigheid else None,
                    }
                    if hasattr(dst_table, "volgnummer"):
                        rel["volgnummer"] = dst_table.volgnummer

                    relation_obj.append(rel)

                result[attr_name] = relation_obj
            else:
                gob_type = get_gob_type_from_info(attr)
                type_instance = gob_type.from_value(getattr(obj, attr_name))
                result[attr_name] = str(type_instance.to_value)
        return result
