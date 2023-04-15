from dataclasses import dataclass

from gobcore.model.relations import get_relations_for_collection, split_relation_table_name  # type: ignore

from gobeventproducer import gob_model


@dataclass
class RelationInfo:
    """Holds the tablenames involved in a relation as used in this repo."""

    relation_table_name: str
    dst_table_name: str


class RelationInfoBuilder:
    """Holds the logic to build a dict with RelationInfo objects for a given collection."""

    @classmethod
    def build(cls, catalogue_name: str, collection_name: str) -> dict[str, RelationInfo]:
        """Build a dict with [relation_attr_name: RelationInfo] for the given collection."""
        result = {}

        for attr_name, relname in get_relations_for_collection(gob_model, catalogue_name, collection_name).items():
            rel_table_name = gob_model.get_table_name("rel", relname)
            result[attr_name] = RelationInfo(
                relation_table_name=rel_table_name, dst_table_name=cls._get_rel_dst_tablename(rel_table_name)
            )
        return result

    @classmethod
    def _get_rel_dst_tablename(cls, rel_table_name: str):
        info = split_relation_table_name(rel_table_name)
        reference = gob_model.get_reference_by_abbreviations(info["dst_cat_abbr"], info["dst_col_abbr"])
        return gob_model.get_table_name_from_ref(reference)
