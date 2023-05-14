from collections import defaultdict
from pathlib import Path
from typing import Literal, Optional, Union

import yaml
from pydantic import BaseModel

FieldMappingTypes = Union[str, "MappedObjectDefinition"]
FieldMapping = dict[str, FieldMappingTypes]


class MappedObjectDefinition(BaseModel):
    """Represents a mapped object."""

    type: Literal["object"]
    mapping: FieldMapping


class MappingDefinition(BaseModel):
    """The object representation of a YAML mapping file."""

    catalog: str
    collection: str
    version: str  # The Amsterdam Schema this definition maps to.
    mapping: FieldMapping


class MappingDefinitionLoader:
    """Entry class for fetching a MappingDefinition."""

    _mapping_definitions = None

    def __init__(self):
        if not self._mapping_definitions:
            self._load()

    def _load_mapping_definition(self, path: Path) -> MappingDefinition:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            mapping_definition = MappingDefinition.parse_obj(data)
            return mapping_definition

    def _load(self):
        p = Path(__file__).parent
        mapping_definitions: dict[str, dict[str, MappingDefinition]] = defaultdict(dict)
        for file in p.glob("**/*.yml"):
            mapping_definition = self._load_mapping_definition(file)
            mapping_definitions[mapping_definition.catalog][mapping_definition.collection] = mapping_definition
        self._mapping_definitions = mapping_definitions

    def get(self, catalog: str, collection: str) -> Optional[MappingDefinition]:
        """Get the MappingDefinition for catalog/collection. Returns None if not exists."""
        try:
            return self._mapping_definitions[catalog][collection]  # type: ignore[index]
        except KeyError:
            return None
