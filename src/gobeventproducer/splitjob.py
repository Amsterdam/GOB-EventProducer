from typing import Any

from gobcore.message_broker.config import CONNECTION_PARAMS, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY
from gobcore.message_broker.message_broker import Connection as MessageBrokerConnection

from gobeventproducer import gob_model


def _start_workflow(original_msg: dict[str, Any], catalogue: str, collection: str) -> None:
    new_msg = {
        **original_msg,
        "header": {
            **original_msg.get("header", {}),
            "catalogue": catalogue,
            "collection": collection,
            "split_from": original_msg["header"].get("jobid"),
        },
        "workflow": {
            "workflow_name": "event_produce",
        },
    }
    with MessageBrokerConnection(CONNECTION_PARAMS) as connection:
        connection.publish(WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY, new_msg)


def trigger_event_produce_for_all_collections(msg: dict[str, Any], catalogue: str) -> int:
    """Start event produce workflow for all collections in catalogue."""
    collection_names = gob_model[catalogue]["collections"].keys()

    catalogue_abbreviation = gob_model[catalogue]["abbreviation"].lower()
    rel_collection_names = [c for c in gob_model["rel"]["collections"] if c.startswith(catalogue_abbreviation)]
    cat_col_combinations = [(catalogue, c) for c in collection_names] + [("rel", c) for c in rel_collection_names]

    for cat, col in cat_col_combinations:
        _start_workflow(msg, cat, col)
    return len(cat_col_combinations)
