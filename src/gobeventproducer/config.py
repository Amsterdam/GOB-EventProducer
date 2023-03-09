import os

DATABASE_CONFIG = {
    "drivername": "postgresql",
    "username": os.getenv("GOB_EVENTPRODUCER_DATABASE_USER", "gob_eventproducer"),
    "password": os.getenv("GOB_EVENTPRODUCER_DATABASE_PASSWORD", "insecure"),
    "host": os.getenv("GOB_EVENTPRODUCER_DATABASE_HOST", "localhost"),
    "port": os.getenv("GOB_EVENTPRODUCER_DATABASE_PORT", 5430),
    "database": os.getenv("GOB_EVENTPRODUCER_DATABASE", "gob_eventproducer"),
}

GOB_DATABASE_CONFIG = {
    "drivername": "postgresql",
    "username": os.getenv("GOB_DATABASE_USER", "gob"),
    "database": os.getenv("GOB_DATABASE_NAME", "gob"),
    "password": os.getenv("GOB_DATABASE_PASSWORD", "insecure"),
    "host": os.getenv("GOB_DATABASE_HOST_OVERRIDE", "localhost"),
    "port": os.getenv("GOB_DATABASE_PORT_OVERRIDE", 5406),
}

LISTEN_TO_CATALOGS = os.getenv("LISTEN_TO_CATALOGS", "").split(",")
