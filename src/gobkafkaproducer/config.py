import os

KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

KAFKA_CONNECTION_CONFIG = {
    'bootstrap_servers': KAFKA_SERVER,
    'sasl_mechanism': 'PLAIN',
    'security_protocol': 'SASL_SSL',
    'sasl_plain_username': KAFKA_USERNAME,
    'sasl_plain_password': KAFKA_PASSWORD,
}

DATABASE_CONFIG = {
    'drivername': 'postgres',
    'username': os.getenv("GOB_KAFKAPRODUCER_DATABASE_USER", "gob_kafka"),
    'password': os.getenv("GOB_KAFKAPRODUCER_DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("GOB_KAFKAPRODUCER_DATABASE_HOST_OVERRIDE", "localhost"),
    'port': os.getenv("GOB_KAFKAPRODUCER_DATABASE_PORT_OVERRIDE", 5410),
    'database': os.getenv("GOB_KAFKAPRODUCER_DATABASE", 'gob_kafka'),
}

GOB_DATABASE_CONFIG = {
    'drivername': 'postgres',
    'username': os.getenv("GOB_DATABASE_USER", "gob"),
    'database': os.getenv("GOB_DATABASE_NAME", "gob"),
    'password': os.getenv("GOB_DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("GOB_DATABASE_HOST_OVERRIDE", "localhost"),
    'port': os.getenv("GOB_DATABASE_PORT_OVERRIDE", 5406),
}
