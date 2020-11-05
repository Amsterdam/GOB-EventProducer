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
