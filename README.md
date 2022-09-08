# GOB-KafkaProducer

Reads GOB events from RabbitMQ event exchange and writes to events to a Kafka server.

Note: Only one instance of this service should be run at any given time, as running multiple instances in parallel does
not guarantee ordered delivery of events. Support for parallelisation should be implemented in the future.

# Environment variables
Example environment variables are set in `.env.example`. Create your own `.env` based on this example file:

```bash
cp .env.example .env
```
To initialise the configuration:

```bash
export $(cat .env | xargs)
```

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Docker

## Requirements

* docker compose >= 1.25
* Docker CE >= 18.09

## Run

```bash
docker compose build
docker compose up &
```

## Tests

```bash
docker compose -f src/.jenkins/test/docker-compose.yml build
docker compose -f src/.jenkins/test/docker-compose.yml run --rm test
```

# Local

## Requirements

* Python >= 3.9

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment:

```bash
source venv/bin/activate
```

# Run

Start the service:

```bash
cd src
python -m gobkafkaproducer
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```
