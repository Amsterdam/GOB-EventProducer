# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-slim-bullseye as wheelhouse
MAINTAINER datapunt@amsterdam.nl


# Application stage.
FROM amsterdam/gob_baseimage:3.9-slim-bullseye as application
MAINTAINER datapunt@amsterdam.nl
# GOB base image: SQL Server driver.

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobeventproducer in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt
# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

# Copy gobeventproducer module.
COPY gobeventproducer gobeventproducer

# Copy Alembic config.
COPY alembic alembic
COPY alembic.ini alembic.ini

USER datapunt
CMD ["python", "-m", "gobeventproducer"]


# Test.
FROM application as test
USER root

# Remove gobcore/gobconfig tests (if present).
RUN rm -rf /app/src/gobcore/tests
RUN rm -rf /app/src/gobconfig/tests

# Copy test module and tests.
COPY test.sh pyproject.toml ./
COPY tests tests

# Copy Jenkins files.
COPY .jenkins /.jenkins

USER datapunt
