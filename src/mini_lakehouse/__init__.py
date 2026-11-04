"""Mini-Lakehouse: an event-driven lakehouse platform (Kafka -> Iceberg).

The package is organised as ports and adapters (hexagonal architecture):

* ``domain``  - pure business logic, no I/O. Tested without containers.
* ``ports``   - typed interfaces the domain depends on (EventSource, TableWriter, Catalog).
* ``adapters``- concrete implementations (Kafka/Redpanda, Iceberg, MinIO, Postgres).
* ``gateway`` / ``ingestor`` / ``maintenance`` / ``platform_api`` - the deployable services.
"""

__version__ = "0.1.0"
