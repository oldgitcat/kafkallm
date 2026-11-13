# Kafka

The control plane uses Apache Kafka 4.4 in KRaft mode. The service is defined
in the top-level `compose.yaml`.

- Internal listener: `kafka:9092`
- Host listener: `127.0.0.1:19092`
- Controller listener: `kafka:9093`
- Data volume: `kafka-data` at `/var/lib/kafka/data`
- Automatic topic creation: disabled

Application topics:

| Topic | Purpose |
|---|---|
| `jobs.in` | Submitted jobs |
| `jobs.out` | Results |
| `jobs.state` | Compacted GUI job state |
| `agents.state` | Compacted agent registry |
| `sandboxes.state` | Compacted sandbox registry |
| `agent.events` | Agent activity |
| `jobs.cancel` | Cancellation requests |
| `jobs.logs` | Job log lines |

Relevant settings are documented in `docs/setup.md` and `compose.yaml`.
