# kafkallm

Eine kleine, containerisierte LLM-Jobverarbeitung mit **Redpanda für Jobs, Ergebnisse und Audit-Events**. Es gibt bewusst kein PostgreSQL und keinen Scheduler. Arize Phoenix dient optional als grafische Trace-Oberfläche, nicht als Job-Store.

```text
Producer/CLI → Redpanda jobs.in → isolierter Agent-Worker
                                      ├─→ Redpanda jobs.out
                                      ├─→ Redpanda agent.events
                                      ├─→ OpenTelemetry → Phoenix
                                      └─→ optional RustFS für Dateien
```

## Enthalten

- Redpanda mit den Topics `jobs.in`, `jobs.out` und `agent.events`
- Python-Worker mit manueller Kafka-Offset-Bestätigung
- Verknüpfte Traces für Submit, Worker, Agent, LLM und Tool-Aufrufe
- Lokale Arize-Phoenix-GUI mit persistentem SQLite-Volume
- Agent-Harness mit Timeout, Schrittlimit und separatem Workspace pro Run
- Tools zum Lesen, Schreiben und Auflisten von Workspace-Dateien
- OpenAI-kompatibler LLM-Anschluss inklusive Tool-Calling
- Echo-Provider für Tests ohne LLM und API-Key
- Optionaler S3-kompatibler RustFS-Container für Ein- und Ausgabedateien
- Redpanda Console und eine CLI zum Absenden und Lesen von Jobs

## Schnellstart

Voraussetzungen: Docker und Docker Compose.

```bash
cp .env.example .env
docker compose up -d --build
```

Der Standard-Worker verwendet `LLM_PROVIDER=echo`. Dadurch lässt sich die gesamte Verarbeitung ohne externen LLM-Dienst testen.

Einen Job absenden:

```bash
docker compose run --rm jobctl submit "Sage Hallo"
```

Das Ergebnis lesen:

```bash
docker compose run --rm jobctl results --from-beginning --idle-timeout 3
```

Worker-Logs ansehen:

```bash
docker compose logs -f agent-worker
```

Die Redpanda Console läuft unter <http://localhost:8080>. Dort lassen sich Topics und einzelne Nachrichten ansehen. Die Phoenix-GUI läuft unter <http://localhost:6006>.

## Nachverfolgbarkeit und Phoenix

Jeder Job erhält eine `trace_id`. Sie wird über Kafka-Header weitergereicht und verbindet das Absenden, den Worker, den Agenten sowie alle LLM- und Tool-Aufrufe. Zusätzlich schreibt der Worker strukturierte Audit-Events nach `agent.events`:

- `job.received` und `job.completed`
- `agent.completed`
- `llm.completed`
- `tool.completed`
- `artifact.stored`

Die Events enthalten Dauer, Status, Fehler, Tokenverbrauch und Beziehungen über `parent_event_id`. Audit-Nachrichten kannst du auch ohne Phoenix lesen:

```bash
docker compose run --rm jobctl events --from-beginning --idle-timeout 3
```

Phoenix zeigt dieselbe verteilte Ausführung als Span-Baum. Prompts, LLM-Antworten und Tool-Ein-/Ausgaben werden standardmäßig **nicht** erfasst. Wenn diese Inhalte lokal in Phoenix erscheinen dürfen, setze in `.env`:

```dotenv
PHOENIX_CAPTURE_CONTENT=true
```

Anschließend Worker und CLI-Images neu starten beziehungsweise ausführen. Die Audit-Events enthalten auch mit dieser Einstellung nur Metadaten; Inhalte werden ausschließlich an Phoenix gesendet. Der Phoenix-Container speichert seine Daten in einem persistenten Volume. Der Jobablauf hängt nicht von Phoenix ab.

`PHOENIX_ENABLED=false` deaktiviert den OTLP-Export aus Worker und CLI. Mit `PHOENIX_UI_PORT` und `OTLP_GRPC_PORT` lassen sich belegte Host-Ports ändern. Phoenix läuft im Compose-Setup ohne Authentifizierung und ist deshalb ausschließlich an `127.0.0.1` gebunden; für einen Netzwerk- oder Produktivbetrieb müssen Authentifizierung und TLS separat konfiguriert werden.

## Echtes LLM verwenden

In `.env` beispielsweise folgende Werte setzen:

```dotenv
LLM_PROVIDER=openai
LLM_MODEL=dein-modell
LLM_API_KEY=dein-api-key
# Optional für Ollama, vLLM oder einen anderen OpenAI-kompatiblen Endpunkt:
LLM_BASE_URL=
```

Danach den Worker neu starten:

```bash
docker compose up -d --build --force-recreate agent-worker
```

Bei einem lokalen OpenAI-kompatiblen Endpunkt ohne Authentifizierung kann `LLM_API_KEY` leer bleiben. Der Endpunkt muss aus dem Worker-Container erreichbar sein.

Der Agent kann diese Tools verwenden:

| Tool | Zweck |
|---|---|
| `read_workspace_file` | UTF-8-Datei aus dem Workspace lesen |
| `write_workspace_file` | Textdatei schreiben, Ergebnisse unter `outputs/` |
| `list_workspace_files` | Vorhandene Workspace-Dateien auflisten |

Es gibt absichtlich kein Shell-Tool und keinen Host-Zugriff.

## RustFS und Dateien aktivieren

```bash
docker compose --profile storage up -d --build
```

- S3-API: <http://localhost:9000>
- RustFS Console: <http://localhost:9001/rustfs/console/>
- Lokaler Standard-Account: `rustfs`
- Lokales Standard-Secret: `kafkallm-secret`

Diese Zugangsdaten sind nur für die lokale Entwicklung gedacht und sollten in `.env` geändert werden. Falls die Ports bereits belegt sind, können `RUSTFS_API_PORT` und `RUSTFS_CONSOLE_PORT` in `.env` angepasst werden.

Dafür muss in `.env` ein S3-Endpunkt gesetzt sein, wie im Beispiel:

```dotenv
S3_ENDPOINT_URL=http://rustfs:9000
```

Eingabedateien werden als S3-Referenz übergeben:

```bash
docker compose run --rm jobctl submit \
  "Lies die Eingabedatei und fasse sie zusammen" \
  --input-file jobs/example.txt
```

Der Bucket muss die Datei bereits enthalten. Der Worker lädt sie nach `inputs/`. Vom Agenten unter `outputs/` erzeugte Dateien lädt der Harness automatisch nach RustFS hoch. Das Ergebnis in `jobs.out` enthält anschließend Bucket und Key.

Ein fehlender oder nicht erreichbarer Artefakt-Speicher macht einen ansonsten erfolgreichen Agentenlauf **nicht** mehr zu einem fehlgeschlagenen Job. Der Job bleibt `succeeded`, die Dateien fehlen jedoch und `artifact_error` beschreibt den Upload-Fehler. Uploads werden dreimal versucht und bereits teilweise hochgeladene Dateien bei einem endgültigen Fehler bereinigt.

## Jobformat

```json
{
  "job_id": "optional-vom-producer-gesetzt",
  "run_id": "optional-vom-producer-gesetzt",
  "trace_id": "wird-automatisch-ueber-openTelemetry-erzeugt",
  "type": "llm_task",
  "instruction": "Analysiere die Eingabedaten",
  "input": {" beliebig": "JSON-Daten" },
  "input_files": [],
  "timeout_seconds": 300,
  "max_steps": 6
}
```

`job_id` und `run_id` werden automatisch als UUIDs erzeugt. Nachrichten besitzen die `job_id` als Kafka-Key.

Ein Ergebnis enthält unter anderem:

- `status`: `succeeded` oder `failed`
- `result` oder `error`
- `artifacts`: hochgeladene Dateien
- `artifact_error`: optionaler Fehler beim Speichern erzeugter Dateien; ändert den Jobstatus nicht
- `trace_id`, `worker_id`, Zeitstempel, Schritte und Tokenverbrauch

## Mehrere Worker

```bash
docker compose up -d --scale agent-worker=2
```

Die Worker verwenden gemeinsam die Consumer Group `agent-workers`. Jeder Worker verarbeitet jeweils einen Job. Die Topics werden standardmäßig mit drei Partitionen angelegt.

## Betrieb und Garantien

- Der Worker wartet beim Start bis zu `KAFKA_STARTUP_TIMEOUT_SECONDS` (Standard: 300 Sekunden) mit exponentiellem Backoff auf Redpanda, statt sofort in eine Crash-Schleife zu laufen.
- Redpanda, Console und RustFS besitzen Restart-Policies. Nach einem Rechner- oder Docker-Neustart startet der Stack dadurch selbstständig wieder.
- Der Kafka-Offset wird erst bestätigt, nachdem das Ergebnis erfolgreich nach `jobs.out` geschrieben wurde.
- Die Verarbeitung ist **at-least-once**. Nach einem Absturz kann ein Job erneut ausgeführt werden.
- Audit-Events werden asynchron eingereiht. Ein Audit- oder Phoenix-Ausfall verändert weder Jobstatus noch Ergebnis.
- Topics haben standardmäßig sieben Tage Retention, konfigurierbar über `KAFKA_TOPIC_RETENTION_MS`. Für längere Audits muss dieser Wert erhöht werden.
- Phoenix ist nur ein optionaler Trace-Viewer; fehlgeschlagene OTLP-Exporte verändern kein Job-Ergebnis.
- Der Worker läuft als Non-Root, mit Read-only-Root-Filesystem, ohne Linux-Capabilities und ohne Host-Mounts.
- Das Compose-Setup ist für Entwicklung und Tests gedacht. Redpanda besitzt hier keine SASL/TLS-Konfiguration.

Stoppen ohne Daten-Volumes zu löschen:

```bash
docker compose down
```

## Lokale Tests

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

Die Unit-Tests benötigen weder Docker noch Redpanda.
