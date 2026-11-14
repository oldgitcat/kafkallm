# Setup and configuration

[Back to the main README](../README.md)

## Quick start

### Prerequisites

- Linux with KVM (`/dev/kvm`) and virtualization enabled in the BIOS
- Rootless Podman with a Compose provider (`podman compose`) and the `krun`
  runtime (`crun-krun`)
- Python 3.12+ and `make` for the host-side scripts
- An API key or reachable endpoint for your LLM provider (OpenRouter, OpenAI,
  Ollama, or a custom OpenAI-compatible service)
- Network access while building the job images
- About 4 GiB of disk for the default image, more for the optional reverse image

goosetown relies on goose subagents and tool calling. Use a model with reliable
tool calling; small free models may ignore the delegation workflow and answer
directly.

### 1. Configure

```bash
make setup
```

This creates `.env` from `.env.example`, generates
`LLM_GATEWAY_VIRTUAL_KEY`, asks for the GUI username and password, and renders
`infra/agentgateway/config.yml`.
When a terminal is attached, `make setup` asks which provider to use
(OpenRouter, Ollama, or a custom OpenAI-compatible endpoint), then prompts for
the key/base URL and model. In a non-interactive shell it validates the
existing values instead. You can also edit `LLM_UPSTREAM_PROVIDER`,
`LLM_UPSTREAM_API_KEY`, `LLM_UPSTREAM_BASE_URL`, and `GOOSE_MODEL` directly in
`.env`. Other provider presets remain available for manual configuration.
Then check the host:

```bash
make doctor
```

### 2. Prepare the host

```bash
make network   # once per host: create the isolated IPv6-only Podman network
make images    # build the goose job image into rootless Podman storage
```

The host directories under `KAFKALLM_JOB_ROOT` are created by
`scripts/prepare-job-root.sh`, which `make setup` and `make up` call
automatically. The default is `~/.local/share/kafkallm`, owned by your user;
no `sudo` is needed.

If `make doctor` reports a missing Podman socket, enable the rootless API:

```bash
systemctl --user enable --now podman.socket
```

For the optional reverse-engineering sandbox (REA + Ghidra), also run:

```bash
make rea-image
```

### 3. Start the stack

```bash
make up
```

Open:

- GUI: `http://<host-ip>:8090`
- MLflow traces: <http://localhost:5000>
- Kafka UI: <http://localhost:8080>

Once `.env` is valid and the prerequisites are installed, `make bootstrap` runs
setup, network creation, the default image build, and `make up` in one command.

The GUI is published through Caddy on `GUI_BIND` (default `0.0.0.0`, so it is
reachable from other machines in the network) and protected by HTTP basic auth.
`make setup` asks for the GUI username and password. Set `GUI_BIND=127.0.0.1`
for local-only access. Change the credentials later with
`./scripts/gui-auth.sh --force` or `make gui-auth-force`; after either, restart
Caddy with `podman compose up -d caddy`. Caddy submits to the GUI, which writes
to `jobs.in` and reads the state and audit topics. The GUI has no local database.

<details>
<summary>Manual setup without make</summary>

The Make targets wrap the scripts in this repository:

```bash
./scripts/setup.sh             # creates .env, generates the gateway key
./scripts/prepare-job-root.sh  # creates the host job directories
./scripts/init-job-network.sh
./scripts/build-goose-image.sh
./scripts/build-rea-image.sh   # optional
./scripts/build-proxy-image.sh
podman build -t kafkallm-app:latest .
podman build -f tools/gui/Dockerfile -t kafkallm-gui:latest .
podman compose --profile storage up -d
./scripts/doctor.sh
```

`make help` lists every target.
</details>

### Command line

The CLI runs on the host. `scripts/kafkallm-cli` creates/uses the local
virtualenv, loads `.env`, and talks to Kafka on `127.0.0.1:19092`. No extra
Compose container is needed.

```bash
# Submit a job
./scripts/kafkallm-cli submit "Say hello"

# Submit a job with a local file or binary
./scripts/kafkallm-cli submit \
  "Inspect the uploaded binary" --upload "$PWD/tool.bin"

# Submit into the reverse-engineering sandbox (REA + Ghidra)
./scripts/kafkallm-cli submit \
  "Reverse engineer this binary and summarize its behavior" \
  --sandbox reverse --upload "$PWD/tool.bin"

# Pull and run an image that is not in the sandbox registry
./scripts/kafkallm-cli submit "Say hello" \
  --sandbox adhoc --image ghcr.io/example/agent:1.2.3

# Submit without the learning pass (skill improvement)
./scripts/kafkallm-cli submit "Say hello" --no-reflect

# Submit a complete Job JSON payload from a file or stdin
./scripts/kafkallm-cli submit --job-json - < job.json

# Print the exact Job JSON schema (job, agent, or sandbox)
./scripts/kafkallm-cli schema job

# Read results, materialized state, audit events, or live activity
./scripts/kafkallm-cli results --from-beginning --idle-timeout 5
./scripts/kafkallm-cli states --from-beginning --idle-timeout 5
./scripts/kafkallm-cli events --from-beginning --idle-timeout 5
./scripts/kafkallm-cli logs --from-beginning --idle-timeout 5
```

Kafka is also published directly on `127.0.0.1:19092`, so any Kafka client can
produce a `Job` JSON record to `jobs.in`:

```bash
key=$(uuidgen)
printf '%s\n' \
  '{"instruction":"Write /workspace/outputs/hello.md","sandbox":"default"}' \
  | kcat -P -b 127.0.0.1:19092 -t jobs.in -k "$key"
```

Worker and GUI logs:

```bash
podman compose logs -f agent-worker
podman compose logs -f gui
podman compose logs -f caddy
podman compose logs -f agentgateway
podman compose logs -f mlflow
```

The Kafka UI runs at <http://localhost:8080>. MLflow runs at
<http://localhost:5000>.


## Configuration

Runtime defaults live in `src/kafkallm/core/settings.py`. `.env`, created from
`.env.example`, only overrides them. The file is gitignored. `make setup`
generates the virtual gateway key, validates the selected provider, and renders
the agentgateway config. For hosted providers, the provider key is the only
required secret.

| Setting | Default | Purpose |
|---|---|---|
| `GOOSE_MODEL` | `nvidia/nemotron-3.5-lightning:free` | Model used inside the job; must exist at the selected provider |
| `LLM_UPSTREAM_PROVIDER` | `openrouter` | Upstream provider (`openrouter`, `openai`, `ollama`, `custom`, provider presets) |
| `LLM_UPSTREAM_API_KEY` | — | Provider credential; only agentgateway sees it. `OPENROUTER_API_KEY` is accepted as a legacy alias for OpenRouter |
| `LLM_UPSTREAM_BASE_URL` | provider default | Endpoint override; required for `custom`, defaults to host Ollama for `ollama` |
| `LLM_UPSTREAM_FORMAT` | `completions` | Wire format for `custom` (see the module's `PROVIDER_FORMATS`) |
| `LLM_GATEWAY_VIRTUAL_KEY` | generated | Rotatable key used by jobs |
| `AGENTGATEWAY_INLINE_VIRTUAL_KEY` | `true` | Write the local virtual key into the gitignored config so the playground's saved key works; `false` keeps only the environment reference |
| `KAFKALLM_JOB_ROOT` | `~/.local/share/kafkallm` | Host directory for workspaces, skills, tools, and Goose config |
| `PODMAN_SOCKET` | `/run/user/$UID/podman/podman.sock` | Rootless Podman API socket mounted into the worker |
| `GOOSE_IMAGE` | `kafkallm-goose-job:latest` | Default job microVM image |
| `GOOSE_RE_IMAGE` | `kafkallm-rea-job:latest` | Optional reverse-engineering image |
| `GOOSE_JOB_PROXY_IMAGE` | `kafkallm-gateway-proxy:latest` | Distroless proxy image started next to each job |
| `GOOSE_JOB_RAM_MIB` / `GOOSE_JOB_CPUS` | `2048` / `2` | Per-job resources |
| `GOOSE_JOB_REFLECT_ENABLED` | `true` | Default for the per-job learning pass |
| `GOOSE_JOB_AGENTS_DIR` | `~/.local/share/kafkallm/agents` | Markdown agent profiles, built-in and custom (GUI reads/writes them) |
| `GOOSE_JOB_CONFIG_DIR` | `~/.local/share/kafkallm/config` | Goose config copied into each job (active file: `config.yaml`) |
| `GUI_BIND` | `0.0.0.0` | Host interface for the Caddy GUI endpoint; `127.0.0.1` for local-only access |
| `GUI_PORT` | `8090` | Published Caddy/GUI port |
| `GUI_AUTH_USER` / `GUI_AUTH_HASH` | generated by `make setup` | HTTP basic auth credentials enforced by Caddy |
| `GUI_MAX_REQUEST_BYTES` | `2097152` | Limit for JSON request bodies (uploads use `UPLOAD_MAX_*`) |
| `LLM_GATEWAY_PORT` | `8091` | LLM listener port inside the container and published on the host; the playground uses it too |
| `AGENTGATEWAY_UI_PORT` | `15000` | agentgateway admin UI / LLM playground (localhost only) |
| `AGENTGATEWAY_CORS_ORIGINS` | local playground origins | Browser origins allowed to call the LLM listener; empty disables CORS |
| `S3_ENDPOINT_URL` | `http://rustfs:9000` (storage profile) | Uploads and artifacts |
| `MLFLOW_ENABLED` | `true` | OTLP traces to MLflow |

See `.env.example` for the common settings and
`infra/agentgateway/config.template.yml` for the gateway policy template. The active
file is `infra/agentgateway/config.yml` (gitignored), rendered by
`make gateway-config` or automatically by `make up`.

## Reproducible builds

- `constraints.txt` pins the transitive Python dependencies of the app image.
  Regenerate it intentionally with `make lock` (uses the current app image).
- The app and goose build stages use digest-pinned base images, and the Compose
  services are pinned by digest in `compose.yaml`. Update the digests
  intentionally when you upgrade.
- The Goose release tarball is pinned by version and SHA-256 in
  `stall/standard/Dockerfile`. The REA image downloads are pinned the same way.
- `make images` builds the Goose job image. `make rea-image` builds the optional
  REA image; its Frida CLI dependencies are pinned in
  `stall/reversing/requirements.txt`.


