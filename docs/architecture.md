# Architecture and job lifecycle

[Back to the main README](../README.md)

## How a job runs

See [Container and process map](services.md) for the services, networks, ports,
and worker-created containers.

1. The GUI or CLI writes a JSON job to `jobs.in` and a `queued` state to
   `jobs.state`.
2. The worker consumes `jobs.in`, publishes `running`, and prepares a unique
   directory under `GOOSE_JOB_WORKSPACE_ROOT`.
3. Files uploaded through the GUI or attached with `submit --upload` are
   downloaded from RustFS into `inputs/`. The job sees them under
   `/workspace/inputs`.
4. The worker creates a per-job allowlisting proxy on the IPv6-only
   `kafkallm-jobs` network, carrying the job's W3C `traceparent`, then starts a
   goose microVM through the rootless Podman API. The job is attached only to
   that internal network, and `host.containers.internal` is overridden to the
   per-job proxy's IPv6 address. Everything the job needs is copied into the
   container's writable root filesystem before it starts: the job directory to
   `/workspace`, the prompt and scripts to `/jobboot`, and the persistent skill
   store to `/opt/kafkallm/skills-persistent`. No host path is bind mounted
   into the microVM. Inside the guest, the entrypoint copies the store into a
   disposable `/workspace/.claude/skills` tree. The microVM receives
   `OPENAI_HOST` and the gateway virtual key, never the provider key.
5. Inside the microVM, the entrypoint seeds the workspace with goosetown
   (`AGENTS.md`, the `goose` wrapper, `gtwall`, and the role skills) and runs:

   ```bash
   stdbuf -oL -eL ./goose run --no-session -q --output-format stream-json \
     --with-builtin developer --max-turns "$GOOSE_MAX_TURNS" \
     -i /jobboot/main-prompt.txt
   ```

   In multi-agent mode the worker additionally enables the `summon` and
   `skills` builtins and writes the selected flock to `.claude/agents/*.md`,
   so `delegate(source: "...")` can resolve the role names.

   The worker tails the line-delimited stream-json file and publishes live
   activity to `jobs.logs` for the GUI.

6. goose talks the OpenAI protocol to `http://host.containers.internal:8091`,
   which resolves to the proxy. The proxy accepts only
   `POST /v1/chat/completions`, `GET /v1/models`, and
   `GET /v1/models/<id>`, allows one request per connection, and forwards to
   agentgateway on the host; the job cannot reach the internet, the LAN, Compose
   services, or the Podman API. agentgateway verifies the virtual key, replaces
   it with the configured provider key, and streams the upstream response back.
7. The goosetown orchestrator decomposes the job, dispatches parallel subagents
   through goose's `delegate`/`load` tools, coordinates them over `gtwall`, and
   writes result files to `outputs/`.
8. The worker parses the structured goose output, copies `outputs/` back
   (skipping symlinks and special files), runs the optional learning pass in a
   separate microVM, validates and merges any skill changes, publishes `jobs.out`
   and the final `jobs.state`, uploads artifacts, and commits the Kafka offset.
9. Audit events go to `agent.events`; traces go to MLflow.

The microVM never talks to Kafka. The worker is the only Kafka participant.


## Kafka topics

| Topic | Cleanup | Purpose |
|---|---|---|
| `jobs.in` | delete, 7 days | Jobs submitted by the GUI or CLI |
| `jobs.out` | delete, 7 days | Final `JobResult` records |
| `jobs.state` | **compact** | Latest materialized `JobState` per `job_id`; the GUI's database |
| `agents.state` | **compact** | Latest goosetown `AgentSpec` per `agent_id` |
| `sandboxes.state` | **compact** | Latest `SandboxSpec` per `sandbox_id`; the environment registry |
| `jobs.cancel` | delete, 7 days | Cancellation requests published by the GUI |
| `jobs.logs` | delete, 7 days | Live job activity from goose stream-json and stderr |
| `agent.events` | delete, 7 days | Audit events for each job |

The GUI consumes `jobs.state`, `agents.state`, `sandboxes.state`, `jobs.logs`,
and `agent.events` from the beginning with unique consumer groups. Compaction keeps
the latest state per key, so a restart rebuilds the view without a local store.
State transitions are published by both the GUI/CLI (`queued`) and the worker
(`running`, `succeeded`/`failed`).


## Jobs

See [Jobs and manual Kafka submission](jobs.md) for the Job schema, the
Kafka message envelope, and manual producer examples.

## MicroVM job image

The job image is built with `stall/standard/Dockerfile`:

- Debian bookworm slim, with bash, git, jq, Python 3, ripgrep, binutils, file,
  less, unzip/zip, and xxd
- Pinned goose `1.59.0` release, verified with SHA-256 at build time
- No `curl`, `wget`, or other network clients in the final image
- goosetown template and skills under `/opt/goosetown`
- Tools and goose config are copied in by the worker at job start; the image
  does not mount them
- Optional extra Debian packages through `GOOSE_JOB_EXTRA_PACKAGES`

The sandbox images form a chain: `stall/standard/Dockerfile` builds
`kafkallm-goose-job:latest` from Debian, goose, common tools, and the
goosetown template and skills; `stall/reversing/Dockerfile` adds the
reverse-engineering tooling. The proxy is a separate distroless Python image
built from `infra/proxy/Dockerfile` and started as a normal container next to
each job.

Build it with `./scripts/build-goose-image.sh`. The worker expects it in the
rootless Podman storage and verifies it at startup (it can pull a registry
image if `GOOSE_IMAGE` points to one).

### Reverse-engineering sandbox

`sandbox=reverse` selects a second image, built from
`stall/reversing/Dockerfile`, instead of the default image. It adds:

- **REA** `6.8.0` as a local stdio MCP server (`rea mcp`), wired into goose as
  the `rea` extension. The `rea` CLI is also on `PATH`. The MCP input schema
  profile is `compact`; output schemas are set to `off` to keep the tool list
  small (`full` is the other valid output profile).
- **Ghidra** `12.1.4` with **Temurin JDK 21.0.13+9**, the combination verified
  by REA.
- **Kuna** `1.833`, pinned to the release REA verifies, as an alternative
  decompiler provider (`REA_KUNA_COMMAND`). The static musl build avoids glibc
  compatibility warnings on Debian bookworm. Kuna's bundled skill is seeded
  into reverse jobs.
- **JADX** via `jadx-headless-mcp` `0.7.1` (`REA_JADX_MCP_JAR`) for Android
  APK/DEX analysis, plus `apktool` and `apksigner` for packaging and signing.
- **Frida** `17.26.1` with `frida-tools` for local dynamic instrumentation.
  REA's bundled Frida bindings expose the full session/script tool set; the
  standalone `frida`, `frida-ps`, and `frida-trace` CLIs are also on `PATH`.
- Static RE tools: GDB, binutils, binwalk, YARA, strace, ltrace, nasm,
  patchelf, 7-Zip, and Python libraries for Capstone, pyelftools, pefile, and
  pwntools. REA can also use the bundled objdump and GDB.

The reverse image starts from `kafkallm-goose-job:latest`, so build the normal
image first. `scripts/build-rea-image.sh` does that automatically if the base
image is missing. Both images are pinned and verified with SHA-256 at build
time. The reverse image is roughly 4.1 GiB on disk. Verify the REA integration
offline with:

```bash
podman run --rm --network none localhost/kafkallm-rea-job:latest \
  rea doctor --json
```

REA's `doctor` still reports Hopper and IDA as missing because those are
proprietary engines that cannot be redistributed. All open-source providers
shipped in the image are available.

Use it from the GUI (sandbox selector) or the CLI:

```bash
./scripts/kafkallm-cli submit \
  "Analyze the uploaded binary under /workspace/inputs with REA and explain its network behavior" \
  --sandbox reverse --upload "$PWD/tool.bin"
```

The job image is selected from the GUI-managed sandbox registry, not supplied
in the job payload. Job submissions carry only a sandbox ID, and the worker
resolves that ID to the image stored in the registry. The worker copies only
`/workspace`, `/jobboot`, the tools/config directories, and the persistent
skills into the container; nothing from the host is mounted. The microVM still
has no internet access; REA and Ghidra run fully
offline against uploaded files. Ghidra is JVM-heavy, so set
`GOOSE_JOB_RAM_MIB=4096` (or more) and raise `GOOSE_JOB_MEMORY_LIMIT` to leave
room for the hypervisor overhead (for example `6g`) when using reverse jobs.
The per-job reservation is the same for all sandboxes, so a larger value
applies to all jobs on that worker.

REA is MIT-licensed. Ghidra and Kuna are Apache 2.0. Frida is LGPL-2.1.
`jadx-headless-mcp` and `apktool`/`apksigner` are Apache 2.0. The image also
installs the Debian packages above under their respective licenses. Frida is
included for local instrumentation; a remote Frida server for physical Android
devices is not bundled.

Per-job layout. The main job VM:

| Path | Source | Purpose |
|---|---|---|
| `/workspace` | copy of `goose-vm-<run_id>` | `inputs/`, `outputs/`, `.scratch/`, the goosetown workspace, and a disposable skill copy; copied back after the run and removed |
| `/jobboot` | copy of the job bundle | main run script and prompt |
| `/opt/kafkallm/skills-persistent` | copy of `GOOSE_JOB_SKILLS_DIR` | shared goosetown skills; changes stay inside the disposable VM |
| `/opt/kafkallm/tools`, `/opt/kafkallm/config` | copies of the host directories | tools and Goose config |
| `/opt/data` | container root filesystem | goose config, sessions, and wall state; discarded with the microVM |

The learning pass runs after the main VM exits, in its own microVM with a fresh
random workspace:

| Path | Source | Purpose |
|---|---|---|
| `/workspace` | copy of `<job-workdir>/.reflect-<random>` | disposable skill copy and reflect control files |
| `/workspace/outputs` | copy of the collected outputs | review material |
| `/jobboot` | copy of the reflect bundle | reflect run script and prompt |
| `/opt/data` | container root filesystem | goose config and sessions; discarded with the microVM |

The worker reads the result and skill changes back through the Podman archive
API. The microVM never has a writable path to the host filesystem, and its
container root filesystem is deleted with the microVM.

The microVM costs more than a container: each job reserves `GOOSE_JOB_RAM_MIB`
of memory and boots a guest kernel. With the learning pass enabled, the two
microVMs run sequentially, not at the same time.


## Editing the sandbox

The job image is intentionally small, but you have four extension points. None
of them weakens the network isolation: the job still reaches only the
allowlisting proxy.

### 1. Host-provided tools

Put static binaries or extracted tools under `GOOSE_JOB_TOOLS_DIR`
(`~/.local/share/kafkallm/tools` by default). The worker copies this directory
into the microVM at `/opt/kafkallm/tools` and prepends
`/opt/kafkallm/tools/bin` to `PATH`.

```bash
mkdir -p ~/.local/share/kafkallm/tools/bin
cp my-analyzer ~/.local/share/kafkallm/tools/bin/
```

### 2. Goose config and MCP servers

Put a `config.yaml` in `GOOSE_JOB_CONFIG_DIR`
(`~/.local/share/kafkallm/config/config.yaml` by default). The worker
copies the directory into the microVM at `/opt/kafkallm/config`, and the
entrypoint copies it into Goose's config directory before each run. See
`gaense/config/config.yaml.example` for an example.

MCP servers must already exist in the image or in the tools directory. Jobs
have no web access, so remote MCP servers and `npx` downloads do not work.

### 3. Skills

Skills live in `GOOSE_JOB_SKILLS_DIR` (`~/.local/share/kafkallm/skills`). Add a
directory with a regular `SKILL.md`; later jobs receive a copy. A job's edits to
its disposable `.claude/skills` tree do not persist automatically. The isolated
learning pass may propose changes, and the worker merges only validated,
non-deleting updates.

### 4. Build a custom image

Install extra Debian packages into the normal image:

```bash
GOOSE_JOB_EXTRA_PACKAGES="openjdk-17-jre-headless" \
  ./scripts/build-goose-image.sh
```

For larger changes, create a Dockerfile that extends either job image and pass
it explicitly:

```bash
GOOSE_JOB_BASE_IMAGE=kafkallm-rea-job:latest \
GOOSE_IMAGE=kafkallm-custom-job:latest \
GOOSE_JOB_DOCKERFILE=/path/to/your/Dockerfile \
  ./scripts/build-goose-image.sh
```

If you extend the reverse image, it already contains REA, Ghidra, and JDK 21;
add only the extra tools you need. The standard reverse sandbox itself is
`stall/reversing/Dockerfile`.

To inspect the exact sandbox interactively, without a job and without network:

```bash
./scripts/sandbox-shell.sh
```


## Why Podman

Both the control plane and the job microVMs run under **rootless Podman** by
default. `make up` builds the worker and GUI images with `podman build` and runs
the stack with `podman compose`.

- **Podman runs the microVMs.** Only Podman's `crun` krun runtime can start a
  libkrun microVM, selected through the `run.oci.handler=krun` annotation.
  Docker's runc has no krun handler.
- **The control plane also runs under rootless Podman.** The worker, GUI,
  Caddy, Apache Kafka, RustFS, MLflow, and the Kafka UI are ordinary containers.
  One engine means one image store.
- **Rootless Podman is required.** Rootful Podman can break libkrun's TSI
  networking, leaving the microVMs without an outbound path and stalling every
  job on the LLM provider. Use the rootless socket.

The control plane uses Podman Compose because some host interfaces expose
unusual netmasks (for example a `/0` netmask from a mobile hotspot) that make
netavark reject every IPv4 bridge subnet. Gänsestall therefore writes an
IPv6-only internal job network directly into rootless Podman's network store
instead of asking netavark to derive one. Other container bridges are
unaffected.

Docker is not supported for the control plane. The worker and GUI rely on
Podman's `keep-id` user namespace, and the job microVMs always use rootless
Podman.

The worker reaches the rootless Podman API through the Unix socket that Compose
mounts into it; no TCP socket proxy is involved. The worker runs with a
`keep-id` user namespace, so its UID/GID `10001` maps to the host user. Job
directories and skills live under `~/.local/share/kafkallm`, and the microVM
guest root maps to the same host user, so neither `sudo` nor world-writable
modes are needed.


## Workspace and artifacts

The worker runs as UID/GID `10001`, mapped to the host user. It stages each job
under its staging directory and copies inputs into the job directory under
`GOOSE_JOB_WORKSPACE_ROOT`. That directory is copied into the microVM at
`/workspace`; after the job, the worker reads `outputs/` back through the
archive API and deletes the job directory.

Artifacts are uploaded only from `outputs/`. A missing or unreachable artifact
store does not turn a successful run into a failed job; the job remains
`succeeded` with an `artifact_error`. Uploads are retried three times.

### Downloading results and artifacts

The job detail in the GUI has **Download result** for the text result and a
**Download** link for every uploaded artifact. Downloads are proxied through
the GUI, so the browser does not need S3 credentials.

The same files are available over HTTP:

```bash
# Result text as Markdown (Caddy asks for the GUI password)
curl -fL -u '<gui-user>' "http://<host-ip>:8090/api/jobs/<job_id>/result" -o result.md

# Artifact by index, in the same order as the artifacts list
curl -fL -u '<gui-user>' "http://<host-ip>:8090/api/jobs/<job_id>/artifacts/0" -o artifact.bin
```


## RustFS and input files

Start with the storage profile:

```bash
podman compose --profile storage up -d --build
```

```dotenv
S3_ENDPOINT_URL=http://rustfs:9000
```

Local RustFS defaults:

- S3 API: <http://localhost:9000> (or `RUSTFS_API_PORT`)
- Console: <http://localhost:9001/rustfs/console/>
- Account: `rustfs`
- Secret: `kafkallm-secret`

These credentials are for local development only.

Submit a job with an existing S3 object:

```bash
./scripts/kafkallm-cli submit \
  "Read the input file and summarize it" \
  --input-file jobs/example.txt
```

The bucket must already contain the file.

Upload local files or binaries:

- **GUI:** use **Input files or binaries** in the submit form. The browser
  uploads to `POST /api/uploads`; the files are stored under
  `uploads/<uuid>/<filename>` in RustFS and attached to the job as
  `input_files`. The job copies them into `/workspace/inputs` as
  `NN-<filename>` (for example `00-tool.bin`).
- **CLI:**

  ```bash
  ./scripts/kafkallm-cli submit \
    "Inspect the uploaded files" \
    --upload "$PWD/data.csv" \
    --upload "$PWD/tool.bin"
  ```

Uploads require `S3_ENDPOINT_URL` and the S3 credentials. Limits are
`UPLOAD_MAX_FILES` (default 16), `UPLOAD_MAX_FILE_BYTES` (default 50 MiB), and
`UPLOAD_MAX_TOTAL_BYTES` (default 100 MiB).


