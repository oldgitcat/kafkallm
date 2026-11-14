# Agents, skills, and the learning pass

[Back to the main README](../README.md)

## goosetown and skills

The job image contains a small goosetown hub:

- `AGENTS.md` — workspace and coordination contract for every job
- `goose` — the upstream goosetown launcher (sets telepathy and wall files)
- `gtwall` — the upstream Town Wall broadcast tool
- `.claude/skills/goosetown-orchestrator/SKILL.md` — adapted orchestrator
  guidance for an unattended, offline job
- role skills for workers, writers, reviewers, and a local researcher

The upstream role skills and `gtwall` are vendored from goosetown under
Apache 2.0; see `goose-assets/LICENSE-goosetown`. The orchestrator and `AGENTS.md`
are adapted so they do not depend on the internet, `bd`, `uv`, or the goosetown
dashboard. The browser-based goosetown dashboard is not used; Gänsestall has its
own job GUI.

Skills are stored on the host at `GOOSE_JOB_SKILLS_DIR`
(`~/.local/share/gaensestall/skills` by default). The store is copied into
each main job at `/opt/gaensestall/skills-persistent`; the guest entrypoint copies
it into a disposable `/workspace/.claude/skills` tree. A job can edit that copy,
but it cannot write the shared store. Only the worker writes the store, after a
separate learning-pass microVM proposes changes and the worker has validated
them. A skill added by an operator or accepted from a learning pass is available
to later jobs.

Each skill is a directory with a regular `SKILL.md`. Symlinks, special files,
invalid names, and oversized trees are rejected by the worker. Merging only adds
or replaces files, so existing skills are never deleted.

### Agent management

The GUI has a **Goosetown agents** panel. Agent profiles are stored in the
compacted `agents.state` Kafka topic, so there is still no separate database.
On the first GUI start the standard flock is seeded:

- `Orchestrator` — decomposes the job and synthesizes the result
- `Local researcher` — read-only search of inputs, notes, and skills
- `Worker` — builds code, scripts, configs, and data
- `Writer` — synthesizes sources into one deliverable
- `Reviewer` — reviews against explicit acceptance criteria

You can add, edit, enable, or disable **custom** agents in the GUI. The five
built-in goosetown roles are read-only. Each profile has a name, role,
description, instructions, and a list of skills. Use **+ New agent** next to the
flock list or in the panel header to create one; it is selected automatically,
and its display name can be changed later with **Edit**.

The job form has **Use goosetown multi-agent flock**. When it is enabled, the
selected agents are embedded in the job as a snapshot. The worker writes them
to `/jobboot/agents.json` and, more importantly, renders each non-orchestrator
role as an agent source at `.claude/agents/<agent_id>.md`. goose's `summon`
extension discovers those files, so the orchestrator can call
`delegate(source: "<role name>", ...)` and `load(source: ...)`. The run also
enables the `skills` builtin so the role skills are discoverable. When the
flock is disabled, the prompt runs a single goose agent without `summon` or
delegation.

#### Agent profile directory

The GUI also imports agent profiles from `GOOSE_JOB_AGENTS_DIR`
(`~/.local/share/gaensestall/agents` by default) at startup, next to the skills
directory. Each `*.md` file needs YAML front matter; the filename is the
`agent_id`:

```markdown
---
name: "Security reviewer"
role: reviewer
description: "Checks security-sensitive outputs."
skills:
  - goosetown-reviewer
enabled: true
---

Review the assigned output for security issues and report file/line references.
```

The body becomes the `instructions` field. The parser accepts the flat subset
used by goose agent sources: strings (optionally quoted), booleans, and lists in
`- item` or `[a, b]` form.

The directory is the durable store for agents:

- The five built-in roles are defined as files in the repository/app image at
  `goose-assets/agents/*.md`. On GUI start they are copied into `GOOSE_JOB_AGENTS_DIR` if
  missing, so every agent is file-defined. They use the same format as custom
  profiles.
- The GUI still marks the built-in IDs as read-only and blocks disable/delete.
  To change a built-in agent, edit its seeded file in `GOOSE_JOB_AGENTS_DIR` and
  restart the GUI. Delete the runtime file and restart to restore the shipped
  default.
- **+ New agent** writes `<agent-id>.md` and publishes the profile to
  `agents.state`. **Edit** and **Disable** update both the file and Kafka;
  disabling sets `enabled: false` instead of deleting the file.
- On every GUI start the built-in profile files are seeded into the directory,
  the defaults are seeded into Kafka, custom agents that exist only in Kafka are
  exported to files, and then all files are imported. Files win over Kafka for
  their `agent_id`, including edits to built-in profiles.
- Files edited outside the GUI are picked up when you click
  **Reload from files** in the Agents panel, or on the next GUI restart.
- The directory is mounted read-write into the GUI container. Kafka remains the
  runtime registry that the GUI reads from and the worker snapshots into jobs.

### Sandbox management

The GUI has a **Sandboxes** panel and the job form's sandbox dropdown is built
from it. The registry is the compacted `sandboxes.state` topic, so there is no
separate database. On submit, the GUI writes the selected entry's image
reference into the job as `sandbox_image` (for example
`localhost/gaensestall-goose-job:latest` or `ghcr.io/org/agent:1.2.3`). The worker
runs that reference and pulls it from its registry when it is not already in
local Podman storage. The sandbox entry itself supplies the optional `env`,
`extensions`, and `instructions`. On the first GUI start two environments are
seeded:

- `Default` — `GOOSE_IMAGE`, the normal goose + goosetown image
- `Reverse engineering` — `GOOSE_RE_IMAGE`, with REA as a local MCP server,
  Ghidra, and the RE-specific toolchain

After the first start the registry is the source of truth. Changing
`GOOSE_IMAGE` or `GOOSE_RE_IMAGE` in `.env` does not rewrite existing entries;
edit the image in the GUI instead.

Use **+ New sandbox** to register another image. A sandbox entry has:

| Field | Purpose |
|---|---|
| `name` | Label shown in the dropdown and job detail |
| `image` | OCI image reference, for example `localhost/gaensestall-malware:latest` |
| `description` | Short explanation shown below the dropdown |
| `instructions` | Optional prompt note about the environment's capabilities |
| `env` | Optional non-reserved environment variables for the microVM |
| `extensions` | Optional local goose extensions in `name:/absolute/path` form |
| `enabled` | Disabled environments stay in history but leave the dropdown |

Build or pull the image into the worker's rootless Podman storage first. The
worker verifies the image at job start and gives a clear error if it is
unavailable. `GAENSESTALL_*`, `GOOSE_*`, `OPENAI_*`, and process-critical variables
such as `PATH` and `HOME` are reserved and rejected in the GUI, so registry
entries cannot override LLM proxy credentials or runtime controls.

Adding an image therefore needs no Python or Compose change:

1. Build or pull it into the same rootless Podman storage the worker uses
   (`podman build -t localhost/my-agent:latest ...` or
   `podman pull ...`, as the same user that owns `PODMAN_SOCKET`).
2. Open the GUI, choose **Sandboxes → + New sandbox**, and enter the image
   reference plus optional `env` and `extensions`.
3. Submit a small job with that environment. The worker uses the entry's image
   reference and pulls it from the registry if it is not local; watch the live
   activity panel during the first start.

The entry is stored in Kafka and appears immediately in the dropdown. Deleting
an entry disables it for new jobs; queued jobs still carry their image
reference, while `env`, `extensions`, and `instructions` are read from the
registry at run time.

The CLI accepts the same registry IDs:

```bash
./scripts/gaensestall-cli submit "Analyze the sample" --sandbox malware-analysis
```

New agent profiles follow the same pattern: **Goosetown agents → + New agent**,
choose a role and skills, then select the agent in the job form. No restart and
no code change are required.

A separate persistent goosetown container is not used: the actual multi-agent
flock runs inside each disposable job microVM. This keeps the jobs isolated and
avoids a second long-lived service that would need host workspace access.

### Learning pass

After a successful main run, the worker can run a second, shorter goose session
in a **separate microVM**. That VM starts only after the main VM has exited and
been removed. It gets a fresh random workspace, the already-collected outputs
read-only, and a disposable copy of the persistent skills. It does not get the
main job's environment, sandbox extensions, or writable workspace. This prevents
a compromised main job from forging a success or writing the tree that the
worker merges.

The learning pass may propose skill changes. The worker accepts them only when
the reflect run exited successfully, then validates and merges each skill as a
whole. Validation requires a directory with a regular, UTF-8 `SKILL.md` and only
regular files below it; symlinks, special files, oversized trees, invalid names,
and more than three skills per pass are rejected. Merging only adds or replaces
files, so a proposal can never delete existing guidance. A failed or rejected
pass is reported as `goose.reflect` or `skills.rejected` and does not fail the
job; accepted changes produce `skills.persisted`.

The job form has a **Run learning pass** checkbox. It is checked by default
from the worker setting, and the choice travels in the Job message as
`reflect`. A job can therefore enable or disable the pass regardless of the
worker default. The CLI has `--no-reflect` for the same override.

The pass needs enough remaining time: it is skipped when fewer than 60 seconds
are left at the start, and it reserves at most 25 % of the remaining budget
(capped by `GOOSE_JOB_REFLECT_TIMEOUT_SECONDS`). The main run is shortened so it
cannot consume the reflect budget.

- `GOOSE_JOB_REFLECT_ENABLED=true` (worker default when a job has no `reflect`)
- `GOOSE_JOB_REFLECT_TIMEOUT_SECONDS=300`
- `GOOSE_JOB_MAX_TURNS=60` (main run)
- `GOOSE_JOB_SUBAGENT_MAX_TURNS=40`


