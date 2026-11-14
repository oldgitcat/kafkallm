# Goose assets

This directory holds the built-in goosetown agents, role skills, workspace
template, and Goose configuration used by every job.

## Contents

- `agents/` — built-in agent profiles, copied into the worker/GUI image and
  seeded into the host agent directory on first start
- `skills/` — built-in role skills baked into the sandbox image and seeded into
  each job's disposable `.claude/skills` copy
- `template/` — copied into `/workspace` at job start (`AGENTS.md`, `goose`,
  `gtwall`)
- `config/config.yaml.example` — example Goose configuration, including MCP
  extensions
- `LICENSE-goosetown` — upstream goosetown license

The upstream files (`goose`, `gtwall`, the worker/writer/reviewer/local-researcher
skills) are from [aaif-goose/goosetown](https://github.com/aaif-goose/goosetown)
at commit `d1f62b0bce24ad0ec74bae51044378c4fed40600`, Apache 2.0.
`template/AGENTS.md` and `skills/goosetown-orchestrator/SKILL.md` are adapted for
an unattended, offline Gänsestall job.

The sandbox images are built from [`sandboxes/`](../sandboxes/README.md). Host-provided
tools and Goose config work without rebuilding; see the main README section
"Editing the sandbox".
