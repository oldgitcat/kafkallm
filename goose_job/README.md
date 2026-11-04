# goose job image

This directory builds the microVM image used for every job.

## Contents

- `Dockerfile` — Debian bookworm, the pinned goose CLI, and the goosetown
  template and skills
- `template/` — copied into `/workspace` at job start (`AGENTS.md`, `goose`,
  `gtwall`)
- `skills/` — copied into the persistent `.claude/skills` directory on first use
- `LICENSE-goosetown` — upstream goosetown license

The upstream files (`goose`, `gtwall`, the worker/writer/reviewer/local-researcher
skills) are from [aaif-goose/goosetown](https://github.com/aaif-goose/goosetown)
at commit `d1f62b0bce24ad0ec74bae51044378c4fed40600`, Apache 2.0.
`template/AGENTS.md` and `skills/goosetown-orchestrator/SKILL.md` are adapted for
an unattended, offline Kafkallm job.

## Build

```bash
./scripts/build-goose-image.sh
```

The image is built into the rootless Podman storage, not Docker. The worker
creates microVMs from it through the Podman API. The goose release is pinned and
its SHA-256 is verified during the build.

## Smoke test

```bash
podman run --rm --runtime=krun \
  --annotation krun.ram_mib=1024 --annotation krun.cpus=1 \
  kafkallm-goose-job:latest \
  sh -c 'goose --version; ls /opt/goosetown/skills'
```
