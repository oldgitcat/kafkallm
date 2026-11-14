# Sandboxes

This directory builds the sandbox images used by the job microVMs.

```text
sandboxes/
├── standard/
│   └── Dockerfile          # standard job sandbox
└── reversing/
    ├── Dockerfile          # REA + Ghidra sandbox
    └── requirements.txt
```

## Image chain

```text
gaensestall-goose-job:latest   (standard)
        │
        ▼
gaensestall-rea-job:latest     (reversing)
```

- `standard/Dockerfile` builds `gaensestall-goose-job:latest`: Debian, the pinned
  goose CLI, common tools, and the goosetown template, built-in skills, and
  license from `goose-assets/`.
- `reversing/Dockerfile` builds `gaensestall-rea-job:latest`: the standard image
  plus REA, Ghidra, Kuna, JADX, Frida, apktool/apksigner, and common RE tools.

The separate LLM proxy image lives in `infra/llm-proxy/`.

The agents, skills, template, config, and goosetown license live in
`goose-assets/`. The image build context is the repository root, so the Dockerfiles
reference both `goose-assets/` and `sandboxes/`.

## Build

```bash
make images
# equivalent: ./scripts/build-goose-image.sh
```

For the optional REA + Ghidra image:

```bash
make rea-image
```

The images are built into the rootless Podman storage, not Docker. The worker
creates microVMs from them through the Podman API. The goose release is pinned
and its SHA-256 is verified during the build.

Add Debian packages to the standard image at build time:

```bash
GOOSE_JOB_EXTRA_PACKAGES="openjdk-17-jre-headless" \
  ./scripts/build-goose-image.sh
```

Use a custom Dockerfile for larger changes:

```bash
GOOSE_JOB_BASE_IMAGE=gaensestall-goose-job:latest \
GOOSE_JOB_DOCKERFILE=/path/to/your/Dockerfile \
  ./scripts/build-goose-image.sh
```

Host-provided tools and goose config work without rebuilding. See the main
README section "Editing the sandbox". Open the exact offline image with:

```bash
./scripts/sandbox-shell.sh
```

## Smoke test

```bash
podman run --rm --runtime=krun \
  --annotation krun.ram_mib=1024 --annotation krun.cpus=1 \
  gaensestall-goose-job:latest \
  sh -c 'goose --version; ls /opt/goosetown/skills'
```
