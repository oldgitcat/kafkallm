"""Shell and prompt assets baked into the job microVM bundles."""

from __future__ import annotations

_VM_RUN_SCRIPT = r"""#!/bin/sh
set -u

# The microVM's guest root maps to the host user that runs Podman, so the
# workspace root can end up owned by that UID while the worker that collects
# and removes the tree runs as a different UID. Make the whole tree writable
# on every exit path so the worker can clean it up.
cleanup_permissions() {
  chmod -R a+rwX /workspace 2>/dev/null || true
}
trap cleanup_permissions EXIT INT TERM

RESULT_DIR=/workspace/.kafkallm
if ! mkdir -p "$RESULT_DIR" 2>/dev/null; then
  echo "Cannot create $RESULT_DIR in the shared workspace." >&2
  exit 3
fi

# Seed the workspace with goosetown (AGENTS.md, the goose wrapper, gtwall) and
# the orchestrator role skills. The persistent skill store is read-only; each
# job edits a disposable copy so it cannot poison later jobs.
seed_skills() {
  source_dir=$1
  target_dir=$2
  [ -d "$source_dir" ] || return 0
  mkdir -p "$target_dir"
  for skill in "$source_dir"/*; do
    [ -e "$skill" ] || continue
    name=$(basename "$skill")
    if [ ! -e "$target_dir/$name" ]; then
      cp -a "$skill" "$target_dir/" 2>/dev/null || true
    fi
  done
}
if [ -d /opt/goosetown/template ]; then
  cp -an /opt/goosetown/template/. /workspace/ 2>/dev/null || true
fi
mkdir -p /workspace/.claude/skills
seed_skills /opt/kafkallm/skills-persistent /workspace/.claude/skills
seed_skills /opt/goosetown/skills /workspace/.claude/skills
# Sandbox-specific skills are baked into the image (for example Kuna and REA
# guidance in the reverse image) and are not visible to other sandboxes.
seed_skills /opt/kafkallm/sandbox-skills /workspace/.claude/skills
# REA's own doctor looks for its skill under HOME as well.
seed_skills /opt/kafkallm/sandbox-skills /opt/data/.claude/skills
# Rootless Podman maps the job uid into a subuid range, so worker-created files
# are not owned by the job. The disposable skills copy must stay writable.
chmod -R a+rwX /workspace/.claude 2>/dev/null || true
chmod +x /workspace/goose /workspace/gtwall 2>/dev/null || true

# Seed goose's user config from the host-mounted sandbox config. This is how
# operators add MCP servers and other goose extensions without rebuilding.
if [ -d /opt/kafkallm/config ]; then
  mkdir -p "$GOOSE_PATH_ROOT/config"
  cp -an /opt/kafkallm/config/. "$GOOSE_PATH_ROOT/config/" 2>/dev/null || true
fi

cd /workspace || exit 3
export GOOSE_BIN="${GOOSE_BIN:-/usr/local/goose/bin/goose}"
export GOOSE_MOIM_MESSAGE_FILE="/tmp/goose-telepathy.txt"
export GOOSE_GTWALL_FILE="/tmp/goose-wall.log"
: > "$GOOSE_MOIM_MESSAGE_FILE"
: > "$GOOSE_GTWALL_FILE"

main_timeout=${KAFKALLM_JOB_MAIN_TIMEOUT:-300}

# Summon provides the delegate/load tools, and Skills makes the goosetown role
# skills discoverable inside the orchestrator and its delegates. Keep both out
# of single-agent runs so those jobs cannot spawn subagents by accident.
builtins=developer
if [ "${KAFKALLM_MULTI_AGENT:-0}" = "1" ]; then
  builtins="$builtins,summon,skills"
fi

# The sandbox image may ship local stdio MCP servers as extensions. Ghidra and
# other tools also need a large scratch area on the host-backed workspace, not
# the small /tmp tmpfs.
mkdir -p /workspace/.scratch/tmp
# krun maps guest writes to the host user that runs Podman, while the worker
# collects the workspace as a different UID. Keep the scratch tree writable
# so the worker can remove it after the job.
chmod 0777 /workspace/.scratch /workspace/.scratch/tmp 2>/dev/null || true
export TMPDIR=/workspace/.scratch/tmp

set --
if [ -n "${KAFKALLM_SANDBOX_EXTENSIONS:-}" ]; then
  old_ifs=$IFS
  IFS=,
  for extension in $KAFKALLM_SANDBOX_EXTENSIONS; do
    extension_path=${extension#*:}
    if [ -x "$extension_path" ]; then
      set -- "$@" --with-extension "$extension"
    else
      echo "Sandbox extension $extension is missing at $extension_path." >&2
    fi
  done
  IFS=$old_ifs
fi

timeout --signal=TERM "$main_timeout" \
  stdbuf -oL -eL ./goose run --no-session -q --output-format stream-json \
    --with-builtin "$builtins" \
    --max-turns "${GOOSE_MAX_TURNS:-60}" \
    "$@" \
    -i /jobboot/main-prompt.txt \
  > "$RESULT_DIR/main.jsonl" 2> "$RESULT_DIR/main.err"
echo "$?" > "$RESULT_DIR/main.exit"
if [ "$(cat "$RESULT_DIR/main.exit")" = "124" ] \
   && [ ! -s "$RESULT_DIR/main.jsonl" ]; then
  cat > "$RESULT_DIR/main.jsonl" <<'JSON'
{"type":"error","message":"goose exceeded the main time budget."}
JSON
fi

# Ghidra and other tools may create read-only files in the scratch tree. Make
# everything removable by the worker UID before the microVM exits.
chmod -R a+rwX /workspace/.scratch 2>/dev/null || true
"""

_REFLECT_RUN_SCRIPT = r"""#!/bin/sh
set -u
umask 022

# The reflect workspace is created by the worker but the guest root may take
# ownership of its root. Make the tree writable on every exit path so the
# worker can read it for validation and remove it afterwards.
cleanup_permissions() {
  chmod -R a+rwX /workspace 2>/dev/null || true
}
trap cleanup_permissions EXIT INT TERM

RESULT_DIR=/workspace/.kafkallm
if ! mkdir -p "$RESULT_DIR" 2>/dev/null; then
  echo "Cannot create $RESULT_DIR in the shared workspace." >&2
  exit 3
fi
cd /workspace || exit 3
export GOOSE_BIN="${GOOSE_BIN:-/usr/local/goose/bin/goose}"
export GOOSE_MOIM_MESSAGE_FILE="/tmp/goose-telepathy.txt"
export GOOSE_GTWALL_FILE="/tmp/goose-wall.log"
: > "$GOOSE_MOIM_MESSAGE_FILE"
: > "$GOOSE_GTWALL_FILE"
mkdir -p /workspace/.scratch
chmod 0777 /workspace/.scratch 2>/dev/null || true
export TMPDIR=/workspace/.scratch

timeout --signal=TERM "$KAFKALLM_JOB_REFLECT_TIMEOUT" \
  stdbuf -oL -eL "$GOOSE_BIN" run --no-session -q --output-format stream-json \
    --with-builtin developer \
    --max-turns "${GOOSE_REFLECT_MAX_TURNS:-20}" \
    -i /jobboot/reflect-prompt.txt \
  > "$RESULT_DIR/reflect.jsonl" 2> "$RESULT_DIR/reflect.err"
echo "$?" > "$RESULT_DIR/reflect.exit"

# The worker validates and merges this tree. Make it readable after the guest
# root has written it through the mapped subuid.
chmod -R a+rX /workspace/.claude/skills 2>/dev/null || true
"""

_REFLECT_PROMPT = """You are the isolated learning pass for one completed Gänsestall job.

You may propose a small, focused set of reusable skill changes (at most three
skills). The worker validates the result and merges it into the shared skill
store; nothing else from this run is kept.

1. Inspect the shared skills in /workspace/.claude/skills with the file tools.
2. Read the completed job's outputs in /workspace/outputs (read-only). Ignore
   the internal control files in /workspace/.kafkallm.
3. Update the best matching skill, or create one class-level skill if none
   fits. Never name a skill after this job, run, error, or one-off artifact.
4. Capture durable procedures, commands, tool quirks, verification steps, and
   templates. Do not store secrets, credentials, customer data, job-specific
   outputs, or transient facts.
5. Only regular files inside a directory with SKILL.md are kept. Symlinks,
   oversized files, and more than three skill changes are rejected. Existing
   files are never deleted.
6. If there is nothing durable to save, do nothing.

Keep it short: a few focused skill changes and a few tool calls. Return a
one-line summary.
"""
