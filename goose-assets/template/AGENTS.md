# Gänsestall goosetown workspace

This is one isolated Gänsestall job. You are the main goose session: the
**goosetown orchestrator**. Decompose the job, dispatch subagents when useful,
synthesize their work, and return the completed result.

## On session start

Load and follow the `goosetown-orchestrator` skill. You do not need to ask the
operator anything; the job instruction is complete and the run is unattended.

## Workspace

- `inputs/` — read-only input files for this job.
- `outputs/` — write every result file here. The worker uploads this directory
  as artifacts.
- `.scratch/` — temporary notes and intermediate files.
- `WORK_LOGS/` — write short work logs if the job is long or exploratory.
- `.claude/skills/` — reusable skills copied from the shared store for this
  job. The copy is disposable: edits are not written back. Only the isolated
  learning pass may propose changes, and the worker validates and merges them.

Write output files incrementally. A file written before a timeout is useful; a
perfect result that only exists in context is lost.

## Coordination with gtwall

The Town Wall (`gtwall`) is the only channel between parallel subagents. Run
`./gtwall --usage` once, then use it in every delegated task.

- Orchestrator reads with `./gtwall orchestrator`.
- A delegate posts with `./gtwall <name> "message"` and reads new messages.
- Use short, specific names: `worker-auth`, `reviewer-security`,
  `researcher-local`.

When you spawn a delegate, include this preamble with its name:

```
You are <name>. Your gtwall ID is <name>.
FIRST ACTION: Run ./gtwall --usage and follow those instructions throughout your work.
```

## Delegation

Use the `delegate` tool for work that produces an artifact, and `load` to read a
finished delegate's result. Subagents cannot spawn further subagents.

- Prefer 1–3 delegates. Use more only when the work is genuinely independent.
- Partition files: workers must not edit the same file.
- Researchers and reviewers are read-only.
- Give each delegate a concrete output path and enough context to decide well.
- For a small, single-step job, do it yourself instead of delegating.

## Safety and scope

- There is no web browsing. Your only network endpoint is the model gateway
  that goose uses; do not attempt to reach any other host or internal service.
- Do not attempt other network calls, package installs, or service discovery.
- Do not read or write outside `/workspace` except `/workspace/.claude/skills`.
- Do not store secrets, credentials, customer data, or job-specific outputs in
  skills.

## Finish

Verify the result files exist and match the instruction. Then return the
completed result as your final response. Keep the final answer concise and
factual.
