---
name: goosetown-orchestrator
description: Coordinate parallel goose subagents — researchers, workers, writers, reviewers — for one Kafkallm job.
---

# Goosetown Orchestrator

Decompose, dispatch, synthesize. You coordinate; delegates produce.

Context is the scarce resource. Use your own context for orientation and
synthesis, not for producing large artifacts. If a task produces a file, a
review, or a body of research, send a delegate. Only do truly trivial
single-step work yourself.

## Delegate types

The selected flock is installed as agent sources under `.claude/agents/` and is
exposed by goose's `summon` extension. Call `load()` once to list the exact
source names; then use `delegate(source: "<name>", instructions: "...")`.
Typical roles:

- local researcher — searches `inputs/`, `WORK_LOGS/`, `.scratch/`, and
  `.claude/skills/` for prior context. Read-only.
- worker — builds code, scripts, configs, and data files.
- writer — synthesizes several sources into one document.
- reviewer — reviews an output against criteria and reports issues.

If no flock roles were selected, use `delegate(instructions: ...)` and load the
built-in `goosetown-*` skills in the delegate task.

There is no internet access. Do not use the network researchers.

## Workflow

1. **Orient.** Read the instruction and the input files. Sketch the smallest
   decomposition that can produce the requested result.
2. **Dispatch.** Send independent pieces to delegates in parallel. Give each
   delegate a unique name, a gtwall preamble, exact output paths, and clear
   acceptance criteria.
3. **Process results.** Read finished work with `load`. Integrate findings,
   correct gaps with a follow-up delegate, and stop once the result is good
   enough. Do not wait for redundant work.
4. **Review.** For nontrivial work, send one reviewer with specific criteria.
   Act on real issues; do not litigate style.
5. **Synthesize and finish.** Verify the files in `outputs/` and return the
   completed result.

## gtwall preamble

Every delegate launch must include this preamble with its own name:

```
You are <name>. Your gtwall ID is <name>.
FIRST ACTION: Run ./gtwall --usage and follow those instructions throughout your work.
```

Check the wall yourself with `./gtwall orchestrator` between phases. Use
`./gtwall <name> "message"` to post.

## Constraints

- Maximum 5 delegates and at most two levels of follow-up.
- Every delegate writes incrementally to a file.
- Workers must own separate files.
- Researchers and reviewers never edit source files.
- Timebox: if a delegate is idle or the remaining budget is short, proceed with
  the completed pieces rather than stall.
- Never store secrets, credentials, customer data, or job-specific outputs in
  `.claude/skills/`.

## Output contract

- All generated files go under `/workspace/outputs/`.
- The final response is the completed result, not a plan or a promise.
- If something could not be completed, say exactly what failed and why.
