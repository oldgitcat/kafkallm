# Security and isolation model

[Back to the main README](../README.md)

## Isolation model

Each job runs in its own KVM microVM.

| Property | Value |
|---|---|
| Kernel | Its own guest kernel, separate from the host |
| Privilege escalation | `no-new-privileges:true` |
| Resources | `GOOSE_JOB_RAM_MIB` (default `2048`), `GOOSE_JOB_CPUS` (default `2`), `GOOSE_JOB_PIDS_LIMIT` (default `512`) |
| Filesystem | Disposable container root filesystem, copied tools and Goose config, and a copied persistent skill store |
| Skills | Each job edits a disposable copy. The learning pass runs in a separate microVM, and only the worker merges validated changes into the shared store |
| Network | Attached only to the IPv6-only `gaensestall-jobs` internal network. The HTTP LLM proxy is the only peer; it validates the proxy token, accepts only the chat-completions and model-list endpoints, and forwards them to the MLflow AI Gateway. Internet, LAN, Compose services, other Podman services, and the Podman API are unreachable |
| Credentials | The real provider key is encrypted in Postgres and used only by MLflow. The job gets a rotatable proxy token that is valid only through the proxy |
| Tools | No browser, no `curl`/`wget` in the image; the prompt forbids web and other network use |
| Lifetime | The microVM and its job directory are removed after the job |

The worker runs with SELinux labeling disabled. The job microVM has no host
bind mounts, so no host filesystem is shared with the guest. The microVM still
runs with `no-new-privileges` and the microVM boundary.

What the boundary does **not** cover:

- **The hypervisor.** libkrun and KVM run on the host, so a VMM bug is still a
  host compromise. A microVM raises the bar, but it is not a bulletproof
  boundary.
- **Root-equivalent Podman API.** The mounted Podman socket is equivalent to
  root on the host. It is visible only inside the worker container, so the job
  microVMs cannot reach it, but a compromised worker still means a compromised
  host.
- **Shared skill store.** The main job cannot write the persistent store, but a
  compromised learning pass can propose misleading or malicious instructions.
  Merge validation is structural, not semantic. Review learned skills before
  relying on them in a higher-trust setting.
- **LLM proxy token.** The job holds a static proxy token for the whole job.
  A malicious job can reuse it for its own LLM requests, and MLflow does not
  enforce a per-job request or token budget. Rotate
  `LLM_PROXY_TOKEN` if a job is untrusted.
- **LLM proxy transport.** Proxy-to-MLflow traffic is plain HTTP on the internal
  Compose network. MLflow-to-provider traffic uses the upstream URL; TLS
  verification is on unless `LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY` is set.
- **GUI exposure.** Caddy publishes the GUI on `0.0.0.0` by default and
  enforces HTTP basic auth. The GUI itself is only reachable on the Compose
  network. Use `GUI_BIND=127.0.0.1` for local-only access, and put TLS in front
  of Caddy if the network is untrusted.
- **Proxy configuration.** The proxy forwards only allowlisted HTTP requests to
  one fixed upstream. A compromise of the proxy container still gives access to
  the MLflow AI Gateway.

The `gaensestall-jobs` network is IPv6-only and `internal`, so jobs have no IPv4
address and no default route. The LLM proxy is the only other container
on that network. Create the network on the host with
`./scripts/init-job-network.sh`; the worker fails closed if it is missing.


