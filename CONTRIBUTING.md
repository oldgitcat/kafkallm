# Contributing

Thanks for improving Kafkallm. The project is small enough that a focused pull
request with tests is the best way to contribute.

## Development setup

```bash
git clone https://github.com/oldgitcat/kafkallm.git
cd kafkallm
make setup      # creates .env and generates the gateway virtual key
make doctor     # checks the host and the configuration
make venv       # editable install with dev dependencies
```

Edit `.env` and add an `OPENROUTER_API_KEY` before running `make setup` again.
Never commit `.env`; it is ignored by Git.

The unit tests do not need Docker, Podman, KVM, or Kafka:

```bash
make test
make lint
```

`make format` applies Ruff formatting and safe lint fixes.

## Pull requests

- Keep changes focused and include a test for new behavior.
- Run `make test` and `make lint` before opening the pull request.
- Update the README and `.env.example` when a user-facing setting or workflow
  changes.
- Do not add secrets, generated artifacts, or host-specific paths.
- Describe the motivation, the change, and how you verified it.

## Code style

- Python 3.12+, standard library first.
- Ruff with the configuration in `pyproject.toml`.
- Prefer small modules and explicit data models (Pydantic for cross-service
  records).
- Keep the control plane dependency-light and the job image small.
- Comments should explain why, not restate the code.

## Security

See [SECURITY.md](SECURITY.md). Report vulnerabilities privately, not in a
public issue.
