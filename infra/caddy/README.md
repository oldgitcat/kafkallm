# Caddy

Caddy 2.10 terminates the GUI endpoint and enforces HTTP basic auth. The
service is defined in the top-level `compose.yaml`.

- Host listener: `GUI_BIND:GUI_PORT` (default `0.0.0.0:8090`)
- Caddyfile: `infra/caddy/Caddyfile`
- Credentials: `GUI_AUTH_USER` and `GUI_AUTH_HASH` in `.env`

Generate or change the credentials with `make gui-auth` or
`make gui-auth-force`.
