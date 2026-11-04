"""Allowlisting TCP proxy for the isolated goose job network.

The job microVM is attached only to an IPv6-only, internal Podman network. This
proxy is the only other container on that network. It forwards every accepted
TCP connection to one fixed upstream: the agentgateway listener on the host.

The proxy deliberately has no HTTP awareness and no configurable target list.
A job can therefore reach the model gateway and nothing else.
"""

from __future__ import annotations

import asyncio
import os
import sys

_READ_SIZE = 64 * 1024


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(_READ_SIZE):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def _handle(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    upstream_host: str,
    upstream_port: int,
) -> None:
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            upstream_host, upstream_port
        )
    except OSError as exc:
        print(
            f"gateway proxy: upstream {upstream_host}:{upstream_port} "
            f"is unavailable: {exc}",
            file=sys.stderr,
            flush=True,
        )
        client_writer.close()
        return

    await asyncio.gather(
        _pipe(client_reader, upstream_writer),
        _pipe(upstream_reader, client_writer),
    )


async def main() -> None:
    listen_port = int(os.environ.get("LISTEN_PORT", "8091"))
    upstream_host = os.environ.get("UPSTREAM_HOST", "host.containers.internal")
    upstream_port = int(os.environ.get("UPSTREAM_PORT", str(listen_port)))

    server = await asyncio.start_server(
        lambda reader, writer: _handle(reader, writer, upstream_host, upstream_port),
        host="::",
        port=listen_port,
    )
    print(
        f"gateway proxy: listening on [::]:{listen_port}, "
        f"forwarding to {upstream_host}:{upstream_port}",
        file=sys.stderr,
        flush=True,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
