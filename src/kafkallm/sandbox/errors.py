"""Exceptions raised by the goose job runtime."""

from __future__ import annotations


class GooseJobRuntimeError(RuntimeError):
    pass


class JobCancelledError(GooseJobRuntimeError):
    pass


class WorkerShutdown(GooseJobRuntimeError):
    pass
