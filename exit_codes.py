"""Shared process exit codes used by pipeline wrappers and child processes."""

from enum import IntEnum


class ProcessExitCode(IntEnum):
    """Exit codes shared across subprocess boundaries."""

    RESTART = 42
    STREAM_ERROR = 43


__all__ = ["ProcessExitCode"]
