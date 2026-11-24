"""Exception hierarchy for PipeForge."""


class PipeForgeError(Exception):
    """Base class for all PipeForge errors."""


class SpecError(PipeForgeError):
    """The job spec is invalid or references a missing secret."""


class CheckpointError(PipeForgeError):
    """Checkpoint storage failed (as opposed to 'checkpoint not found')."""


class SourceError(PipeForgeError):
    """Reading from the source failed or the source is misconfigured."""


class SinkError(PipeForgeError):
    """Writing to the sink failed."""


class DuplicateKeyError(SinkError):
    """The batch contains several rows for the same unique key."""
