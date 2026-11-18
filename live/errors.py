"""Exceptions shared by the collector, the source client and the store."""


class SnapshotRequired(RuntimeError):
    """The source cannot resume the event stream and demands a full snapshot."""

    def __init__(self, generation=None, position=None):
        self.generation = generation
        self.position = position
        super().__init__('snapshot_required')


class GapOpen(RuntimeError):
    """The store has an unreconciled gap and refuses to pretend it is closed."""


class PositionGap(ValueError):
    """Received event positions do not continue the trusted boundary."""


class GenerationChanged(ValueError):
    """An event from another stream generation cannot be compared by position."""

    def __init__(self, message, generation=None, position=None):
        self.generation = generation
        self.position = position
        super().__init__(message)
