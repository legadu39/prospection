"""
core/dispatcher.py — SponsorDispatcher
Thin wrapper around ComputeGridOrchestrator exposing the interface
expected by pipeline_bridge.py.
"""
from core.workload_orchestrator import ComputeGridOrchestrator
from core.database import NexusDB


class SponsorDispatcher:
    """
    Wraps ComputeGridOrchestrator and exposes process_dispatch_cycle()
    as required by pipeline_bridge.py.
    """

    def __init__(self, db: NexusDB):
        self._orchestrator = ComputeGridOrchestrator(db)

    def process_dispatch_cycle(self):
        """Delegate to ComputeGridOrchestrator's main yield cycle."""
        return self._orchestrator.process_dispatch_cycle()
