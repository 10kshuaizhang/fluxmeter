"""FluxMeter — streaming metering SDK for AI token billing."""

from fluxmeter.client import FluxMeter
from fluxmeter.event import TokenEvent
from fluxmeter.streaming import StreamingWrapper
from fluxmeter.wrap import BudgetExceededError, StreamKilledError, wrap

__version__ = "1.4.0"
__all__ = [
    "FluxMeter",
    "TokenEvent",
    "StreamingWrapper",
    "wrap",
    "BudgetExceededError",
    "StreamKilledError",
]
