"""FluxMeter — streaming metering SDK for AI token billing."""

from fluxmeter.client import DeliveryError, FluxMeter
from fluxmeter.event import TokenEvent
from fluxmeter.streaming import StreamingWrapper
from fluxmeter.wrap import BudgetExceededError, StreamKilledError, wrap

__version__ = "2.0.0"
__all__ = [
    "FluxMeter",
    "DeliveryError",
    "TokenEvent",
    "StreamingWrapper",
    "wrap",
    "BudgetExceededError",
    "StreamKilledError",
]
