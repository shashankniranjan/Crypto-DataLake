from .config import AggregatorSettings

__all__ = ["AggregationService", "AggregatorSettings"]


def __getattr__(name: str) -> object:
    if name == "AggregationService":
        from .main import AggregationService

        return AggregationService
    raise AttributeError(name)
