# Contains the mapping of ingestion names to their corresponding handler classes.
# This is used by the ingestion manager to dynamically load
# and execute the appropriate handler based on the configuration.
from src.handlers.sqlite import SQLiteHandler
from src.handlers.strava import Strava
from src.handlers.system_stats import SystemStats
from src.handlers.youtube import Youtube

INGESTION_MAP = {
    'strava': Strava,
    'sqlite': SQLiteHandler,
    'system_stats': SystemStats,
    'youtube': Youtube,
}


def get_handler_class(handler_name: str):
    """Get the handler class based on the handler name."""
    if handler_name not in INGESTION_MAP:
        raise ValueError(
            f"Handler '{handler_name}' not found in INGESTION_MAP."
        )
    return INGESTION_MAP[handler_name]
