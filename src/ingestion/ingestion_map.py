#Contains the mapping of ingestion names to their corresponding handler classes. 
#This is used by the ingestion manager to dynamically load 
#and execute the appropriate handler based on the configuration.
from src.handlers.env_manager import EnvManager
from src.handlers.strava import Strava
from src.handlers.sqlite import SQLiteHandler

INGESTION_MAP = {
    "strava": "Strava",
    "sqlite": "SQLiteHandler",
}

def get_handler_class(handler_name: str):
    """Get the handler class based on the handler name."""
    handler_class_name = INGESTION_MAP.get(handler_name)
    if not handler_class_name:
        raise ValueError(f"Handler '{handler_name}' not found in INGESTION_MAP.")
    
    module = __import__(f"src.handlers.{handler_name}", fromlist=[handler_class_name])
    handler_class = getattr(module, handler_class_name)
    
    return handler_class