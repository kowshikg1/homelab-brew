from src.handlers.env_manager import EnvManager
from pathlib import Path

class BaseIngestionHandler:
    def __init__(self, load_env=False) -> None:
        self.env_manager = EnvManager(Path("./src/handlers/.env")) if load_env else None

    def handle(self):
        raise NotImplementedError("Subclasses must implement the handle method.")