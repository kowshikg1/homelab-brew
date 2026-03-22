import os

from pathlib import Path
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)

class EnvManager:
    """A simple environment variable manager that reads from and writes to a .env file."""

    def __init__(self, env_file=".env") -> None:
        self.env_file = env_file
        self.env_vars = {}
        self.load_env()

    def load_env(self) -> None:
        """
        Load environment variables from the .env file into a dictionary.
        """
        if not os.path.exists(self.env_file):
            log.warning(f"{self.env_file} not found. No environment variables loaded.")
            return
        
        with open(self.env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    self.env_vars[key.strip()] = value.strip()

    def get(self, key: str, default: str = None) -> str:
        """
        Get the value of an environment variable.

        :param key: The name of the environment variable.
        :param default: The default value to return if the key is not found.

        :return: The value of the environment variable or the default if not found.
        """
        return self.env_vars.get(key, default)

    def set(self, key: str, value: str, **kwargs: str) -> None:
        """
        Set the value of an environment variable.

        :param key: The name of the environment variable.
        :param value: The value to set for the environment variable.
        :param kwargs: Additional key-value pairs to set.

        :return: None
        """
        self.env_vars[key] = value
        for k, v in kwargs.items():
            self.env_vars[k] = v
        self.save_env()

    def save_env(self) -> None:
        """
        Save the current environment variables to the .env file.
        """
        with open(self.env_file, 'w') as f:
            for key, value in self.env_vars.items():
                f.write(f"{key}={value}\n")