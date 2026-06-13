import json
from typing import Any

import yaml


def load_json(file_path: str) -> Any:
    with open(file_path) as f:
        return json.load(f)


def save_json(data: Any, file_path: str) -> None:
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)


def load_yaml(file_path: str, encoding='utf-8') -> Any:
    with open(file_path, encoding=encoding) as f:
        return yaml.safe_load(f)
