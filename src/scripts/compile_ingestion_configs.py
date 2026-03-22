from pathlib import Path
from src.utils.commons import (
    load_yaml,
    save_json,
)

INGESTION_CONGIG_PATH = Path("./configs/ingestion")
SAVE_PATH = Path("./configs/ingestion_config.json")

def compile_ingestion():
    compiled_configs = {}
    for config_file in Path(INGESTION_CONGIG_PATH).rglob("*.yml"):
        config_data = load_yaml(config_file)
        for job_name, job_config in config_data.items():
            if job_name in compiled_configs:
                raise ValueError(f"Duplicate job name '{job_name}' found in {config_file}. Job names must be unique across all config files.")
            compiled_configs[job_name] = job_config if job_config.get("is_active") else None  # Only include active jobs
    save_json(compiled_configs, SAVE_PATH)

if __name__ == "__main__":
    compile_ingestion()