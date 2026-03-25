from pathlib import Path
from src.utils.commons import load_yaml, save_json,
from src.utils.path_variables import PATH_INGESTION_FOLDER, PATH_INGESTION_CONFIG

def compile_ingestion():
    compiled_configs = {}
    for config_file in Path(PATH_INGESTION_FOLDER).rglob("*.yml"):
        config_data = load_yaml(config_file)
        for job_name, job_config in config_data.items():
            if job_name in compiled_configs:
                raise ValueError(f"Duplicate job name '{job_name}' found in {config_file}. Job names must be unique across all config files.")
            compiled_configs[job_name] = job_config if job_config.get("is_active") else None  # Only include active jobs
    save_json(compiled_configs, PATH_INGESTION_CONFIG)

if __name__ == "__main__":
    compile_ingestion()