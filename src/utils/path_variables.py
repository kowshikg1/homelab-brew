from pathlib import Path

# Env files
ENV_FILE_GLOBAL = Path('./.env')
ENV_FILE_HANDLERS = Path('./src/handlers/.env')

# Configs
PATH_INGESTION_CONFIG = Path('./configs/cache/ingestion_config.json')
# frontend service
PATH_SERVICES_CATALOG = Path('./configs/frontend/services-catalog.yml')
PATH_COMPILED_SERVICES_CATALOG = Path('./configs/cache/services-catalog.json')

# Paths
PATH_INGESTION_FOLDER = Path('./configs/ingestion')
PATH_DDL_FOLDER = Path('./configs/ddl')
PATH_MONITOR_FOLDER = Path('./configs/monitor')

# SQLite
SQLITE_DB_FOLDER = Path('./data')
DEFAULT_SQLITE_DB = './data/data.db'
INGESTION_SQLITE_DB = './data/ingestion.db'
