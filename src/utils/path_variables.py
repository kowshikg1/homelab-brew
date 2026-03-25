from pathlib import Path
#Env files
ENV_FILE_GLOBAL = Path('./.env')
ENV_FILE_HANDLERS = Path('./src/handlers/.env')

#Configs
PATH_INGESTION_CONFIG = Path('./configs/ingestion_config.json')

#Paths
PATH_INGESTION_FOLDER = Path('./configs/ingestion')

#SQLite
DEFAULT_SQLITE_DB = './data/data.db'
INGESTION_SQLITE_DB = './data/ingestion.db'