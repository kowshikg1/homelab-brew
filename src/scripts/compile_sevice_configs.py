from dataclasses import dataclass
from textwrap import dedent

from src.handlers.env_manager import EnvManager
from src.utils.commons import load_yaml

env = EnvManager()

@dataclass
class ServiceConfig:
    #Unit
    name: str
    description: str
    after: str = None
    wants: str = None
    #Service
    type: str = "simple"
    user: str = env.get("USER")
    project_path: str = env.get("PROJECT_PATH")
    venv_path: str = env.get("VENV_PATH")
    exec_module: str = None
    exec_file: str = None
    restart: str = "on-failure"
    restart_sec: str = "5"
    memory_max: str = None
    cpu_quota: str = None
    environment_pythonpath: str = None
    #Install
    wanted_by: str = "multi-user.target"

    def __post_init__(self):
        if not self.exec_module and not self.exec_file:
            raise ValueError("Either exec_module or exec_file must be provided.")


def load_service_config(yaml_path: str, service):
    data = load_yaml(yaml_path)[service]
    data['name'] = service
    service = ServiceConfig(**data)
    WorkingDirectory = f"/home/{service.user}/{service.project_path}"
    return dedent(f"""
    [Unit]
    Description={service.description}
    {f'After={service.after}' if service.after else ''}
    {f'Wants={service.wants}' if service.wants else ''}

    [Service]
    Type={service.type}
    User={service.user}
    WorkingDirectory=/home/{service.user}/{service.project_path}
    {f'ExecStart={service.venv_path}/bin/python -m {service.exec_module}' if service.exec_module else ''}
    {f'ExecStart={service.venv_path}/bin/python {service.exec_file}' if service.exec_file else ''}

    Restart={service.restart}
    RestartSec={service.restart_sec}

    {f'MemoryMax={service.memory_max}' if service.memory_max else ''}
    {f'CPUQuota={service.cpu_quota}' if service.cpu_quota else ''}

    # Environment=PYTHONUNBUFFERED=1
    {f'Environment=PYTHONPATH={service.environment_pythonpath}' if service.environment_pythonpath else f'Environment=PYTHONPATH={WorkingDirectory}'}

    [Install]
    WantedBy={service.wanted_by}
    """)

# sudo systemctl daemon-reexec
# sudo systemctl daemon-reload
# sudo systemctl start mqtt-telegram
# sudo systemctl enable mqtt-telegram
# sudo systemctl status mqtt-telegram
# journalctl -u mqtt-telegram -f
if __name__ == "__main__":
    config = load_service_config("./configs/services/mqtt_telegram.yml", "mqtt-telegram")
    print(config)