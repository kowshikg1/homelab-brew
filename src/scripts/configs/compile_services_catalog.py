import os
import re

from src.handlers.env_manager import EnvManager
from src.utils.file import load_yaml, save_json
from src.utils.path_variables import (
    PATH_COMPILED_SERVICES_CATALOG,
    PATH_SERVICES_CATALOG,
)

ENV_PATTERN = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}')
NETWORKS = ('lan', 'tailscale', 'public')


def _expand_env(value: str, env_manager: EnvManager) -> str:
    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return os.getenv(key) or env_manager.get(key, '')

    return ENV_PATTERN.sub(_replace, value)


def _build_url(
    host: str, port: str, protocol_override: str | None = None
) -> str:
    host = host.rstrip('/')
    if protocol_override:
        host = re.sub(r'^https?://', f'{protocol_override}://', host)
    return f'{host}{port}'


def compile_services_catalog() -> None:
    payload = load_yaml(PATH_SERVICES_CATALOG) or {}
    env_manager = EnvManager()

    # Expand env vars in global hosts block
    raw_hosts: dict[str, str] = payload.get('hosts') or {}
    hosts = {
        k: _expand_env(v, env_manager).rstrip('/') for k, v in raw_hosts.items()
    }

    compiled_services = []
    for raw_service in payload.get('services', []):
        if not raw_service.get('enabled', False):
            continue

        service = dict(raw_service)
        service.setdefault('display_name', service.get('name', ''))

        port = service.pop('port', '')
        protocol_override = service.pop('protocol', None)

        # Build urls per network from global hosts + per-service port
        urls: dict[str, str] = {}
        for network in NETWORKS:
            host = hosts.get(network, '')
            if host and port:
                urls[network] = _build_url(host, port, protocol_override)

        service['urls'] = urls if urls else None
        service['url'] = (
            urls.get('lan') or urls.get('tailscale') or urls.get('public') or ''
        )
        service['status_urls'] = None

        compiled_services.append(service)

    output = {
        'version': int(payload.get('version', 1)),
        'categories': payload.get('categories', {}),
        'services': compiled_services,
    }
    save_json(output, PATH_COMPILED_SERVICES_CATALOG)


if __name__ == '__main__':
    compile_services_catalog()
