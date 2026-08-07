import os
import re
from pathlib import Path
from typing import Any

from src.api.models import ServiceCategory, ServiceItem, ServicesResponse
from src.handlers.env_manager import EnvManager
from src.utils.file import load_json, load_yaml
from src.utils.path_variables import (
    PATH_COMPILED_SERVICES_CATALOG,
    PATH_SERVICES_CATALOG,
)

ENV_PATTERN = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}')
NETWORKS = ('lan', 'tailscale', 'public')


def _expand_env(value: str, env_manager: EnvManager) -> str:
    def _replace(match: re.Match[str]) -> str:
        return os.getenv(match.group(1)) or env_manager.get(match.group(1), '')

    return ENV_PATTERN.sub(_replace, value)


def _build_url(host: str, port: str, protocol: str | None = None) -> str:
    host = host.rstrip('/')
    if protocol:
        host = re.sub(r'^https?://', f'{protocol}://', host)
    return f'{host}{port}'


def _load_payload() -> dict[str, Any]:
    compiled = Path(PATH_COMPILED_SERVICES_CATALOG)
    return (
        load_json(compiled)
        if compiled.exists()
        else load_yaml(PATH_SERVICES_CATALOG)
    )


def _build_service_urls(
    service: dict[str, Any],
    hosts: dict[str, str],
) -> tuple[dict[str, str] | None, str]:
    """Build per-network URLs from global hosts + service port (raw YAML path)."""
    port = service.pop('port')
    protocol = service.pop('protocol', None)
    urls = {
        network: _build_url(host, port, protocol)
        for network in NETWORKS
        if (host := hosts.get(network, ''))
    }
    default_url = (
        urls.get('lan') or urls.get('tailscale') or urls.get('public') or ''
    )
    return (urls or None), default_url


def load_services_catalog() -> ServicesResponse:
    payload = _load_payload() or {}
    env_manager = EnvManager()

    # Hosts block is only present in raw YAML; compiled JSON has urls already expanded.
    raw_hosts: dict[str, str] = payload.get('hosts') or {}
    hosts = {
        k: _expand_env(v, env_manager).rstrip('/') for k, v in raw_hosts.items()
    }

    categories: dict[str, ServiceCategory] = {
        k: ServiceCategory(**v)
        for k, v in (payload.get('categories') or {}).items()
    }

    services: list[ServiceItem] = []
    for raw in payload.get('services') or []:
        if not raw.get('enabled', False):
            continue
        data = dict(raw)
        data.setdefault('display_name', data.get('name', ''))

        if 'port' in data and hosts:
            data['urls'], data['url'] = _build_service_urls(data, hosts)
        else:
            data.pop('port', None)
            data.pop('protocol', None)

        services.append(ServiceItem(**data))

    services.sort(
        key=lambda s: (
            categories.get(
                s.category, ServiceCategory(display_name=s.category)
            ).order,
            s.display_name.lower(),
        )
    )

    return ServicesResponse(
        version=int(payload.get('version', 1)),
        categories=categories,
        services=services,
    )
