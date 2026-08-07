import json
from textwrap import dedent
from unittest.mock import patch

from src.scripts.configs.compile_services_catalog import (
    compile_services_catalog,
)

# Matches the schema in configs/frontend/services-catalog.yml (hosts + port per service)
_BASE_YAML = dedent("""
    version: 1
    hosts:
      lan: http://192.168.0.100
      tailscale: http://100.72.0.10
      public: https://media.example.com
    categories:
      media:
        display_name: Media
        order: 1
    services:
      - name: jellyfin
        display_name: Jellyfin
        category: media
        port: ":8096"
        enabled: true
      - name: disabled_svc
        display_name: Disabled
        category: media
        port: ":9999"
        enabled: false
""").strip()


def _write_and_compile(tmp_path, yaml_text):
    source_path = tmp_path / 'services-catalog.yml'
    output_path = tmp_path / 'services-catalog.json'
    source_path.write_text(yaml_text)
    with (
        patch(
            'src.scripts.configs.compile_services_catalog.PATH_SERVICES_CATALOG',
            source_path,
        ),
        patch(
            'src.scripts.configs.compile_services_catalog.PATH_COMPILED_SERVICES_CATALOG',
            output_path,
        ),
    ):
        compile_services_catalog()
    return json.loads(output_path.read_text())


def test_compile_filters_disabled_services(tmp_path):
    payload = _write_and_compile(tmp_path, _BASE_YAML)
    assert len(payload['services']) == 1
    assert payload['services'][0]['name'] == 'jellyfin'


def test_compile_preserves_version_and_categories(tmp_path):
    payload = _write_and_compile(tmp_path, _BASE_YAML)
    assert payload['version'] == 1
    assert payload['categories']['media']['display_name'] == 'Media'
    assert payload['categories']['media']['order'] == 1


def test_compile_builds_urls_from_hosts_and_port(tmp_path):
    service = _write_and_compile(tmp_path, _BASE_YAML)['services'][0]
    assert service['url'] == 'http://192.168.0.100:8096'
    assert service['urls']['lan'] == 'http://192.168.0.100:8096'
    assert service['urls']['tailscale'] == 'http://100.72.0.10:8096'
    assert service['urls']['public'] == 'https://media.example.com:8096'


def test_compile_expands_env_vars_in_hosts(tmp_path):
    yaml_text = dedent("""
        version: 1
        hosts:
          lan: ${LAN_HOST}
          tailscale: ${TS_HOST}
        categories:
          media:
            display_name: Media
            order: 1
        services:
          - name: jellyfin
            category: media
            port: ":8096"
            enabled: true
    """).strip()
    with patch.dict(
        'os.environ',
        {'LAN_HOST': 'http://10.0.0.1', 'TS_HOST': 'http://100.0.0.1'},
    ):
        service = _write_and_compile(tmp_path, yaml_text)['services'][0]
    assert service['urls']['lan'] == 'http://10.0.0.1:8096'
    assert service['urls']['tailscale'] == 'http://100.0.0.1:8096'


def test_compile_protocol_override(tmp_path):
    yaml_text = dedent("""
        version: 1
        hosts:
          lan: http://192.168.0.100
        categories:
          admin:
            display_name: Admin
            order: 1
        services:
          - name: portainer
            category: admin
            port: ":9443"
            protocol: https
            enabled: true
    """).strip()
    service = _write_and_compile(tmp_path, yaml_text)['services'][0]
    assert service['urls']['lan'] == 'https://192.168.0.100:9443'


def test_compile_display_name_defaults_to_name(tmp_path):
    yaml_text = dedent("""
        version: 1
        hosts:
          lan: http://192.168.0.100
        categories:
          media:
            display_name: Media
            order: 1
        services:
          - name: jellyfin
            category: media
            port: ":8096"
            enabled: true
    """).strip()
    service = _write_and_compile(tmp_path, yaml_text)['services'][0]
    assert service['display_name'] == 'jellyfin'
