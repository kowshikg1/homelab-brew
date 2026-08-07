from unittest.mock import MagicMock, patch

from src.api.handlers.catalog import (
    _build_service_urls,
    _build_url,
    _expand_env,
    load_services_catalog,
)

# --- _expand_env ---


def test_expand_env_replaces_os_env_var(monkeypatch):
    monkeypatch.setenv('MY_HOST', 'http://10.0.0.1')
    env = MagicMock()
    env.get.return_value = ''
    assert _expand_env('${MY_HOST}', env) == 'http://10.0.0.1'


def test_expand_env_falls_back_to_env_manager(monkeypatch):
    monkeypatch.delenv('MY_HOST', raising=False)
    env = MagicMock()
    env.get.return_value = 'http://fallback'
    assert _expand_env('${MY_HOST}', env) == 'http://fallback'


def test_expand_env_no_placeholders():
    assert (
        _expand_env('http://static.host', MagicMock()) == 'http://static.host'
    )


def test_expand_env_multiple_placeholders(monkeypatch):
    monkeypatch.setenv('SCHEME', 'http')
    monkeypatch.setenv('HOST', '10.0.0.1')
    env = MagicMock()
    env.get.return_value = ''
    assert _expand_env('${SCHEME}://${HOST}', env) == 'http://10.0.0.1'


# --- _build_url ---


def test_build_url_basic():
    assert (
        _build_url('http://192.168.0.1', ':8096') == 'http://192.168.0.1:8096'
    )


def test_build_url_strips_trailing_slash():
    assert (
        _build_url('http://192.168.0.1/', ':8096') == 'http://192.168.0.1:8096'
    )


def test_build_url_protocol_override_http_to_https():
    assert (
        _build_url('http://192.168.0.1', ':9443', 'https')
        == 'https://192.168.0.1:9443'
    )


def test_build_url_protocol_override_preserves_host():
    assert (
        _build_url('https://example.com', ':443', 'http')
        == 'http://example.com:443'
    )


def test_build_url_no_protocol_override():
    assert (
        _build_url('https://example.com', ':443') == 'https://example.com:443'
    )


# --- _build_service_urls ---


def test_build_service_urls_all_networks():
    service = {'port': ':8096'}
    hosts = {
        'lan': 'http://10.0.0.1',
        'tailscale': 'http://100.0.0.1',
        'public': 'https://example.com',
    }
    urls, default = _build_service_urls(service, hosts)
    assert urls == {
        'lan': 'http://10.0.0.1:8096',
        'tailscale': 'http://100.0.0.1:8096',
        'public': 'https://example.com:8096',
    }
    assert default == 'http://10.0.0.1:8096'


def test_build_service_urls_lan_takes_priority_for_default():
    service = {'port': ':8096'}
    hosts = {'lan': 'http://10.0.0.1', 'tailscale': 'http://100.0.0.1'}
    _, default = _build_service_urls(service, hosts)
    assert default == 'http://10.0.0.1:8096'


def test_build_service_urls_falls_back_to_tailscale():
    service = {'port': ':8096'}
    _, default = _build_service_urls(service, {'tailscale': 'http://100.0.0.1'})
    assert default == 'http://100.0.0.1:8096'


def test_build_service_urls_pops_port_and_protocol():
    service = {'port': ':8096', 'protocol': 'https', 'name': 'x'}
    _build_service_urls(service, {'lan': 'http://10.0.0.1'})
    assert 'port' not in service
    assert 'protocol' not in service


def test_build_service_urls_empty_hosts_returns_none():
    service = {'port': ':8096'}
    urls, default = _build_service_urls(service, {})
    assert urls is None
    assert default == ''


# --- load_services_catalog ---

_SAMPLE_PAYLOAD = {
    'version': 1,
    'hosts': {'lan': 'http://10.0.0.1', 'tailscale': 'http://100.0.0.1'},
    'categories': {
        'media': {'display_name': 'Media', 'order': 1},
        'admin': {'display_name': 'Admin', 'order': 2},
    },
    'services': [
        {
            'name': 'jellyfin',
            'category': 'media',
            'port': ':8096',
            'enabled': True,
        },
        {
            'name': 'portainer',
            'category': 'admin',
            'port': ':9443',
            'enabled': True,
        },
        {
            'name': 'disabled',
            'category': 'media',
            'port': ':9999',
            'enabled': False,
        },
    ],
}


def _load(payload=_SAMPLE_PAYLOAD):
    with (
        patch('src.api.handlers.catalog._load_payload', return_value=payload),
        patch('src.api.handlers.catalog.EnvManager'),
    ):
        return load_services_catalog()


def test_load_services_catalog_filters_disabled():
    result = _load()
    names = [s.name for s in result.services]
    assert 'disabled' not in names
    assert len(result.services) == 2


def test_load_services_catalog_builds_urls():
    service = next(s for s in _load().services if s.name == 'jellyfin')
    assert service.url == 'http://10.0.0.1:8096'
    assert service.urls.lan == 'http://10.0.0.1:8096'
    assert service.urls.tailscale == 'http://100.0.0.1:8096'


def test_load_services_catalog_defaults_display_name_to_name():
    service = next(s for s in _load().services if s.name == 'jellyfin')
    assert service.display_name == 'jellyfin'


def test_load_services_catalog_preserves_version_and_categories():
    result = _load()
    assert result.version == 1
    assert 'media' in result.categories
    assert result.categories['media'].display_name == 'Media'


def test_load_services_catalog_sorted_by_category_order_then_name():
    result = _load()
    names = [s.name for s in result.services]
    # media (order=1) before admin (order=2)
    assert names.index('jellyfin') < names.index('portainer')


def test_load_services_catalog_empty_services():
    payload = {**_SAMPLE_PAYLOAD, 'services': []}
    result = _load(payload)
    assert result.services == []
