import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

import src.api.handlers.service_status as ss_module
from src.api.handlers.service_status import (
    _probe_url,
    build_services_with_status,
)
from src.api.models import ServiceItem


def _make_service(name='test', url='http://10.0.0.1:8096') -> ServiceItem:
    return ServiceItem(name=name, display_name=name, category='media', url=url)


# --- _probe_url ---


def test_probe_url_returns_online_for_2xx():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response

    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', False):
        result = asyncio.run(_probe_url('http://10.0.0.1:8096', mock_client))
    assert result == 'online'


def test_probe_url_returns_offline_for_5xx():
    mock_response = MagicMock()
    mock_response.status_code = 503
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response

    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', False):
        result = asyncio.run(_probe_url('http://10.0.0.1:8096', mock_client))
    assert result == 'offline'


def test_probe_url_returns_online_for_4xx():
    # 4xx means the server is reachable, not offline
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response

    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', False):
        result = asyncio.run(_probe_url('http://10.0.0.1:8096', mock_client))
    assert result == 'online'


def test_probe_url_returns_offline_on_connect_error():
    mock_client = AsyncMock()
    mock_client.get.side_effect = httpx.ConnectError('refused')

    result = asyncio.run(_probe_url('http://10.0.0.1:8096', mock_client))
    assert result == 'offline'


def test_probe_url_mock_mode_skips_request():
    mock_client = AsyncMock()
    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', True):
        result = asyncio.run(_probe_url('http://10.0.0.1:8096', mock_client))
    assert result == 'online'
    mock_client.get.assert_not_called()


# --- build_services_with_status ---


def test_build_services_mock_mode_returns_online():
    services = [_make_service('jellyfin')]
    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', True):
        result = asyncio.run(build_services_with_status(services))
    assert result[0].status.status == 'online'


def test_build_services_no_url_returns_unknown():
    service = ServiceItem(
        name='no_url', display_name='No URL', category='media', url=''
    )
    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', False):
        result = asyncio.run(build_services_with_status([service]))
    assert result[0].status.status == 'unknown'


def test_build_services_uses_cached_status():
    ss_module._status_cache.clear()
    url = 'http://10.0.0.1:8096'
    ss_module._status_cache[url] = (time.time(), 'online')

    service = _make_service(url=url)
    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', False):
        result = asyncio.run(build_services_with_status([service]))
    assert result[0].status.status == 'online'
    ss_module._status_cache.clear()


def test_build_services_expired_cache_re_probes():
    ss_module._status_cache.clear()
    url = 'http://10.0.0.1:8096'
    # Write an expired entry
    ss_module._status_cache[url] = (
        time.time() - ss_module.CACHE_TTL_SECONDS - 1,
        'online',
    )

    service = _make_service(url=url)
    mock_response = MagicMock()
    mock_response.status_code = 503

    with (
        patch.object(ss_module, 'MOCK_SERVICE_STATUS', False),
        patch('httpx.AsyncClient') as mock_client_cls,
    ):
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client
        result = asyncio.run(build_services_with_status([service]))

    assert result[0].status.status == 'offline'
    ss_module._status_cache.clear()


def test_build_services_preserves_other_fields():
    service = _make_service('jellyfin', 'http://10.0.0.1:8096')
    with patch.object(ss_module, 'MOCK_SERVICE_STATUS', True):
        result = asyncio.run(build_services_with_status([service]))
    assert result[0].name == 'jellyfin'
    assert result[0].url == 'http://10.0.0.1:8096'
