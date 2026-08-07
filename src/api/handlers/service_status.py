import asyncio
import os
import time

import httpx

from src.api.models import ServiceItem, ServiceNetworkStatus

CACHE_TTL_SECONDS = int(os.getenv('SERVICE_STATUS_CACHE_TTL_SECONDS', '30'))
REQUEST_TIMEOUT_SECONDS = float(
    os.getenv('SERVICE_STATUS_TIMEOUT_SECONDS', '1.2')
)
MAX_CONCURRENCY = int(os.getenv('SERVICE_STATUS_MAX_CONCURRENCY', '8'))
MOCK_SERVICE_STATUS = (
    os.getenv('MOCK_SERVICE_STATUS', 'false').lower() == 'true'
)

_status_cache: dict[str, tuple[float, str]] = {}
_cache_lock = asyncio.Lock()


def _get_probe_url(service: ServiceItem) -> str | None:
    """Return the URL to use for server-side health probing (always LAN)."""
    return service.url or None


async def _probe_url(url: str, client: httpx.AsyncClient) -> str:
    if MOCK_SERVICE_STATUS:
        return 'online'
    try:
        response = await client.get(url)
        return 'online' if response.status_code < 500 else 'offline'
    except Exception:
        return 'offline'


async def _get_status_for_url(
    url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> str:
    now = time.time()

    async with _cache_lock:
        cached = _status_cache.get(url)
        if cached and now - cached[0] < CACHE_TTL_SECONDS:
            return cached[1]

    async with semaphore:
        status = await _probe_url(url, client)

    async with _cache_lock:
        _status_cache[url] = (time.time(), status)

    return status


async def build_services_with_status(
    services: list[ServiceItem],
) -> list[ServiceItem]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)

    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=True
    ) as client:

        async def _enrich(service: ServiceItem) -> ServiceItem:
            url = _get_probe_url(service)
            if not url:
                return service.model_copy(
                    update={'status': ServiceNetworkStatus(status='unknown')}
                )

            status = await _get_status_for_url(url, client, semaphore)
            return service.model_copy(
                update={'status': ServiceNetworkStatus(status=status)}
            )

        return await asyncio.gather(*[_enrich(service) for service in services])
