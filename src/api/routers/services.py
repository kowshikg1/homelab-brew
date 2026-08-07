from fastapi import APIRouter

from src.api.handlers.catalog import load_services_catalog
from src.api.handlers.service_status import build_services_with_status
from src.api.models import ServicesResponse

router = APIRouter(prefix='/api', tags=['services'])


@router.get('/services', response_model=ServicesResponse)
async def get_services() -> ServicesResponse:
    payload = load_services_catalog()
    enriched_services = await build_services_with_status(payload.services)
    return ServicesResponse(
        version=payload.version,
        categories=payload.categories,
        services=enriched_services,
    )
