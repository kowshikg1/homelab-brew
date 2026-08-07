import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routers.health import router as health_router
from src.api.routers.services import router as services_router


def _cors_config() -> dict:
    configured = os.getenv('API_CORS_ORIGINS', '').strip()
    if configured:
        return {
            'allow_origins': [
                o.strip() for o in configured.split(',') if o.strip()
            ],
            'allow_credentials': True,
        }
    # No explicit allowlist — allow all origins (private homelab, not public-facing)
    return {
        'allow_origins': ['*'],
        'allow_credentials': False,  # cannot use credentials with wildcard
    }


def create_app() -> FastAPI:
    app = FastAPI(title='Homelab Brew API', version='0.1.0')

    app.add_middleware(
        CORSMiddleware,
        **_cors_config(),
        allow_methods=['*'],
        allow_headers=['*'],
    )

    app.include_router(health_router)
    app.include_router(services_router)
    return app


app = create_app()
