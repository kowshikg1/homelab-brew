from pydantic import BaseModel, Field


class ServiceUrls(BaseModel):
    lan: str | None = None
    tailscale: str | None = None
    public: str | None = None


class ServiceNetworkStatus(BaseModel):
    status: str | None = None


class ServiceItem(BaseModel):
    name: str
    display_name: str
    category: str
    url: str
    icon: str | None = None
    urls: ServiceUrls | None = None
    status: ServiceNetworkStatus | None = None
    enabled: bool = True


class ServiceCategory(BaseModel):
    display_name: str
    order: int = Field(default=999, ge=0)


class ServicesResponse(BaseModel):
    version: int
    categories: dict[str, ServiceCategory]
    services: list[ServiceItem]
