import type { NetworkMode } from '../../types/network'
import type { ServiceAvailability, ServiceItem } from '../../types/services'

type ServiceCardProps = {
  service: ServiceItem
  networkMode: NetworkMode
}

const ICON_MAP: Record<string, string> = {
  // by service name
  jellyfin: '🎬',
  radarr: '🎞️',
  sonarr: '📺',
  utorrent: '⬇️',
  immich: '📷',
  portainer: '🐳',
  // by icon key
  media: '🎬',
  film: '🎞️',
  tv: '📺',
  download: '⬇️',
  image: '📷',
  photo: '📷',
  whale: '🐳',
}

function pickIcon(service: ServiceItem): string {
  const key = service.icon?.trim().toLowerCase()
  return (
    (key && ICON_MAP[key]) ||
    ICON_MAP[service.name.toLowerCase()] ||
    '🔗'
  )
}

function pickStatus(service: ServiceItem): ServiceAvailability {
  return service.status?.status ?? 'unknown'
}

export function ServiceCard({ service }: ServiceCardProps): JSX.Element {
  // service.url is already resolved to the correct network URL by serviceDataSource
  const status = pickStatus(service)

  return (
    <a
      href={service.url}
      target="_blank"
      rel="noreferrer"
      className="service-card"
    >
      <div className="service-card-head">
        <span className="service-icon" aria-hidden="true">
          {pickIcon(service)}
        </span>
        <strong>{service.display_name}</strong>
      </div>

      <small className="service-url">{service.url}</small>

      <div className="service-status-row">
        <span className={`status-dot status-${status}`} aria-hidden="true" />
        <span className="service-status-label">{status}</span>
      </div>
    </a>
  )
}
