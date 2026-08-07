import type { NetworkMode } from '../../types/network'
import type { ServiceCategory, ServiceItem } from '../../types/services'
import { ServiceCard } from './ServiceCard'
import { groupServicesByCategory } from './serviceUtils'

type ServicesPanelProps = {
  categories: Record<string, ServiceCategory>
  services: ServiceItem[]
  networkMode: NetworkMode
  loading: boolean
  error: string | null
}

export function ServicesPanel({
  categories,
  services,
  networkMode,
  loading,
  error,
}: ServicesPanelProps): JSX.Element {
  const groupedServices = groupServicesByCategory(services)

  return (
    <>
      <section className="hero">
        <h2>Services</h2>
        {/* <p>Quick access to your running homelab apps.</p> */}
      </section>

      {loading && <p className="status">Loading services...</p>}
      {error && <p className="status is-warning">{error}</p>}
      {!loading && services.length === 0 && (
        <p className="status">No services configured yet.</p>
      )}

      <div className="category-grid">
        {Object.entries(groupedServices).map(([category, items]) => (
          <section key={category} className="category-card">
            <h3>{categories[category]?.display_name ?? category}</h3>
            <div className="service-list">
              {items.map((service) => (
                <ServiceCard
                  key={service.name}
                  service={service}
                  networkMode={networkMode}
                />
              ))}
            </div>
          </section>
        ))}
      </div>
    </>
  )
}
