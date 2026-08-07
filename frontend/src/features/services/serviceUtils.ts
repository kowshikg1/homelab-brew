import type { ServiceCategory, ServiceItem } from '../../types/services'

export function sortServices(
  services: ServiceItem[],
  categories: Record<string, ServiceCategory>,
): ServiceItem[] {
  return [...services].sort((left, right) => {
    const leftOrder = categories[left.category]?.order ?? 999
    const rightOrder = categories[right.category]?.order ?? 999
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder
    }
    return left.display_name.localeCompare(right.display_name)
  })
}

export function groupServicesByCategory(
  services: ServiceItem[],
): Record<string, ServiceItem[]> {
  return services.reduce<Record<string, ServiceItem[]>>((acc, svc) => {
    acc[svc.category] = acc[svc.category] ?? []
    acc[svc.category].push(svc)
    return acc
  }, {})
}
