import { useEffect, useState } from 'react'

import type { NetworkMode } from '../../types/network'
import type {
  ServiceCategory,
  ServiceItem,
  ServicesState,
} from '../../types/services'
import { loadServicesResponse } from './serviceDataSource'
import { sortServices } from './serviceUtils'

export function useServices(): ServicesState {
  const [services, setServices] = useState<ServiceItem[]>([])
  const [categories, setCategories] = useState<Record<string, ServiceCategory>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [networkMode, setNetworkMode] = useState<NetworkMode>('unknown')

  useEffect(() => {
    const loadServices = async () => {
      try {
        const { payload, fallbackUsed, networkMode } =
          await loadServicesResponse()
        setNetworkMode(networkMode)
        setCategories(payload.categories)
        setServices(sortServices(payload.services, payload.categories))
        setError(
          fallbackUsed
            ? `Using local service config because API is unavailable (${networkMode} mode).`
            : null,
        )
      } catch (loadingError) {
        const message =
          loadingError instanceof Error
            ? loadingError.message
            : 'Unknown services loading error'
        setError(message)
        setCategories({})
        setServices([])
        setNetworkMode('unknown')
      } finally {
        setLoading(false)
      }
    }

    void loadServices()
  }, [])

  return { services, categories, loading, error, networkMode }
}
