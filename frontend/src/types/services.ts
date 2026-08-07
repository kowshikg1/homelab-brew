import type { NetworkMode } from './network'

export type ServiceAvailability = 'online' | 'offline' | 'unknown'

export type ServiceItem = {
  name: string
  display_name: string
  category: string
  url: string
  icon?: string | null
  urls?: {
    lan?: string | null
    tailscale?: string | null
    public?: string | null
  } | null
  status?: {
    status?: ServiceAvailability | null
  } | null
  enabled: boolean
}

export type ServiceCategory = {
  display_name: string
  order: number
}

export type ServicesResponse = {
  version: number
  categories: Record<string, ServiceCategory>
  services: ServiceItem[]
}

export type ServicesState = {
  services: ServiceItem[]
  categories: Record<string, ServiceCategory>
  loading: boolean
  error: string | null
  networkMode: NetworkMode
}
