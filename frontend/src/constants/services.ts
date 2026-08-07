import type { ApiTarget } from '../types/network'
import { detectNetworkMode } from '../utils/networkUtils'

// The port the API server runs on. Override via VITE_API_PORT env var.
const API_PORT = import.meta.env.VITE_API_PORT ?? '8010'

export const API_TARGET_CACHE_KEY = 'homelab-api-target-v1'
export const API_TARGET_CACHE_TTL_MS = 10 * 60 * 1000
export const API_PROBE_TIMEOUT_MS = 1200
export const LOCAL_SERVICES_CONFIG_PATH = '/services.local.json'

function buildApiTargets(): ApiTarget[] {
  const currentHost = window.location.hostname || '192.168.0.100'
  const currentMode = detectNetworkMode()

  // Candidates in probe order. Deduped by baseUrl below.
  const candidates: ApiTarget[] = [
    // 1. Current host — auto-detects mode; works for LAN and Tailscale without env vars
    {
      mode: currentMode,
      baseUrl: `${window.location.protocol}//${currentHost}:${API_PORT}/api`,
    },
    // 2. Explicit overrides (set these in .env if your API is on a different host)
    ...(import.meta.env.VITE_API_BASE_URL_LAN
      ? [{ mode: 'lan' as const, baseUrl: import.meta.env.VITE_API_BASE_URL_LAN }]
      : []),
    ...(import.meta.env.VITE_API_BASE_URL_TAILSCALE
      ? [{ mode: 'tailscale' as const, baseUrl: import.meta.env.VITE_API_BASE_URL_TAILSCALE }]
      : []),
    ...(import.meta.env.VITE_API_BASE_URL_PUBLIC
      ? [{ mode: 'public' as const, baseUrl: import.meta.env.VITE_API_BASE_URL_PUBLIC }]
      : []),
    // 3. Fully fixed override (bypasses all detection)
    ...(import.meta.env.VITE_API_BASE_URL
      ? [{ mode: 'unknown' as const, baseUrl: import.meta.env.VITE_API_BASE_URL }]
      : []),
  ]

  // Deduplicate by baseUrl
  const seen = new Set<string>()
  return candidates.filter(({ baseUrl }) => {
    const url = baseUrl.trim()
    if (!url || seen.has(url)) return false
    seen.add(url)
    return true
  })
}

export const API_TARGETS: ApiTarget[] = buildApiTargets()
