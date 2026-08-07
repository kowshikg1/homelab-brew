import {
  API_PROBE_TIMEOUT_MS,
  API_TARGET_CACHE_KEY,
  API_TARGET_CACHE_TTL_MS,
  API_TARGETS,
  LOCAL_SERVICES_CONFIG_PATH,
} from '../../constants/services'
import type { ApiTarget, NetworkMode } from '../../types/network'
import type { ServiceItem, ServicesResponse } from '../../types/services'

const SERVICES_FETCH_TIMEOUT_MS = Math.max(API_PROBE_TIMEOUT_MS * 3, 2500)

async function fetchJsonOrThrow(
  path: string,
  timeoutMs = SERVICES_FETCH_TIMEOUT_MS,
): Promise<ServicesResponse> {
  const controller = new AbortController()
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs)
  const response = await fetch(path, {
    signal: controller.signal,
    cache: 'no-store',
  }).finally(() => {
    window.clearTimeout(timeout)
  })

  if (!response.ok) {
    throw new Error(`${path} responded with status ${response.status}`)
  }

  return (await response.json()) as ServicesResponse
}

function withTimeout(url: string, timeoutMs: number): Promise<Response> {
  const controller = new AbortController()
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs)
  return fetch(url, {
    signal: controller.signal,
    cache: 'no-store',
  }).finally(() => {
    window.clearTimeout(timeout)
  })
}

async function isTargetReachable(target: ApiTarget): Promise<boolean> {
  try {
    const response = await withTimeout(
      `${target.baseUrl}/health`,
      API_PROBE_TIMEOUT_MS,
    )
    return response.ok
  } catch {
    return false
  }
}

function readCachedTarget(): ApiTarget | null {
  const raw = window.sessionStorage.getItem(API_TARGET_CACHE_KEY)
  if (!raw) {
    return null
  }

  try {
    const parsed = JSON.parse(raw) as {
      mode: NetworkMode
      baseUrl: string
      checkedAt: number
    }
    if (Date.now() - parsed.checkedAt > API_TARGET_CACHE_TTL_MS) {
      return null
    }
    return { mode: parsed.mode, baseUrl: parsed.baseUrl }
  } catch {
    return null
  }
}

function writeCachedTarget(target: ApiTarget): void {
  window.sessionStorage.setItem(
    API_TARGET_CACHE_KEY,
    JSON.stringify({
      mode: target.mode,
      baseUrl: target.baseUrl,
      checkedAt: Date.now(),
    }),
  )
}

function clearCachedTarget(): void {
  window.sessionStorage.removeItem(API_TARGET_CACHE_KEY)
}

async function detectApiTarget(): Promise<ApiTarget> {
  const cachedTarget = readCachedTarget()
  if (cachedTarget) {
    const matchedTarget = API_TARGETS.find(
      (target) =>
        target.baseUrl === cachedTarget.baseUrl &&
        target.mode === cachedTarget.mode,
    )

    if (matchedTarget && (await isTargetReachable(matchedTarget))) {
      return matchedTarget
    }

    clearCachedTarget()
  }

  for (const target of API_TARGETS) {
    if (await isTargetReachable(target)) {
      writeCachedTarget(target)
      return target
    }
  }

  return {
    mode: 'unknown',
    baseUrl: '',
  }
}

function pickUrlByMode(service: ServiceItem, mode: NetworkMode): string {
  const urlCandidates = service.urls ?? {}
  const mapByMode: Record<NetworkMode, Array<string | null | undefined>> = {
    lan: [urlCandidates.lan, urlCandidates.tailscale, urlCandidates.public],
    tailscale: [
      urlCandidates.tailscale,
      urlCandidates.lan,
      urlCandidates.public,
    ],
    public: [urlCandidates.public, urlCandidates.tailscale, urlCandidates.lan],
    unknown: [urlCandidates.lan, urlCandidates.tailscale, urlCandidates.public],
  }

  const chosen = mapByMode[mode].find((candidate) => !!candidate?.trim())
  return chosen ?? service.url
}

function resolveServiceUrls(
  payload: ServicesResponse,
  mode: NetworkMode,
): ServicesResponse {
  return {
    ...payload,
    services: payload.services.map((service) => ({
      ...service,
      url: pickUrlByMode(service, mode),
    })),
  }
}

export async function loadServicesResponse(): Promise<{
  payload: ServicesResponse
  fallbackUsed: boolean
  networkMode: NetworkMode
}> {
  const preferredTarget = await detectApiTarget()
  const targetsInOrder = [
    preferredTarget,
    ...API_TARGETS.filter(
      (target) =>
        !(
          target.baseUrl === preferredTarget.baseUrl &&
          target.mode === preferredTarget.mode
        ),
    ),
  ]

  let lastApiError: Error | null = null

  for (const target of targetsInOrder) {
    if (!target.baseUrl.trim()) {
      continue
    }

    try {
      const payload = await fetchJsonOrThrow(`${target.baseUrl}/services`)
      writeCachedTarget(target)
      return {
        payload: resolveServiceUrls(payload, target.mode),
        fallbackUsed: false,
        networkMode: target.mode,
      }
    } catch (apiError) {
      lastApiError =
        apiError instanceof Error
          ? apiError
          : new Error('Unknown API error while loading services')
    }
  }

  try {
    const payload = await fetchJsonOrThrow(LOCAL_SERVICES_CONFIG_PATH, 1500)
    return {
      payload: resolveServiceUrls(payload, preferredTarget.mode),
      fallbackUsed: true,
      networkMode: preferredTarget.mode,
    }
  } catch (localError) {
    const apiMessage = lastApiError?.message ?? 'Unknown API error'
    const localMessage =
      localError instanceof Error
        ? localError.message
        : 'Unknown local config error'
    throw new Error(
      `Failed to load services. API: ${apiMessage}. Local: ${localMessage}.`,
    )
  }
}
