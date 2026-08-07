import type { NetworkMode } from '../types/network'

/**
 * Check if an IP is in Tailscale range (100.64.0.0/10)
 */
function isTailscaleIP(ip: string): boolean {
  const parts = ip.split('.')
  if (parts.length !== 4) return false
  
  const firstOctet = parseInt(parts[0], 10)
  const secondOctet = parseInt(parts[1], 10)
  
  // Tailscale range: 100.64.0.0/10 = 100.64.0.0 to 100.127.255.255
  return firstOctet === 100 && secondOctet >= 64 && secondOctet <= 127
}

/**
 * Detects the network mode based on the current page location
 */
export function detectNetworkMode(): NetworkMode {
  const hostname = window.location.hostname

  // Check if it's localhost or 127.0.0.1 (LAN)
  if (hostname === 'localhost' || hostname === '127.0.0.1') {
    return 'lan'
  }

  // Check for private IP ranges (LAN)
  if (
    hostname.startsWith('192.168.') ||
    hostname.startsWith('10.') ||
    hostname.startsWith('172.1') ||
    hostname.startsWith('172.2') ||
    hostname.startsWith('172.3')
  ) {
    return 'lan'
  }

  // Check if it's a tailscale hostname or IP
  if (hostname.includes('.ts.net') || isTailscaleIP(hostname)) {
    return 'tailscale'
  }

  // Default to unknown
  return 'unknown'
}

/**
 * Gets the appropriate URL for a service based on network mode
 */
export function getServiceUrl(
  serviceUrl: string,
  urls: { lan?: string | null; tailscale?: string | null; public?: string | null } | null | undefined,
  networkMode: NetworkMode,
): string {
  if (!urls) return serviceUrl

  switch (networkMode) {
    case 'lan':
      return urls.lan || serviceUrl
    case 'tailscale':
      return urls.tailscale || serviceUrl
    default:
      return urls.public || serviceUrl
  }
}
