export type NetworkMode = 'lan' | 'tailscale' | 'public' | 'unknown'

export type ApiTarget = {
  mode: NetworkMode
  baseUrl: string
}
