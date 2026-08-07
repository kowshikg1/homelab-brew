import { useMemo } from 'react'
import type { ThemeMode } from '../types/app'
import { detectNetworkMode } from '../utils/networkUtils'
import { ThemeIcon } from './icons/ThemeIcon'

type TopBarProps = {
  theme: ThemeMode
  onThemeToggle: () => void
  onSidebarToggle: () => void
}

export function TopBar({
  theme,
  onThemeToggle,
  onSidebarToggle,
}: TopBarProps): JSX.Element {
  const networkMode = useMemo(() => detectNetworkMode(), [])
  const badgeText = `Network: ${networkMode}`

  return (
    <header className="topbar">
      <div className="topbar-left">
        <button
          className="icon-btn"
          type="button"
          aria-label="Toggle sidebar"
          onClick={onSidebarToggle}
        >
          <span className="icon icon-menu" aria-hidden="true" />
        </button>
        <h1>Homelab Brew</h1>
      </div>
      <div className="topbar-right">
        <span className={`network-badge mode-${networkMode}`}>{badgeText}</span>
        <button
          className="theme-toggle"
          type="button"
          aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
          onClick={onThemeToggle}
        >
          <ThemeIcon theme={theme} />
        </button>
      </div>
    </header>
  )
}
