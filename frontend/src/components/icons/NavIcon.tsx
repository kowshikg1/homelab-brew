import type { IconName } from '../../types/navigation'

type NavIconProps = {
  name: IconName
}

export function NavIcon({ name }: NavIconProps): JSX.Element {
  if (name === 'services') {
    return (
      <svg viewBox="0 0 24 24" className="nav-icon" aria-hidden="true">
        <rect x="3" y="4" width="8" height="7" rx="1" />
        <rect x="13" y="4" width="8" height="7" rx="1" />
        <rect x="3" y="13" width="8" height="7" rx="1" />
        <rect x="13" y="13" width="8" height="7" rx="1" />
      </svg>
    )
  }

  if (name === 'ingestions') {
    return (
      <svg viewBox="0 0 24 24" className="nav-icon" aria-hidden="true">
        <path d="M12 3v14" />
        <path d="m7 12 5 5 5-5" />
        <path d="M4 20h16" />
      </svg>
    )
  }

  if (name === 'logs') {
    return (
      <svg viewBox="0 0 24 24" className="nav-icon" aria-hidden="true">
        <rect x="4" y="3" width="16" height="18" rx="2" />
        <path d="M8 8h8" />
        <path d="M8 12h8" />
        <path d="M8 16h5" />
      </svg>
    )
  }

  return (
    <svg viewBox="0 0 24 24" className="nav-icon" aria-hidden="true">
      <path d="M4 18v-5a4 4 0 0 1 4-4h8" />
      <path d="m16 4 4 5-4 5" />
      <path d="M4 6h6" />
      <path d="M4 10h4" />
    </svg>
  )
}
