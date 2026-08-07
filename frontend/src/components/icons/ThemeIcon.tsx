type ThemeIconProps = {
  theme: 'light' | 'dark'
}

export function ThemeIcon({ theme }: ThemeIconProps): JSX.Element {
  if (theme === 'dark') {
    return (
      <svg viewBox="0 0 24 24" className="theme-icon" aria-hidden="true">
        <circle cx="12" cy="12" r="5" />
        <path d="M12 1v6" />
        <path d="M12 17v6" />
        <path d="M4.22 4.22l4.24 4.24" />
        <path d="M15.54 15.54l4.24 4.24" />
        <path d="M1 12h6" />
        <path d="M17 12h6" />
        <path d="M4.22 19.78l4.24-4.24" />
        <path d="M15.54 8.46l4.24-4.24" />
      </svg>
    )
  }

  return (
    <svg viewBox="0 0 24 24" className="theme-icon" aria-hidden="true">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  )
}
