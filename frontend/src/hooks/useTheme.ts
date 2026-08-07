import { useEffect, useState } from 'react'

import type { ThemeMode } from '../types/app'

export function useTheme(): {
  theme: ThemeMode
  toggleTheme: () => void
} {
  const [theme, setTheme] = useState<ThemeMode>('dark')

  useEffect(() => {
    const savedTheme = window.localStorage.getItem('homelab-theme')
    const nextTheme = savedTheme === 'light' ? 'light' : 'dark'
    setTheme(nextTheme)
    document.documentElement.setAttribute('data-theme', nextTheme)
  }, [])

  const toggleTheme = () => {
    const nextTheme = theme === 'dark' ? 'light' : 'dark'
    setTheme(nextTheme)
    window.localStorage.setItem('homelab-theme', nextTheme)
    document.documentElement.setAttribute('data-theme', nextTheme)
  }

  return { theme, toggleTheme }
}
