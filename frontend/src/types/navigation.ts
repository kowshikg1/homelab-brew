import type { AppSection } from './app'

export type IconName = 'services' | 'ingestions' | 'logs' | 'scripts'

export type NavItem = {
  key: AppSection
  label: string
  icon: IconName
}
