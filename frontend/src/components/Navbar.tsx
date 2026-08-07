import { NAV_ITEMS } from '../constants/navigation'
import { NavIcon } from './icons/NavIcon'
import type { AppSection } from '../types/app'

type NavbarProps = {
  isOpen: boolean
  activeSection: AppSection
  onSelect: (section: AppSection) => void
}

export function Navbar({
  isOpen,
  activeSection,
  onSelect,
}: NavbarProps): JSX.Element {
  return (
    <aside className={`sidebar ${isOpen ? 'is-expanded' : 'is-hidden'}`}>
      <nav className="sidebar-nav" aria-label="Primary">
        <ul className="sidebar-list">
          {NAV_ITEMS.map((item) => (
            <li key={item.key}>
              <button
                className={`sidebar-link ${activeSection === item.key ? 'is-active' : ''}`}
                type="button"
                onClick={() => onSelect(item.key)}
                title={item.label}
              >
                <NavIcon name={item.icon} />
                <span>{item.label}</span>
              </button>
            </li>
          ))}
        </ul>
      </nav>
    </aside>
  )
}
