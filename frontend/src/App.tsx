import { useState } from 'react'

import { Navbar } from './components/Navbar'
import { PlaceholderPanel } from './components/PlaceholderPanel'
import { TopBar } from './components/TopBar'
import { ServicesPanel } from './features/services/ServicesPanel'
import { useServices } from './features/services/useServices'
import { useTheme } from './hooks/useTheme'
import type { AppSection } from './types/app'

export function App(): JSX.Element {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false)
  const [activeSection, setActiveSection] =
    useState<AppSection>('Services')

  const { theme, toggleTheme } = useTheme()
  const { services, categories, loading, error, networkMode } = useServices()

  const handleSectionSelect = (section: AppSection) => {
    setActiveSection(section)
    if (window.innerWidth < 900) {
      setIsSidebarOpen(false)
    }
  }

  return (
    <div className="page-shell">
      <TopBar
        theme={theme}
        onThemeToggle={toggleTheme}
        onSidebarToggle={() => setIsSidebarOpen((prev) => !prev)}
      />

      {isSidebarOpen && (
        <button
          className="sidebar-backdrop"
          type="button"
          aria-label="Close sidebar"
          onClick={() => setIsSidebarOpen(false)}
        />
      )}

      <div className="layout">
        <Navbar
          isOpen={isSidebarOpen}
          activeSection={activeSection}
          onSelect={handleSectionSelect}
        />

        <main className="content-area">
          {activeSection === 'Services' ? (
            <ServicesPanel
              categories={categories}
              services={services}
              networkMode={networkMode}
              loading={loading}
              error={error}
            />
          ) : (
            <PlaceholderPanel section={activeSection} />
          )}
        </main>
      </div>
    </div>
  )
}
