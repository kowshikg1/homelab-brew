import type { AppSection } from '../types/app'

type PlaceholderPanelProps = {
  section: AppSection
}

export function PlaceholderPanel({ section }: PlaceholderPanelProps): JSX.Element {
  return (
    <section className="placeholder-card">
      <h2>{section}</h2>
      <p>This section is reserved for the next phase.</p>
    </section>
  )
}
