import { useEffect, useState } from 'react'

import { instrumentBenchmark, listInstruments } from './api.js'
import InstrumentTable from './components/InstrumentTable.jsx'
import PriceChart from './components/PriceChart.jsx'

const TABS = [
  { key: 'stock', label: 'Stocks' },
  { key: 'crypto', label: 'Crypto' },
]

export default function App() {
  const [tab, setTab] = useState('stock')
  const [instruments, setInstruments] = useState([])
  const [generatedAt, setGeneratedAt] = useState(null)
  const [selectedId, setSelectedId] = useState(null)
  const [detail, setDetail] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    listInstruments(tab)
      .then((payload) => {
        if (cancelled) return
        setInstruments(payload.instruments)
        setGeneratedAt(payload.generated_at)
        setSelectedId(payload.instruments[0]?.id ?? null)
      })
      .catch((problem) => !cancelled && setError(problem.message))
      .finally(() => !cancelled && setLoading(false))

    return () => {
      cancelled = true
    }
  }, [tab])

  useEffect(() => {
    if (!selectedId) {
      setDetail(null)
      return
    }
    let cancelled = false

    instrumentBenchmark(selectedId)
      .then((payload) => !cancelled && setDetail(payload))
      .catch(() => !cancelled && setDetail(null))

    return () => {
      cancelled = true
    }
  }, [selectedId])

  return (
    <main>
      <header>
        <h1>Market Pulse</h1>
        <p className="subtitle">
          A scheduled batch contour for equities and a 24/7 streaming one for crypto,
          reduced to a single set of metrics.
        </p>
        {generatedAt ? (
          <p className="stamp">Snapshot generated {generatedAt}</p>
        ) : null}
      </header>

      <nav className="tabs">
        {TABS.map((entry) => (
          <button
            key={entry.key}
            onClick={() => setTab(entry.key)}
            className={tab === entry.key ? 'active' : ''}
          >
            {entry.label}
          </button>
        ))}
      </nav>

      {error ? <p className="error">{error}</p> : null}
      {loading ? <p className="empty">Loading…</p> : null}

      {!loading && !error ? (
        <>
          <section>
            <InstrumentTable
              instruments={instruments}
              selectedId={selectedId}
              onSelect={setSelectedId}
            />
          </section>

          <section>
            <h2>
              {selectedId}
              {detail?.benchmark_id ? ` vs ${detail.benchmark_id}` : ''}
            </h2>
            <p className="hint">Both series rebased to 100 at the start of the window.</p>
            <PriceChart
              instrumentId={selectedId}
              benchmarkId={detail?.benchmark_id}
              series={detail?.series}
              benchmarkSeries={detail?.benchmark_series}
            />
          </section>
        </>
      ) : null}
    </main>
  )
}
