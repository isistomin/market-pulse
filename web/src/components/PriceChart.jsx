import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { number } from '../format.js'

/** Rebase both series to 100 at the first common date, so instruments with very
 *  different prices stay comparable on one axis. */
function rebase(series, benchmarkSeries) {
  if (!series.length) return []

  const benchmarkByDate = new Map(benchmarkSeries.map((point) => [point.date, point.close]))
  const base = series[0].close
  const benchmarkBase = benchmarkSeries.length ? benchmarkSeries[0].close : null

  return series.map((point) => {
    const benchmarkClose = benchmarkByDate.get(point.date)
    return {
      date: point.date,
      instrument: base ? (point.close / base) * 100 : null,
      benchmark:
        benchmarkBase && benchmarkClose ? (benchmarkClose / benchmarkBase) * 100 : null,
    }
  })
}

export default function PriceChart({ instrumentId, benchmarkId, series, benchmarkSeries }) {
  const data = rebase(series ?? [], benchmarkSeries ?? [])

  if (!data.length) {
    return <p className="empty">No history for {instrumentId} yet.</p>
  }

  return (
    <ResponsiveContainer width="100%" height={320}>
      <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#2a2f3a" />
        <XAxis dataKey="date" minTickGap={40} stroke="#8b93a7" />
        <YAxis domain={['auto', 'auto']} stroke="#8b93a7" />
        <Tooltip
          formatter={(value) => number(value)}
          contentStyle={{ background: '#151922', border: '1px solid #2a2f3a' }}
        />
        <Legend />
        <Line
          type="monotone"
          dataKey="instrument"
          name={instrumentId}
          stroke="#4c9aff"
          dot={false}
          strokeWidth={2}
        />
        {benchmarkId ? (
          <Line
            type="monotone"
            dataKey="benchmark"
            name={benchmarkId}
            stroke="#8b93a7"
            dot={false}
            strokeDasharray="4 4"
          />
        ) : null}
      </LineChart>
    </ResponsiveContainer>
  )
}
