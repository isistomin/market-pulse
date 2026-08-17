import { useState } from 'react'

import { number, percent, signClass } from '../format.js'

const COLUMNS = [
  { key: 'id', label: 'Instrument', render: (i) => i.id, numeric: false },
  { key: 'close', label: 'Close', render: (i) => number(i.close) },
  { key: 'daily_return', label: 'Return 1d', render: (i) => percent(i.daily_return), signed: true },
  { key: 'volatility_20d', label: 'Vol 20d', render: (i) => percent(i.volatility_20d) },
  { key: 'volatility_60d', label: 'Vol 60d', render: (i) => percent(i.volatility_60d) },
  { key: 'drawdown', label: 'Drawdown', render: (i) => percent(i.drawdown), signed: true },
  { key: 'excess_return', label: 'vs benchmark', render: (i) => percent(i.excess_return), signed: true },
]

function compare(a, b, key) {
  const left = a[key]
  const right = b[key]
  if (left === null || left === undefined) return 1
  if (right === null || right === undefined) return -1
  return typeof left === 'string' ? left.localeCompare(right) : left - right
}

export default function InstrumentTable({ instruments, selectedId, onSelect }) {
  const [sortKey, setSortKey] = useState('id')
  const [ascending, setAscending] = useState(true)

  const sorted = [...instruments].sort((a, b) => {
    const result = compare(a, b, sortKey)
    return ascending ? result : -result
  })

  function toggle(key) {
    if (key === sortKey) {
      setAscending(!ascending)
      return
    }
    setSortKey(key)
    setAscending(true)
  }

  return (
    <table className="instruments">
      <thead>
        <tr>
          {COLUMNS.map((column) => (
            <th
              key={column.key}
              onClick={() => toggle(column.key)}
              className={sortKey === column.key ? 'sorted' : ''}
            >
              {column.label}
              {sortKey === column.key ? (ascending ? ' ▲' : ' ▼') : ''}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {sorted.map((instrument) => (
          <tr
            key={instrument.id}
            onClick={() => onSelect(instrument.id)}
            className={instrument.id === selectedId ? 'selected' : ''}
          >
            {COLUMNS.map((column) => (
              <td
                key={column.key}
                className={column.signed ? signClass(instrument[column.key]) : ''}
              >
                {column.render(instrument)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}
