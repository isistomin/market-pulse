export function percent(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

export function number(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—'
  return value.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

export function signClass(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return ''
  if (value > 0) return 'positive'
  if (value < 0) return 'negative'
  return ''
}
