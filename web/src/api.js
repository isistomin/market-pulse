const BASE = import.meta.env.VITE_API_BASE ?? '/api'

async function get(path) {
  const response = await fetch(`${BASE}${path}`)
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? `request failed with ${response.status}`)
  }
  return response.json()
}

export const listInstruments = (type) =>
  get(`/instruments${type ? `?type=${type}` : ''}`)

export const instrumentMetrics = (id) =>
  get(`/instruments/${encodeURIComponent(id)}/metrics`)

export const instrumentBenchmark = (id) =>
  get(`/instruments/${encodeURIComponent(id)}/benchmark`)
