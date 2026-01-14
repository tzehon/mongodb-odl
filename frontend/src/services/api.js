/**
 * API service for communicating with the ODL Banking API
 */

const API_BASE = import.meta.env.VITE_API_URL || '';

class ApiError extends Error {
  constructor(message, status, data) {
    super(message);
    this.status = status;
    this.data = data;
  }
}

async function fetchApi(endpoint, options = {}) {
  const url = `${API_BASE}${endpoint}`;

  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new ApiError(
      error.detail || `API error: ${response.status}`,
      response.status,
      error
    );
  }

  return response.json();
}

// Account endpoints
export const accountsApi = {
  list: (limit = 100) => fetchApi(`/api/v1/accounts?limit=${limit}`),

  getLatestStatement: (accountNumber) =>
    fetchApi(`/api/v1/accounts/${accountNumber}/statements/latest`),

  getStatements: (accountNumber, startDate, endDate) => {
    const params = new URLSearchParams();
    if (startDate) params.set('start_date', startDate.toISOString());
    if (endDate) params.set('end_date', endDate.toISOString());
    return fetchApi(`/api/v1/accounts/${accountNumber}/statements?${params}`);
  },

  getBalance: (accountNumber) =>
    fetchApi(`/api/v1/accounts/${accountNumber}/balance`),

  getSummary: (accountNumber) =>
    fetchApi(`/api/v1/accounts/${accountNumber}/summary`),
};

// Transaction endpoints
export const transactionsApi = {
  list: (accountNumber, options = {}) => {
    const params = new URLSearchParams();
    if (options.limit) params.set('limit', options.limit);
    if (options.offset) params.set('offset', options.offset);
    if (options.startDate) params.set('start_date', options.startDate.toISOString());
    if (options.endDate) params.set('end_date', options.endDate.toISOString());
    if (options.minAmount) params.set('min_amount', options.minAmount);
    if (options.maxAmount) params.set('max_amount', options.maxAmount);
    if (options.category) params.set('category', options.category);
    if (options.type) params.set('type', options.type);
    return fetchApi(`/api/v1/accounts/${accountNumber}/transactions?${params}`);
  },

  getCategories: (accountNumber) =>
    fetchApi(`/api/v1/accounts/${accountNumber}/transactions/categories`),

  getMerchants: (accountNumber, limit = 50) =>
    fetchApi(`/api/v1/accounts/${accountNumber}/transactions/merchants?limit=${limit}`),

  getStats: (accountNumber, startDate, endDate) => {
    const params = new URLSearchParams();
    if (startDate) params.set('start_date', startDate.toISOString());
    if (endDate) params.set('end_date', endDate.toISOString());
    return fetchApi(`/api/v1/accounts/${accountNumber}/transactions/stats?${params}`);
  },

  getByCategory: (accountNumber) =>
    fetchApi(`/api/v1/accounts/${accountNumber}/transactions/by-category`),
};

// Search endpoints
export const searchApi = {
  search: (accountNumber, query, options = {}) => {
    const params = new URLSearchParams();
    params.set('q', query);
    if (options.limit) params.set('limit', options.limit);
    if (options.fuzzy !== undefined) params.set('fuzzy', options.fuzzy);
    return fetchApi(`/api/v1/accounts/${accountNumber}/transactions/search?${params}`);
  },

  autocomplete: (accountNumber, query, limit = 10) => {
    const params = new URLSearchParams();
    params.set('q', query);
    params.set('limit', limit);
    return fetchApi(`/api/v1/accounts/${accountNumber}/transactions/autocomplete?${params}`);
  },

  globalSearch: (query, limit = 20) => {
    const params = new URLSearchParams();
    params.set('q', query);
    params.set('limit', limit);
    return fetchApi(`/api/v1/search/global?${params}`);
  },
};

// Metrics endpoints
export const metricsApi = {
  get: () => fetchApi('/api/v1/metrics'),
  getHistory: (windowMinutes = 5) =>
    fetchApi(`/api/v1/metrics/history?window_minutes=${windowMinutes}`),
  reset: () => fetchApi('/api/v1/metrics/reset', { method: 'POST' }),
  getSyncStatus: () => fetchApi('/api/v1/sync/status'),
};

// Benchmark endpoints
export const benchmarkApi = {
  start: () => fetchApi('/api/v1/benchmark/start', { method: 'POST' }),
  stop: () => fetchApi('/api/v1/benchmark/stop', { method: 'POST' }),
  status: () => fetchApi('/api/v1/benchmark/status'),
};

// Health check
export const healthApi = {
  check: () => fetchApi('/health'),
};

// WebSocket status
export const wsApi = {
  getStatus: () => fetchApi('/ws/status'),
};

export { ApiError };
