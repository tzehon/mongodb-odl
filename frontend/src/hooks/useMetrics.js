import { useState, useEffect, useCallback, useRef } from 'react';
import { metricsApi } from '../services/api';

/**
 * Custom hook for fetching and polling API metrics
 */
export function useMetrics(pollInterval = 2000) {
  const [metrics, setMetrics] = useState(null);
  const [history, setHistory] = useState([]);
  const [syncStatus, setSyncStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const intervalRef = useRef(null);

  const fetchMetrics = useCallback(async () => {
    try {
      const [metricsData, historyData, syncData] = await Promise.all([
        metricsApi.get(),
        metricsApi.getHistory(5),
        metricsApi.getSyncStatus(),
      ]);

      setMetrics(metricsData);
      setHistory(historyData.dataPoints || []);
      setSyncStatus(syncData);
      setError(null);
    } catch (e) {
      console.error('Failed to fetch metrics:', e);
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  const resetMetrics = useCallback(async () => {
    try {
      await metricsApi.reset();
      await fetchMetrics();
    } catch (e) {
      console.error('Failed to reset metrics:', e);
      setError(e.message);
    }
  }, [fetchMetrics]);

  useEffect(() => {
    fetchMetrics();

    intervalRef.current = setInterval(fetchMetrics, pollInterval);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [fetchMetrics, pollInterval]);

  return {
    metrics,
    history,
    syncStatus,
    loading,
    error,
    refresh: fetchMetrics,
    reset: resetMetrics,
  };
}

export default useMetrics;
