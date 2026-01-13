import { useState, useEffect, useRef } from 'react';
import { Play, Square, Settings, CheckCircle, XCircle, AlertTriangle } from 'lucide-react';
import { Card } from './common/Card';
import { Badge } from './common/Badge';
import { LoadingSpinner } from './common/LoadingSpinner';
import { accountsApi, transactionsApi, searchApi } from '../services/api';

export function LoadTestPanel() {
  const [config, setConfig] = useState({
    concurrentUsers: 10,
    durationSeconds: 30,
    targetQps: 500,
  });

  const [isRunning, setIsRunning] = useState(false);
  const [results, setResults] = useState(null);
  const [liveStats, setLiveStats] = useState({
    requests: 0,
    successful: 0,
    failed: 0,
    latencies: [],
    startTime: null,
  });
  const [accounts, setAccounts] = useState([]);

  const abortControllerRef = useRef(null);
  const statsRef = useRef(liveStats);

  // Load accounts for testing
  useEffect(() => {
    async function loadAccounts() {
      try {
        const accountList = await accountsApi.list(50);
        setAccounts(accountList);
      } catch (e) {
        console.error('Failed to load accounts:', e);
      }
    }
    loadAccounts();
  }, []);

  // Keep statsRef in sync
  useEffect(() => {
    statsRef.current = liveStats;
  }, [liveStats]);

  const calculatePercentile = (arr, p) => {
    if (arr.length === 0) return 0;
    const sorted = [...arr].sort((a, b) => a - b);
    const index = Math.ceil(p * sorted.length) - 1;
    return sorted[Math.max(0, index)];
  };

  const runLoadTest = async () => {
    if (accounts.length === 0) {
      alert('No accounts available for testing');
      return;
    }

    setIsRunning(true);
    setResults(null);
    setLiveStats({
      requests: 0,
      successful: 0,
      failed: 0,
      latencies: [],
      startTime: Date.now(),
    });

    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    const startTime = Date.now();
    const endTime = startTime + config.durationSeconds * 1000;
    const delayBetweenRequests = 1000 / (config.targetQps / config.concurrentUsers);

    // Request types with weights
    const requestTypes = [
      { type: 'transactions', weight: 3 },
      { type: 'balance', weight: 2 },
      { type: 'search', weight: 1 },
    ];

    const totalWeight = requestTypes.reduce((sum, r) => sum + r.weight, 0);

    const selectRequestType = () => {
      let random = Math.random() * totalWeight;
      for (const req of requestTypes) {
        random -= req.weight;
        if (random <= 0) return req.type;
      }
      return requestTypes[0].type;
    };

    const makeRequest = async () => {
      if (signal.aborted || Date.now() >= endTime) return;

      const account = accounts[Math.floor(Math.random() * accounts.length)];
      const requestType = selectRequestType();
      const requestStart = performance.now();

      try {
        switch (requestType) {
          case 'transactions':
            await transactionsApi.list(account, { limit: 20 });
            break;
          case 'balance':
            await accountsApi.getBalance(account);
            break;
          case 'search':
            const searchTerms = ['grab', 'fairprice', 'netflix', 'singtel', 'dbs'];
            const term = searchTerms[Math.floor(Math.random() * searchTerms.length)];
            await searchApi.search(account, term, { limit: 10 });
            break;
        }

        const latency = performance.now() - requestStart;
        setLiveStats((prev) => ({
          ...prev,
          requests: prev.requests + 1,
          successful: prev.successful + 1,
          latencies: [...prev.latencies.slice(-999), latency],
        }));
      } catch (e) {
        if (!signal.aborted) {
          setLiveStats((prev) => ({
            ...prev,
            requests: prev.requests + 1,
            failed: prev.failed + 1,
          }));
        }
      }
    };

    // Create worker functions
    const workers = [];
    for (let i = 0; i < config.concurrentUsers; i++) {
      const worker = async () => {
        while (!signal.aborted && Date.now() < endTime) {
          await makeRequest();
          await new Promise((resolve) => setTimeout(resolve, delayBetweenRequests));
        }
      };
      workers.push(worker());
    }

    // Wait for all workers to complete
    await Promise.all(workers);

    // Calculate final results
    const stats = statsRef.current;
    const actualDuration = (Date.now() - startTime) / 1000;
    const actualQps = stats.requests / actualDuration;
    const avgLatency =
      stats.latencies.length > 0
        ? stats.latencies.reduce((a, b) => a + b, 0) / stats.latencies.length
        : 0;

    const finalResults = {
      totalRequests: stats.requests,
      successfulRequests: stats.successful,
      failedRequests: stats.failed,
      averageLatencyMs: avgLatency,
      p50LatencyMs: calculatePercentile(stats.latencies, 0.5),
      p95LatencyMs: calculatePercentile(stats.latencies, 0.95),
      p99LatencyMs: calculatePercentile(stats.latencies, 0.99),
      actualQps,
      durationSeconds: actualDuration,
      slaPassed: actualQps >= config.targetQps * 0.9 && calculatePercentile(stats.latencies, 0.95) < 100,
      timestamp: new Date().toISOString(),
    };

    setResults(finalResults);
    setIsRunning(false);
  };

  const stopTest = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    setIsRunning(false);
  };

  const currentQps = liveStats.startTime
    ? liveStats.requests / ((Date.now() - liveStats.startTime) / 1000)
    : 0;

  const currentP95 = calculatePercentile(liveStats.latencies, 0.95);

  return (
    <Card
      title="Load Test Panel"
      subtitle="Test API performance against SLA targets"
      action={
        isRunning ? (
          <button
            onClick={stopTest}
            className="flex items-center gap-1.5 px-4 py-2 bg-red-500 text-white text-sm font-medium rounded-lg hover:bg-red-600"
          >
            <Square className="w-4 h-4" />
            Stop Test
          </button>
        ) : (
          <button
            onClick={runLoadTest}
            disabled={accounts.length === 0}
            className="flex items-center gap-1.5 px-4 py-2 bg-mongodb-green text-white text-sm font-medium rounded-lg hover:bg-mongodb-leaf disabled:opacity-50"
          >
            <Play className="w-4 h-4" />
            Start Test
          </button>
        )
      }
    >
      <div className="space-y-4">
        {/* Configuration */}
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-3">
            <Settings className="w-4 h-4 text-gray-500 dark:text-gray-400" />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-200">Test Configuration</span>
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">
                Concurrent Users
              </label>
              <input
                type="number"
                value={config.concurrentUsers}
                onChange={(e) =>
                  setConfig((c) => ({
                    ...c,
                    concurrentUsers: Math.min(100, Math.max(1, parseInt(e.target.value) || 1)),
                  }))
                }
                disabled={isRunning}
                className="w-full px-3 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm disabled:bg-gray-100"
                min="1"
                max="100"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">
                Duration (seconds)
              </label>
              <input
                type="number"
                value={config.durationSeconds}
                onChange={(e) =>
                  setConfig((c) => ({
                    ...c,
                    durationSeconds: Math.min(300, Math.max(5, parseInt(e.target.value) || 30)),
                  }))
                }
                disabled={isRunning}
                className="w-full px-3 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm disabled:bg-gray-100"
                min="5"
                max="300"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Target QPS</label>
              <input
                type="number"
                value={config.targetQps}
                onChange={(e) =>
                  setConfig((c) => ({
                    ...c,
                    targetQps: Math.min(2000, Math.max(10, parseInt(e.target.value) || 500)),
                  }))
                }
                disabled={isRunning}
                className="w-full px-3 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm disabled:bg-gray-100"
                min="10"
                max="2000"
              />
            </div>
          </div>
        </div>

        {/* Live stats during test */}
        {isRunning && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 animate-fade-in">
            <div className="flex items-center gap-2 mb-3">
              <LoadingSpinner size="sm" />
              <span className="text-sm font-medium text-blue-800">Test in progress...</span>
            </div>
            <div className="grid grid-cols-4 gap-4">
              <div>
                <div className="text-xs text-blue-600">Requests</div>
                <div className="text-2xl font-bold text-blue-800">{liveStats.requests}</div>
              </div>
              <div>
                <div className="text-xs text-blue-600">Current QPS</div>
                <div className="text-2xl font-bold text-blue-800">{currentQps.toFixed(0)}</div>
              </div>
              <div>
                <div className="text-xs text-blue-600">P95 Latency</div>
                <div className="text-2xl font-bold text-blue-800">{currentP95.toFixed(0)}ms</div>
              </div>
              <div>
                <div className="text-xs text-blue-600">Failed</div>
                <div className="text-2xl font-bold text-red-600">{liveStats.failed}</div>
              </div>
            </div>
          </div>
        )}

        {/* Results */}
        {results && (
          <div
            className={`rounded-lg p-4 border ${
              results.slaPassed
                ? 'bg-green-50 border-green-200'
                : 'bg-red-50 border-red-200'
            }`}
          >
            <div className="flex items-center gap-2 mb-4">
              {results.slaPassed ? (
                <CheckCircle className="w-6 h-6 text-green-500" />
              ) : (
                <XCircle className="w-6 h-6 text-red-500" />
              )}
              <span
                className={`text-lg font-semibold ${
                  results.slaPassed ? 'text-green-800' : 'text-red-800'
                }`}
              >
                Test {results.slaPassed ? 'PASSED' : 'FAILED'}
              </span>
            </div>

            <div className="grid grid-cols-4 gap-4 mb-4">
              <div>
                <div className="text-xs text-gray-500 dark:text-gray-400">Total Requests</div>
                <div className="text-xl font-bold text-gray-900 dark:text-white">{results.totalRequests}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500 dark:text-gray-400">Actual QPS</div>
                <div className="text-xl font-bold text-gray-900 dark:text-white">
                  {results.actualQps.toFixed(1)}
                  {results.actualQps >= config.targetQps * 0.9 ? (
                    <CheckCircle className="inline w-4 h-4 ml-1 text-green-500" />
                  ) : (
                    <AlertTriangle className="inline w-4 h-4 ml-1 text-yellow-500" />
                  )}
                </div>
              </div>
              <div>
                <div className="text-xs text-gray-500 dark:text-gray-400">P95 Latency</div>
                <div className="text-xl font-bold text-gray-900 dark:text-white">
                  {results.p95LatencyMs.toFixed(0)}ms
                  {results.p95LatencyMs < 100 ? (
                    <CheckCircle className="inline w-4 h-4 ml-1 text-green-500" />
                  ) : (
                    <XCircle className="inline w-4 h-4 ml-1 text-red-500" />
                  )}
                </div>
              </div>
              <div>
                <div className="text-xs text-gray-500 dark:text-gray-400">Error Rate</div>
                <div className="text-xl font-bold text-gray-900 dark:text-white">
                  {((results.failedRequests / results.totalRequests) * 100).toFixed(2)}%
                </div>
              </div>
            </div>

            <div className="grid grid-cols-4 gap-4 text-sm border-t pt-3">
              <div>
                <span className="text-gray-500 dark:text-gray-400">Avg Latency:</span>{' '}
                <span className="font-medium">{results.averageLatencyMs.toFixed(1)}ms</span>
              </div>
              <div>
                <span className="text-gray-500 dark:text-gray-400">P50:</span>{' '}
                <span className="font-medium">{results.p50LatencyMs.toFixed(1)}ms</span>
              </div>
              <div>
                <span className="text-gray-500 dark:text-gray-400">P99:</span>{' '}
                <span className="font-medium">{results.p99LatencyMs.toFixed(1)}ms</span>
              </div>
              <div>
                <span className="text-gray-500 dark:text-gray-400">Duration:</span>{' '}
                <span className="font-medium">{results.durationSeconds.toFixed(1)}s</span>
              </div>
            </div>
          </div>
        )}

        {/* Help text */}
        {!isRunning && !results && (
          <div className="text-sm text-gray-500 dark:text-gray-400 text-center py-4">
            Configure test parameters and click "Start Test" to run a load test.
            <br />
            Target SLA: {config.targetQps} QPS, &lt;100ms p95 latency
          </div>
        )}
      </div>
    </Card>
  );
}

export default LoadTestPanel;
