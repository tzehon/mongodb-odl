import { useState } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';
import { Activity, Clock, CheckCircle, XCircle, RefreshCw } from 'lucide-react';
import { Card } from './common/Card';
import { Badge } from './common/Badge';
import { LoadingSpinner } from './common/LoadingSpinner';
import { useMetrics } from '../hooks/useMetrics';

export function PerformanceDashboard() {
  const { metrics, history, loading, error, reset } = useMetrics(2000);
  const [selectedMetric, setSelectedMetric] = useState('qps');

  const SLA_QPS = 500;
  const SLA_LATENCY = 100;

  const formatTime = (timestamp) => {
    if (!timestamp) return '';
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-SG', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  const chartData = history.map((point) => ({
    time: formatTime(point.timestamp),
    qps: point.qps?.toFixed(1) || 0,
    latency: point.avgLatencyMs?.toFixed(1) || 0,
    p95: point.p95LatencyMs?.toFixed(1) || 0,
  }));

  if (loading && !metrics) {
    return (
      <Card title="Performance Dashboard">
        <div className="flex items-center justify-center py-12">
          <LoadingSpinner size="lg" />
        </div>
      </Card>
    );
  }

  const qpsMet = metrics?.sla?.qpsMet ?? false;
  const latencyMet = metrics?.sla?.latencyMet ?? false;
  const overallPassed = metrics?.sla?.overallPassed ?? false;

  return (
    <Card
      title="Performance Dashboard"
      subtitle="Real-time SLA monitoring"
      action={
        <button
          onClick={reset}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50"
        >
          <RefreshCw className="w-4 h-4" />
          Reset
        </button>
      }
    >
      <div className="space-y-4">
        {/* SLA Status Banner */}
        <div
          className={`rounded-lg p-4 ${
            overallPassed
              ? 'bg-green-50 border border-green-200'
              : 'bg-red-50 border border-red-200'
          }`}
        >
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              {overallPassed ? (
                <CheckCircle className="w-8 h-8 text-green-500" />
              ) : (
                <XCircle className="w-8 h-8 text-red-500" />
              )}
              <div>
                <div
                  className={`font-semibold ${
                    overallPassed ? 'text-green-800' : 'text-red-800'
                  }`}
                >
                  SLA Status: {overallPassed ? 'PASSING' : 'NOT MET'}
                </div>
                <div
                  className={`text-sm ${
                    overallPassed ? 'text-green-600' : 'text-red-600'
                  }`}
                >
                  Target: {SLA_QPS} QPS, &lt;{SLA_LATENCY}ms p95 latency
                </div>
              </div>
            </div>
            <div className="flex gap-4">
              <Badge variant={qpsMet ? 'success' : 'error'} dot>
                QPS {qpsMet ? 'OK' : 'Low'}
              </Badge>
              <Badge variant={latencyMet ? 'success' : 'error'} dot>
                Latency {latencyMet ? 'OK' : 'High'}
              </Badge>
            </div>
          </div>
        </div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-4 gap-4">
          {/* Current QPS */}
          <div className="bg-gray-50 rounded-lg p-4">
            <div className="flex items-center gap-2 text-gray-600 mb-1">
              <Activity className="w-4 h-4" />
              <span className="text-xs font-medium">Current QPS</span>
            </div>
            <div className="text-3xl font-bold text-gray-900">
              {metrics?.currentQps?.toFixed(1) || '0'}
            </div>
            <div className="text-xs text-gray-500 mt-1">
              Target: {SLA_QPS}
            </div>
            <div className="mt-2 h-1 bg-gray-200 rounded-full overflow-hidden">
              <div
                className={`h-full transition-all ${
                  (metrics?.currentQps || 0) >= SLA_QPS ? 'bg-green-500' : 'bg-yellow-500'
                }`}
                style={{
                  width: `${Math.min(100, ((metrics?.currentQps || 0) / SLA_QPS) * 100)}%`,
                }}
              />
            </div>
          </div>

          {/* P50 Latency */}
          <div className="bg-gray-50 rounded-lg p-4">
            <div className="flex items-center gap-2 text-gray-600 mb-1">
              <Clock className="w-4 h-4" />
              <span className="text-xs font-medium">P50 Latency</span>
            </div>
            <div className="text-3xl font-bold text-gray-900">
              {metrics?.p50LatencyMs?.toFixed(0) || '0'}
              <span className="text-lg font-normal text-gray-500">ms</span>
            </div>
            <div className="text-xs text-gray-500 mt-1">Median response time</div>
          </div>

          {/* P95 Latency */}
          <div className="bg-gray-50 rounded-lg p-4">
            <div className="flex items-center gap-2 text-gray-600 mb-1">
              <Clock className="w-4 h-4" />
              <span className="text-xs font-medium">P95 Latency</span>
            </div>
            <div
              className={`text-3xl font-bold ${
                (metrics?.p95LatencyMs || 0) <= SLA_LATENCY
                  ? 'text-green-600'
                  : 'text-red-600'
              }`}
            >
              {metrics?.p95LatencyMs?.toFixed(0) || '0'}
              <span className="text-lg font-normal">ms</span>
            </div>
            <div className="text-xs text-gray-500 mt-1">
              Target: &lt;{SLA_LATENCY}ms
            </div>
            <div className="mt-2 h-1 bg-gray-200 rounded-full overflow-hidden">
              <div
                className={`h-full transition-all ${
                  (metrics?.p95LatencyMs || 0) <= SLA_LATENCY ? 'bg-green-500' : 'bg-red-500'
                }`}
                style={{
                  width: `${Math.min(100, ((SLA_LATENCY / Math.max(metrics?.p95LatencyMs || 1, 1)) * 100))}%`,
                }}
              />
            </div>
          </div>

          {/* P99 Latency */}
          <div className="bg-gray-50 rounded-lg p-4">
            <div className="flex items-center gap-2 text-gray-600 mb-1">
              <Clock className="w-4 h-4" />
              <span className="text-xs font-medium">P99 Latency</span>
            </div>
            <div className="text-3xl font-bold text-gray-900">
              {metrics?.p99LatencyMs?.toFixed(0) || '0'}
              <span className="text-lg font-normal text-gray-500">ms</span>
            </div>
            <div className="text-xs text-gray-500 mt-1">99th percentile</div>
          </div>
        </div>

        {/* Metric selector tabs */}
        <div className="flex gap-2 border-b">
          <button
            onClick={() => setSelectedMetric('qps')}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              selectedMetric === 'qps'
                ? 'border-mongodb-green text-mongodb-green'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            QPS
          </button>
          <button
            onClick={() => setSelectedMetric('latency')}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              selectedMetric === 'latency'
                ? 'border-mongodb-green text-mongodb-green'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            Latency
          </button>
        </div>

        {/* Chart */}
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="time" tick={{ fontSize: 10 }} stroke="#9CA3AF" />
              <YAxis tick={{ fontSize: 10 }} stroke="#9CA3AF" />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#fff',
                  border: '1px solid #E5E7EB',
                  borderRadius: '8px',
                  fontSize: '12px',
                }}
              />

              {selectedMetric === 'qps' ? (
                <>
                  <ReferenceLine
                    y={SLA_QPS}
                    stroke="#10B981"
                    strokeDasharray="5 5"
                    label={{ value: 'Target', fontSize: 10, fill: '#10B981' }}
                  />
                  <Line
                    type="monotone"
                    dataKey="qps"
                    stroke="#00ED64"
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4 }}
                  />
                </>
              ) : (
                <>
                  <ReferenceLine
                    y={SLA_LATENCY}
                    stroke="#EF4444"
                    strokeDasharray="5 5"
                    label={{ value: 'SLA', fontSize: 10, fill: '#EF4444' }}
                  />
                  <Line
                    type="monotone"
                    dataKey="latency"
                    name="Avg"
                    stroke="#3B82F6"
                    strokeWidth={2}
                    dot={false}
                  />
                  <Line
                    type="monotone"
                    dataKey="p95"
                    name="P95"
                    stroke="#F59E0B"
                    strokeWidth={2}
                    dot={false}
                  />
                </>
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Additional stats */}
        <div className="grid grid-cols-3 gap-4 text-sm border-t pt-4">
          <div>
            <span className="text-gray-500">Uptime:</span>{' '}
            <span className="font-medium">
              {Math.floor((metrics?.uptime || 0) / 60)}m {Math.floor((metrics?.uptime || 0) % 60)}s
            </span>
          </div>
          <div>
            <span className="text-gray-500">Error Rate:</span>{' '}
            <span className="font-medium">{metrics?.errorRate?.toFixed(2) || 0}%</span>
          </div>
          <div>
            <span className="text-gray-500">Connections:</span>{' '}
            <span className="font-medium">
              {metrics?.connectionPoolStats?.current || 0} active
            </span>
          </div>
        </div>
      </div>
    </Card>
  );
}

export default PerformanceDashboard;
