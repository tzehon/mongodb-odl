import { useEffect, useState } from 'react';
import { Database, ArrowRight, Cloud, Server, Activity } from 'lucide-react';
import { Card } from './common/Card';
import { Badge } from './common/Badge';
import { useMetrics } from '../hooks/useMetrics';

export function SyncMonitor() {
  const { metrics, syncStatus, loading } = useMetrics(3000);
  const [docsPerSecond, setDocsPerSecond] = useState(0);
  const [prevCount, setPrevCount] = useState(null);

  useEffect(() => {
    if (syncStatus?.documentCount && prevCount !== null) {
      const diff = syncStatus.documentCount - prevCount;
      setDocsPerSecond(Math.max(0, diff / 3)); // 3 second interval
    }
    if (syncStatus?.documentCount) {
      setPrevCount(syncStatus.documentCount);
    }
  }, [syncStatus?.documentCount]);

  const syncLag = syncStatus?.syncLagSeconds;
  // Thresholds: < 60s = OK, 60-120s = Slow, > 120s = Idle
  const isStreamingIdle = syncLag > 120;
  const syncStatus_color = !syncLag ? 'gray' : syncLag < 60 ? 'green' : syncLag < 120 ? 'yellow' : 'gray';

  return (
    <Card title="Real-time Sync Monitor" subtitle="Data pipeline status">
      <div className="space-y-4">
        {/* Pipeline visualization */}
        <div className="flex items-center justify-between px-4 py-6 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
          {/* Databricks */}
          <div className="flex flex-col items-center">
            <div className="w-16 h-16 rounded-xl bg-gradient-to-br from-databricks-orange to-databricks-red flex items-center justify-center shadow-lg">
              <Database className="w-8 h-8 text-white" />
            </div>
            <span className="mt-2 text-xs font-medium text-gray-700 dark:text-gray-200">Databricks</span>
            <span className="text-xs text-gray-500 dark:text-gray-400">Lakehouse</span>
          </div>

          {/* Arrow with animation */}
          <div className="flex-1 mx-4 flex flex-col items-center gap-2">
            <span className="text-xs text-gray-500 dark:text-gray-400">Spark Streaming</span>
            <div className="w-full h-1 bg-gray-200 dark:bg-gray-600 rounded-full overflow-hidden">
              <div className="h-full w-1/3 bg-mongodb-green data-flow" />
            </div>
          </div>

          {/* MongoDB Atlas */}
          <div className="flex flex-col items-center">
            <div className="w-16 h-16 rounded-xl bg-gradient-to-br from-mongodb-green to-mongodb-leaf flex items-center justify-center shadow-lg">
              <Cloud className="w-8 h-8 text-white" />
            </div>
            <span className="mt-2 text-xs font-medium text-gray-700 dark:text-gray-200">MongoDB Atlas</span>
            <span className="text-xs text-gray-500 dark:text-gray-400">ODL</span>
          </div>

          {/* Arrow */}
          <div className="flex-1 mx-4 flex flex-col items-center gap-2">
            <span className="text-xs text-gray-500 dark:text-gray-400">API</span>
            <div className="w-full h-1 bg-gray-200 dark:bg-gray-600 rounded-full overflow-hidden">
              <div className="h-full w-full bg-mongodb-green opacity-50" />
            </div>
          </div>

          {/* Dashboard */}
          <div className="flex flex-col items-center">
            <div className="w-16 h-16 rounded-xl bg-gradient-to-br from-gray-700 to-gray-900 flex items-center justify-center shadow-lg">
              <Server className="w-8 h-8 text-white" />
            </div>
            <span className="mt-2 text-xs font-medium text-gray-700 dark:text-gray-200">Dashboard</span>
            <span className="text-xs text-gray-500 dark:text-gray-400">This UI</span>
          </div>
        </div>

        {/* Stats grid */}
        <div className="grid grid-cols-4 gap-4">
          {/* Document Count */}
          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
            <div className="flex items-center gap-2 text-gray-600 dark:text-gray-300 mb-1">
              <Database className="w-4 h-4" />
              <span className="text-xs font-medium">Documents</span>
            </div>
            <div className="text-2xl font-bold text-gray-900 dark:text-white">
              {loading ? '...' : (syncStatus?.documentCount || 0).toLocaleString()}
            </div>
          </div>

          {/* Docs per second */}
          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
            <div className="flex items-center gap-2 text-gray-600 dark:text-gray-300 mb-1">
              <Activity className="w-4 h-4" />
              <span className="text-xs font-medium">Docs/sec</span>
            </div>
            <div className="text-2xl font-bold text-gray-900 dark:text-white">
              {docsPerSecond.toFixed(1)}
            </div>
          </div>

          {/* Sync Lag */}
          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
            <div className="flex items-center gap-2 text-gray-600 dark:text-gray-300 mb-1">
              <ArrowRight className="w-4 h-4" />
              <span className="text-xs font-medium">Sync Lag</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-2xl font-bold text-gray-900 dark:text-white">
                {isStreamingIdle ? 'Idle' : syncLag != null ? `${syncLag.toFixed(0)}s` : 'N/A'}
              </span>
              <Badge
                variant={syncStatus_color === 'green' ? 'success' : syncStatus_color === 'yellow' ? 'warning' : 'default'}
                size="sm"
              >
                {isStreamingIdle ? 'No active stream' : syncStatus_color === 'green' ? 'OK' : syncStatus_color === 'yellow' ? 'Slow' : 'Unknown'}
              </Badge>
            </div>
          </div>

          {/* Connection Status */}
          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
            <div className="flex items-center gap-2 text-gray-600 dark:text-gray-300 mb-1">
              <Cloud className="w-4 h-4" />
              <span className="text-xs font-medium">Status</span>
            </div>
            <div className="flex items-center gap-2">
              <span className={`w-3 h-3 rounded-full ${syncStatus?.status === 'connected' ? 'bg-green-500 status-pulse' : 'bg-red-500'}`} />
              <span className="text-lg font-semibold text-gray-900 dark:text-white">
                {syncStatus?.status || 'Unknown'}
              </span>
            </div>
          </div>
        </div>

        {/* Last synced info */}
        {syncStatus?.lastSyncedDocument?.accountNumber && (
          <div className="text-xs text-gray-500 dark:text-gray-400 border-t pt-3">
            <span className="font-medium">Last synced:</span>{' '}
            Account {syncStatus.lastSyncedDocument.accountNumber} at{' '}
            {new Date(syncStatus.lastSyncedDocument.generatedAt).toLocaleString()}
            <span className="ml-2 text-gray-400">
              (source: {syncStatus.lastSyncedDocument.source})
            </span>
          </div>
        )}
      </div>
    </Card>
  );
}

export default SyncMonitor;
