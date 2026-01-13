import { useState, useEffect, useRef } from 'react';
import { Radio, Pause, Play, Trash2, Filter, Wifi, WifiOff } from 'lucide-react';
import { Card } from './common/Card';
import { Badge } from './common/Badge';
import { useWebSocket } from '../hooks/useWebSocket';

export function ChangeStreamFeed({ accountNumber }) {
  const [filterAccount, setFilterAccount] = useState('');
  const [isPaused, setIsPaused] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const feedRef = useRef(null);

  const {
    isConnected,
    events,
    error,
    connectionId,
    clearEvents,
  } = useWebSocket(filterAccount || null);

  // Auto-scroll when new events arrive
  useEffect(() => {
    if (autoScroll && feedRef.current && events.length > 0) {
      feedRef.current.scrollTop = 0;
    }
  }, [events, autoScroll]);

  const displayEvents = isPaused ? events.slice(0, events.length) : events;

  const getOperationColor = (type) => {
    switch (type) {
      case 'insert':
        return 'success';
      case 'update':
      case 'replace':
        return 'info';
      case 'delete':
        return 'error';
      default:
        return 'default';
    }
  };

  const formatTime = (timestamp) => {
    return new Date(timestamp).toLocaleTimeString('en-SG', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  return (
    <Card
      title="Change Stream Feed (CDC)"
      subtitle={isConnected ? 'Real-time document changes' : 'Disconnected'}
      action={
        <div className="flex items-center gap-2">
          {/* Connection status */}
          <div className="flex items-center gap-1.5">
            {isConnected ? (
              <Wifi className="w-4 h-4 text-green-500" />
            ) : (
              <WifiOff className="w-4 h-4 text-red-500" />
            )}
            <span className="text-xs text-gray-500 dark:text-gray-400">
              {isConnected ? 'Connected' : 'Disconnected'}
            </span>
          </div>

          {/* Pause/Resume */}
          <button
            onClick={() => setIsPaused(!isPaused)}
            className={`p-1.5 rounded-lg border transition-colors ${
              isPaused
                ? 'bg-yellow-50 dark:bg-yellow-900/30 border-yellow-200 dark:border-yellow-700 text-yellow-700 dark:text-yellow-400'
                : 'border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 text-gray-600 dark:text-gray-300'
            }`}
            title={isPaused ? 'Resume' : 'Pause'}
          >
            {isPaused ? <Play className="w-4 h-4" /> : <Pause className="w-4 h-4" />}
          </button>

          {/* Clear */}
          <button
            onClick={clearEvents}
            className="p-1.5 border border-gray-200 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-600 text-gray-600 dark:text-gray-300"
            title="Clear events"
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      }
    >
      <div className="space-y-3">
        {/* Filter */}
        <div className="flex items-center gap-2">
          <Filter className="w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Filter by account number..."
            value={filterAccount}
            onChange={(e) => setFilterAccount(e.target.value)}
            className="flex-1 px-3 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-mongodb-green focus:border-transparent"
          />
          <label className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400">
            <input
              type="checkbox"
              checked={autoScroll}
              onChange={(e) => setAutoScroll(e.target.checked)}
              className="rounded border-gray-300 dark:border-gray-600 text-mongodb-green focus:ring-mongodb-green"
            />
            Auto-scroll
          </label>
        </div>

        {/* Error display */}
        {error && (
          <div className="bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300 px-3 py-2 rounded-lg text-sm">
            {error}
          </div>
        )}

        {/* Events feed */}
        <div
          ref={feedRef}
          className="h-80 overflow-y-auto space-y-2 bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3"
        >
          {displayEvents.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-gray-400">
              <Radio className="w-8 h-8 mb-2 animate-pulse" />
              <span className="text-sm">Waiting for changes...</span>
              <span className="text-xs mt-1">
                {isConnected
                  ? 'Events will appear here in real-time'
                  : 'Connecting to change stream...'}
              </span>
            </div>
          ) : (
            displayEvents.map((event) => (
              <div
                key={event.id}
                className="bg-white dark:bg-gray-800 rounded-lg p-3 shadow-sm border border-gray-100 dark:border-gray-700 animate-slide-in"
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <Badge variant={getOperationColor(event.operationType)} size="sm">
                      {event.operationType}
                    </Badge>
                    <span className="text-xs text-gray-500 dark:text-gray-400">
                      {formatTime(event.timestamp)}
                    </span>
                  </div>
                </div>

                {/* Document summary */}
                {event.fullDocument && (
                  <div className="text-sm">
                    <div className="flex items-center gap-2 text-gray-700 dark:text-gray-200">
                      <span className="font-medium">
                        {event.fullDocument.accountNumber}
                      </span>
                      <span className="text-xs text-gray-400">|</span>
                      <span className="text-gray-500 dark:text-gray-400">
                        {event.fullDocument.accountType}
                      </span>
                    </div>

                    {event.fullDocument.latestTransaction && (
                      <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                        Latest: {event.fullDocument.latestTransaction.description}
                        <span
                          className={
                            event.fullDocument.latestTransaction.type === 'credit'
                              ? 'text-emerald-600 ml-2'
                              : 'text-gray-700 dark:text-gray-200 ml-2'
                          }
                        >
                          {event.fullDocument.latestTransaction.type === 'credit'
                            ? '+'
                            : '-'}
                          ${event.fullDocument.latestTransaction.amount?.toFixed(2)}
                        </span>
                      </div>
                    )}

                    <div className="mt-1 text-xs text-gray-400">
                      {event.fullDocument.transactionCount} transactions |
                      Balance: ${event.fullDocument.closingBalance?.toFixed(2)}
                    </div>
                  </div>
                )}

                {/* Update description */}
                {event.updateDescription &&
                  event.updateDescription.updatedFields?.length > 0 && (
                    <div className="mt-2 text-xs text-gray-500 dark:text-gray-400">
                      Updated fields:{' '}
                      {event.updateDescription.updatedFields.join(', ')}
                    </div>
                  )}

                {/* Document key */}
                {event.documentKey && (
                  <div className="mt-1 text-xs text-gray-400 font-mono">
                    ID: {JSON.stringify(event.documentKey._id || event.documentKey)}
                  </div>
                )}
              </div>
            ))
          )}
        </div>

        {/* Stats */}
        <div className="flex items-center justify-between text-xs text-gray-500 dark:text-gray-400 border-t pt-2">
          <span>
            {events.length} events captured
            {isPaused && ' (paused)'}
          </span>
          {connectionId && (
            <span className="font-mono text-gray-400">
              Connection: {connectionId.slice(0, 8)}...
            </span>
          )}
        </div>
      </div>
    </Card>
  );
}

export default ChangeStreamFeed;
