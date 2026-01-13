import { useState, useEffect } from 'react';
import { Database, Activity, AlertCircle } from 'lucide-react';
import { SyncMonitor } from './components/SyncMonitor';
import { AccountLookup } from './components/AccountLookup';
import { TransactionExplorer } from './components/TransactionExplorer';
import { ChangeStreamFeed } from './components/ChangeStreamFeed';
import { PerformanceDashboard } from './components/PerformanceDashboard';
import { LoadTestPanel } from './components/LoadTestPanel';
import { healthApi } from './services/api';

function App() {
  const [selectedAccount, setSelectedAccount] = useState(null);
  const [apiHealthy, setApiHealthy] = useState(null);
  const [activeTab, setActiveTab] = useState('overview');

  // Check API health on mount
  useEffect(() => {
    async function checkHealth() {
      try {
        const health = await healthApi.check();
        setApiHealthy(health.status === 'healthy');
      } catch (e) {
        setApiHealthy(false);
      }
    }
    checkHealth();
    const interval = setInterval(checkHealth, 30000);
    return () => clearInterval(interval);
  }, []);

  const tabs = [
    { id: 'overview', label: 'Overview', icon: Activity },
    { id: 'transactions', label: 'Transactions', icon: Database },
    { id: 'performance', label: 'Performance', icon: Activity },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-mongodb-darkgreen text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <div className="w-10 h-10 rounded-lg bg-mongodb-green flex items-center justify-center">
                  <Database className="w-6 h-6 text-mongodb-darkgreen" />
                </div>
                <div>
                  <h1 className="text-xl font-bold">MongoDB Atlas ODL</h1>
                  <p className="text-xs text-gray-400">Banking Demo Dashboard</p>
                </div>
              </div>
            </div>

            <div className="flex items-center gap-4">
              {/* API Status */}
              <div className="flex items-center gap-2 text-sm">
                <span
                  className={`w-2.5 h-2.5 rounded-full ${
                    apiHealthy === null
                      ? 'bg-gray-400'
                      : apiHealthy
                      ? 'bg-green-400 status-pulse'
                      : 'bg-red-400'
                  }`}
                />
                <span className="text-gray-300">
                  API: {apiHealthy === null ? 'Checking...' : apiHealthy ? 'Connected' : 'Disconnected'}
                </span>
              </div>

              {/* SLA Targets */}
              <div className="text-xs text-gray-400 border-l border-gray-600 pl-4">
                Target SLA: 500 QPS | &lt;100ms p95
              </div>
            </div>
          </div>

          {/* Navigation tabs */}
          <div className="flex gap-1 mt-4">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-4 py-2 rounded-t-lg text-sm font-medium transition-colors ${
                  activeTab === tab.id
                    ? 'bg-gray-50 text-mongodb-darkgreen'
                    : 'text-gray-300 hover:text-white hover:bg-mongodb-slate'
                }`}
              >
                <tab.icon className="w-4 h-4" />
                {tab.label}
              </button>
            ))}
          </div>
        </div>
      </header>

      {/* API Warning */}
      {apiHealthy === false && (
        <div className="bg-red-50 border-b border-red-200 px-4 py-3">
          <div className="max-w-7xl mx-auto flex items-center gap-2 text-red-700">
            <AlertCircle className="w-5 h-5" />
            <span className="text-sm font-medium">
              API is not responding. Please ensure the backend is running at{' '}
              {import.meta.env.VITE_API_URL || 'http://localhost:8000'}
            </span>
          </div>
        </div>
      )}

      {/* Main content */}
      <main className="max-w-7xl mx-auto px-4 py-6">
        {activeTab === 'overview' && (
          <div className="space-y-6">
            {/* Sync Monitor - Full width */}
            <SyncMonitor />

            {/* Two column layout */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Left column - Account Lookup */}
              <div className="lg:col-span-1">
                <AccountLookup onAccountSelect={setSelectedAccount} />
              </div>

              {/* Right column - Change Stream Feed */}
              <div className="lg:col-span-2">
                <ChangeStreamFeed accountNumber={selectedAccount} />
              </div>
            </div>

            {/* Performance and Load Test */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <PerformanceDashboard />
              <LoadTestPanel />
            </div>
          </div>
        )}

        {activeTab === 'transactions' && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Left column - Account Lookup */}
              <div className="lg:col-span-1">
                <AccountLookup onAccountSelect={setSelectedAccount} />
              </div>

              {/* Right column - Transaction Explorer */}
              <div className="lg:col-span-2">
                <TransactionExplorer accountNumber={selectedAccount} />
              </div>
            </div>
          </div>
        )}

        {activeTab === 'performance' && (
          <div className="space-y-6">
            <PerformanceDashboard />
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <LoadTestPanel />
              <ChangeStreamFeed accountNumber={selectedAccount} />
            </div>
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="bg-white border-t mt-auto">
        <div className="max-w-7xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between text-sm text-gray-500">
            <div className="flex items-center gap-2">
              <span>MongoDB Atlas ODL Demo</span>
              <span className="text-gray-300">|</span>
              <span>Competing against Databricks Lakebase</span>
            </div>
            <div className="flex items-center gap-4">
              <span>Architecture: Databricks Lakehouse → Spark Streaming → MongoDB Atlas</span>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
}

export default App;
