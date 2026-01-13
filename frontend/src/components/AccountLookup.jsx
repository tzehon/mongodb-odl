import { useState, useEffect } from 'react';
import { Search, User, CreditCard, Building2, DollarSign, TrendingUp, TrendingDown } from 'lucide-react';
import { Card } from './common/Card';
import { Badge } from './common/Badge';
import { LoadingSpinner } from './common/LoadingSpinner';
import { accountsApi, transactionsApi } from '../services/api';

export function AccountLookup({ onAccountSelect }) {
  const [accounts, setAccounts] = useState([]);
  const [selectedAccount, setSelectedAccount] = useState('');
  const [accountData, setAccountData] = useState(null);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Load account list on mount
  useEffect(() => {
    async function loadAccounts() {
      try {
        const accountList = await accountsApi.list(100);
        setAccounts(accountList);
        if (accountList.length > 0 && !selectedAccount) {
          setSelectedAccount(accountList[0]);
        }
      } catch (e) {
        console.error('Failed to load accounts:', e);
      }
    }
    loadAccounts();
  }, []);

  // Load account details when selection changes
  useEffect(() => {
    if (!selectedAccount) return;

    async function loadAccountData() {
      setLoading(true);
      setError(null);
      try {
        const [summary, statsData] = await Promise.all([
          accountsApi.getSummary(selectedAccount),
          transactionsApi.getStats(selectedAccount),
        ]);
        setAccountData(summary);
        setStats(statsData);
        onAccountSelect?.(selectedAccount);
      } catch (e) {
        console.error('Failed to load account data:', e);
        setError(e.message);
      } finally {
        setLoading(false);
      }
    }
    loadAccountData();
  }, [selectedAccount, onAccountSelect]);

  const formatCurrency = (amount) => {
    return new Intl.NumberFormat('en-SG', {
      style: 'currency',
      currency: accountData?.currency || 'SGD',
    }).format(amount || 0);
  };

  return (
    <Card title="Account Lookup" subtitle="Select and view account details">
      <div className="space-y-4">
        {/* Account selector */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <select
            value={selectedAccount}
            onChange={(e) => setSelectedAccount(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-sm focus:outline-none focus:ring-2 focus:ring-mongodb-green focus:border-transparent"
          >
            <option value="">Select an account...</option>
            {accounts.map((acc) => (
              <option key={acc} value={acc}>
                {acc}
              </option>
            ))}
          </select>
        </div>

        {loading && (
          <div className="flex items-center justify-center py-8">
            <LoadingSpinner size="lg" />
          </div>
        )}

        {error && (
          <div className="bg-red-50 text-red-700 px-4 py-3 rounded-lg text-sm">
            {error}
          </div>
        )}

        {accountData && !loading && (
          <>
            {/* Balance display */}
            <div className="bg-gradient-to-br from-mongodb-darkgreen to-mongodb-slate rounded-xl p-6 text-white">
              <div className="flex items-center gap-2 text-mongodb-green mb-2">
                <DollarSign className="w-5 h-5" />
                <span className="text-sm font-medium">Current Balance</span>
              </div>
              <div className="text-4xl font-bold">
                {formatCurrency(accountData.closingBalance)}
              </div>
              <div className="mt-2 text-sm text-gray-300">
                {accountData.currency} | {accountData.accountType}
              </div>
            </div>

            {/* Account holder info */}
            <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-full bg-mongodb-green/20 flex items-center justify-center">
                  <User className="w-5 h-5 text-mongodb-leaf" />
                </div>
                <div>
                  <div className="font-medium text-gray-900 dark:text-white">
                    {accountData.accountHolder?.name}
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-400">
                    {accountData.accountHolder?.email}
                  </div>
                </div>
              </div>
            </div>

            {/* Quick stats */}
            <div className="grid grid-cols-2 gap-3">
              <div className="bg-emerald-50 rounded-lg p-3">
                <div className="flex items-center gap-2 text-emerald-700 mb-1">
                  <TrendingUp className="w-4 h-4" />
                  <span className="text-xs font-medium">Total Credits</span>
                </div>
                <div className="text-lg font-bold text-emerald-800">
                  {formatCurrency(stats?.totalCredits || accountData.totalCredits)}
                </div>
                <div className="text-xs text-emerald-600">
                  {stats?.creditCount || '-'} transactions
                </div>
              </div>

              <div className="bg-rose-50 rounded-lg p-3">
                <div className="flex items-center gap-2 text-rose-700 mb-1">
                  <TrendingDown className="w-4 h-4" />
                  <span className="text-xs font-medium">Total Debits</span>
                </div>
                <div className="text-lg font-bold text-rose-800">
                  {formatCurrency(stats?.totalDebits || accountData.totalDebits)}
                </div>
                <div className="text-xs text-rose-600">
                  {stats?.debitCount || '-'} transactions
                </div>
              </div>
            </div>

            {/* Additional info */}
            <div className="grid grid-cols-2 gap-3 text-sm">
              <div className="flex items-center gap-2 text-gray-600 dark:text-gray-300">
                <CreditCard className="w-4 h-4" />
                <span>{accountData.accountNumber}</span>
              </div>
              <div className="flex items-center gap-2 text-gray-600 dark:text-gray-300">
                <Building2 className="w-4 h-4" />
                <span>{accountData.branch?.name}</span>
              </div>
            </div>

            {/* Statement period */}
            {accountData.statementPeriod && (
              <div className="text-xs text-gray-500 dark:text-gray-400 border-t pt-3">
                Statement period:{' '}
                {new Date(accountData.statementPeriod.startDate).toLocaleDateString()} -{' '}
                {new Date(accountData.statementPeriod.endDate).toLocaleDateString()}
              </div>
            )}
          </>
        )}
      </div>
    </Card>
  );
}

export default AccountLookup;
