import { useState, useEffect, useCallback } from 'react';
import { Search, Filter, Download, ChevronLeft, ChevronRight, X } from 'lucide-react';
import { Card } from './common/Card';
import { Badge } from './common/Badge';
import { LoadingSpinner } from './common/LoadingSpinner';
import { transactionsApi, searchApi } from '../services/api';

export function TransactionExplorer({ accountNumber }) {
  const [transactions, setTransactions] = useState([]);
  const [categories, setCategories] = useState([]);
  const [loading, setLoading] = useState(false);
  const [pagination, setPagination] = useState({
    page: 1,
    pageSize: 20,
    total: 0,
    totalPages: 0,
  });

  // Filters
  const [filters, setFilters] = useState({
    search: '',
    category: '',
    type: '',
    minAmount: '',
    maxAmount: '',
    startDate: '',
    endDate: '',
  });

  const [searchResults, setSearchResults] = useState(null);
  const [showFilters, setShowFilters] = useState(false);

  // Load categories on mount or account change
  useEffect(() => {
    if (!accountNumber) return;

    async function loadCategories() {
      try {
        const result = await transactionsApi.getCategories(accountNumber);
        setCategories(result.categories || []);
      } catch (e) {
        console.error('Failed to load categories:', e);
      }
    }
    loadCategories();
  }, [accountNumber]);

  // Load transactions
  const loadTransactions = useCallback(async () => {
    if (!accountNumber) return;

    setLoading(true);
    try {
      const options = {
        limit: pagination.pageSize,
        offset: (pagination.page - 1) * pagination.pageSize,
      };

      if (filters.category) options.category = filters.category;
      if (filters.type) options.type = filters.type;
      if (filters.minAmount) options.minAmount = parseFloat(filters.minAmount);
      if (filters.maxAmount) options.maxAmount = parseFloat(filters.maxAmount);
      if (filters.startDate) options.startDate = new Date(filters.startDate);
      if (filters.endDate) options.endDate = new Date(filters.endDate);

      const result = await transactionsApi.list(accountNumber, options);
      setTransactions(result.items || []);
      setPagination((prev) => ({
        ...prev,
        total: result.total,
        totalPages: result.totalPages,
        hasNext: result.hasNext,
        hasPrevious: result.hasPrevious,
      }));
      setSearchResults(null);
    } catch (e) {
      console.error('Failed to load transactions:', e);
    } finally {
      setLoading(false);
    }
  }, [accountNumber, pagination.page, pagination.pageSize, filters]);

  useEffect(() => {
    if (!filters.search) {
      loadTransactions();
    }
  }, [loadTransactions, filters.search]);

  // Search with debounce
  useEffect(() => {
    if (!accountNumber || !filters.search) {
      setSearchResults(null);
      return;
    }

    const timeoutId = setTimeout(async () => {
      setLoading(true);
      try {
        const result = await searchApi.search(accountNumber, filters.search, {
          limit: 50,
          fuzzy: true,
        });
        setSearchResults(result);
        setTransactions(result.results || []);
      } catch (e) {
        console.error('Search failed:', e);
      } finally {
        setLoading(false);
      }
    }, 300);

    return () => clearTimeout(timeoutId);
  }, [accountNumber, filters.search]);

  const handleFilterChange = (key, value) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
    if (key !== 'search') {
      setPagination((prev) => ({ ...prev, page: 1 }));
    }
  };

  const clearFilters = () => {
    setFilters({
      search: '',
      category: '',
      type: '',
      minAmount: '',
      maxAmount: '',
      startDate: '',
      endDate: '',
    });
    setPagination((prev) => ({ ...prev, page: 1 }));
  };

  const exportToCSV = () => {
    const headers = ['Date', 'Description', 'Amount', 'Type', 'Category', 'Merchant', 'Reference'];
    const rows = transactions.map((t) => [
      new Date(t.date).toISOString(),
      t.description,
      t.amount,
      t.type,
      t.category,
      t.merchant,
      t.referenceNumber,
    ]);

    const csv = [headers, ...rows].map((row) => row.join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `transactions_${accountNumber}_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
  };

  const formatCurrency = (amount) => {
    return new Intl.NumberFormat('en-SG', {
      style: 'currency',
      currency: 'SGD',
    }).format(amount || 0);
  };

  if (!accountNumber) {
    return (
      <Card title="Transaction Explorer" subtitle="Select an account to view transactions">
        <div className="text-center py-8 text-gray-500 dark:text-gray-400">
          Please select an account from the Account Lookup panel
        </div>
      </Card>
    );
  }

  const activeFiltersCount = Object.values(filters).filter((v) => v).length;

  return (
    <Card
      title="Transaction Explorer"
      subtitle={searchResults ? `${searchResults.resultCount} search results` : `${pagination.total} transactions`}
      action={
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-lg border transition-colors ${
              showFilters || activeFiltersCount > 0
                ? 'bg-mongodb-green text-white border-mongodb-green'
                : 'border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:bg-gray-700/50'
            }`}
          >
            <Filter className="w-4 h-4" />
            Filters
            {activeFiltersCount > 0 && (
              <span className="ml-1 px-1.5 py-0.5 text-xs bg-white dark:bg-gray-800/20 rounded">
                {activeFiltersCount}
              </span>
            )}
          </button>
          <button
            onClick={exportToCSV}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:bg-gray-700/50"
          >
            <Download className="w-4 h-4" />
            Export
          </button>
        </div>
      }
    >
      <div className="space-y-4">
        {/* Search bar */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search transactions (e.g., fairprice, grab, utilities)"
            value={filters.search}
            onChange={(e) => handleFilterChange('search', e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-mongodb-green focus:border-transparent"
          />
          {filters.search && (
            <button
              onClick={() => handleFilterChange('search', '')}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 dark:text-gray-300"
            >
              <X className="w-4 h-4" />
            </button>
          )}
        </div>

        {/* Filters panel */}
        {showFilters && (
          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4 space-y-3 animate-fade-in">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium text-gray-700 dark:text-gray-200">Filters</span>
              <button
                onClick={clearFilters}
                className="text-xs text-mongodb-leaf hover:underline"
              >
                Clear all
              </button>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Category</label>
                <select
                  value={filters.category}
                  onChange={(e) => handleFilterChange('category', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg"
                >
                  <option value="">All</option>
                  {categories.map((cat) => (
                    <option key={cat} value={cat}>
                      {cat}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Type</label>
                <select
                  value={filters.type}
                  onChange={(e) => handleFilterChange('type', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg"
                >
                  <option value="">All</option>
                  <option value="credit">Credit</option>
                  <option value="debit">Debit</option>
                </select>
              </div>

              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Min Amount</label>
                <input
                  type="number"
                  value={filters.minAmount}
                  onChange={(e) => handleFilterChange('minAmount', e.target.value)}
                  placeholder="0"
                  className="w-full px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg"
                />
              </div>

              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Max Amount</label>
                <input
                  type="number"
                  value={filters.maxAmount}
                  onChange={(e) => handleFilterChange('maxAmount', e.target.value)}
                  placeholder="10000"
                  className="w-full px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg"
                />
              </div>

              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Start Date</label>
                <input
                  type="date"
                  value={filters.startDate}
                  onChange={(e) => handleFilterChange('startDate', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg"
                />
              </div>

              <div>
                <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">End Date</label>
                <input
                  type="date"
                  value={filters.endDate}
                  onChange={(e) => handleFilterChange('endDate', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg"
                />
              </div>
            </div>
          </div>
        )}

        {/* Transaction table */}
        <div className="overflow-x-auto">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <LoadingSpinner size="lg" />
            </div>
          ) : transactions.length === 0 ? (
            <div className="text-center py-12 text-gray-500 dark:text-gray-400">
              No transactions found
            </div>
          ) : (
            <table className="w-full">
              <thead>
                <tr className="text-left text-xs text-gray-500 dark:text-gray-400 border-b">
                  <th className="pb-2 font-medium">Date</th>
                  <th className="pb-2 font-medium">Description</th>
                  <th className="pb-2 font-medium">Category</th>
                  <th className="pb-2 font-medium text-right">Amount</th>
                  <th className="pb-2 font-medium text-right">Balance</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {transactions.map((tx, idx) => (
                  <tr key={tx.transactionId || idx} className="hover:bg-gray-50 dark:bg-gray-700/50">
                    <td className="py-3 text-sm text-gray-600 dark:text-gray-300">
                      {new Date(tx.date).toLocaleDateString('en-SG', {
                        day: '2-digit',
                        month: 'short',
                      })}
                    </td>
                    <td className="py-3">
                      <div className="text-sm font-medium text-gray-900 dark:text-white">
                        {tx.merchant}
                      </div>
                      <div className="text-xs text-gray-500 dark:text-gray-400 truncate max-w-xs">
                        {tx.description}
                      </div>
                    </td>
                    <td className="py-3">
                      <Badge variant="default" size="sm">
                        {tx.category}
                      </Badge>
                    </td>
                    <td className="py-3 text-right">
                      <span
                        className={`text-sm font-medium ${
                          tx.type === 'credit' ? 'text-emerald-600' : 'text-gray-900 dark:text-white'
                        }`}
                      >
                        {tx.type === 'credit' ? '+' : '-'}
                        {formatCurrency(tx.amount)}
                      </span>
                    </td>
                    <td className="py-3 text-right text-sm text-gray-600 dark:text-gray-300">
                      {tx.runningBalance !== undefined
                        ? formatCurrency(tx.runningBalance)
                        : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Pagination */}
        {!searchResults && pagination.totalPages > 1 && (
          <div className="flex items-center justify-between border-t pt-4">
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Showing {(pagination.page - 1) * pagination.pageSize + 1} -{' '}
              {Math.min(pagination.page * pagination.pageSize, pagination.total)} of{' '}
              {pagination.total}
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPagination((p) => ({ ...p, page: p.page - 1 }))}
                disabled={pagination.page === 1}
                className="p-1.5 border border-gray-200 dark:border-gray-600 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50 dark:bg-gray-700/50"
              >
                <ChevronLeft className="w-4 h-4" />
              </button>
              <span className="text-sm text-gray-600 dark:text-gray-300">
                Page {pagination.page} of {pagination.totalPages}
              </span>
              <button
                onClick={() => setPagination((p) => ({ ...p, page: p.page + 1 }))}
                disabled={!pagination.hasNext}
                className="p-1.5 border border-gray-200 dark:border-gray-600 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50 dark:bg-gray-700/50"
              >
                <ChevronRight className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}

export default TransactionExplorer;
