import { useState } from 'react';
import { Database, ChevronDown, ChevronRight, Copy, Check } from 'lucide-react';

const QUERY_DOCS = [
  {
    endpoint: 'GET /accounts/{account}/balance',
    description: 'Get current balance for an account',
    operation: 'find + sort',
    query: `db.account_statements.find(
  { accountNumber: "SG-1234567890" }
).sort(
  { "statementPeriod.endDate": -1 }
).limit(1)`,
    index: 'accountNumber_1_statementPeriod.endDate_-1',
  },
  {
    endpoint: 'GET /accounts/{account}/statements/latest',
    description: 'Get the latest statement for an account',
    operation: 'find + sort',
    query: `db.account_statements.find(
  { accountNumber: "SG-1234567890" }
).sort(
  { "statementPeriod.endDate": -1 }
).limit(1)`,
    index: 'accountNumber_1_statementPeriod.endDate_-1',
  },
  {
    endpoint: 'GET /accounts/{account}/summary',
    description: 'Get account summary with transaction stats',
    operation: 'find + sort + compute',
    query: `// Fetch latest statement
db.account_statements.find(
  { accountNumber: "SG-1234567890" }
).sort(
  { "statementPeriod.endDate": -1 }
).limit(1)

// Then compute totals in Python:
// totalCredits, totalDebits, transactionCount`,
    index: 'accountNumber_1_statementPeriod.endDate_-1',
  },
  {
    endpoint: 'GET /accounts/{account}/transactions',
    description: 'Get transactions with filtering and pagination',
    operation: 'aggregate',
    query: `db.account_statements.aggregate([
  { $match: { accountNumber: "SG-1234567890" } },
  { $unwind: "$transactions" },
  // Optional filters:
  // { $match: { "transactions.category": "groceries" } },
  // { $match: { "transactions.date": { $gte: startDate } } },
  { $sort: { "transactions.date": -1 } },
  { $skip: 0 },
  { $limit: 50 },
  { $replaceRoot: { newRoot: "$transactions" } }
])`,
    index: 'accountNumber_1',
  },
  {
    endpoint: 'GET /accounts/{account}/transactions/search',
    description: 'Full-text search using Atlas Search',
    operation: '$search aggregate',
    query: `db.account_statements.aggregate([
  {
    $search: {
      index: "transaction_search",
      compound: {
        must: [
          { text: { query: "SG-1234567890", path: "accountNumber" } }
        ],
        should: [
          {
            text: {
              query: "fairprice",
              path: "transactions.description",
              fuzzy: { maxEdits: 2 },
              score: { boost: { value: 2 } }
            }
          },
          {
            text: {
              query: "fairprice",
              path: "transactions.merchant",
              fuzzy: { maxEdits: 1 }
            }
          }
        ],
        minimumShouldMatch: 1
      }
    }
  },
  { $unwind: "$transactions" },
  { $match: {
      $or: [
        { "transactions.description": { $regex: "fairprice", $options: "i" } },
        { "transactions.merchant": { $regex: "fairprice", $options: "i" } }
      ]
  }},
  { $limit: 20 }
])`,
    index: 'Atlas Search: transaction_search',
  },
];

function CopyButton({ text }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <button
      onClick={handleCopy}
      className="p-1 rounded hover:bg-gray-700 transition-colors"
      title="Copy query"
    >
      {copied ? (
        <Check className="w-4 h-4 text-green-400" />
      ) : (
        <Copy className="w-4 h-4 text-gray-400" />
      )}
    </button>
  );
}

function QueryCard({ doc }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="border border-gray-700 rounded-lg overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-4 py-3 flex items-center justify-between bg-gray-800 hover:bg-gray-750 transition-colors"
      >
        <div className="flex items-center gap-3">
          {expanded ? (
            <ChevronDown className="w-4 h-4 text-gray-400" />
          ) : (
            <ChevronRight className="w-4 h-4 text-gray-400" />
          )}
          <code className="text-sm text-mongodb-green font-mono">{doc.endpoint}</code>
        </div>
        <span className="text-xs text-gray-500 bg-gray-700 px-2 py-1 rounded">
          {doc.operation}
        </span>
      </button>

      {expanded && (
        <div className="p-4 bg-gray-900 border-t border-gray-700">
          <p className="text-sm text-gray-400 mb-3">{doc.description}</p>

          <div className="mb-3">
            <div className="flex items-center justify-between mb-1">
              <span className="text-xs text-gray-500 uppercase tracking-wide">MongoDB Query</span>
              <CopyButton text={doc.query} />
            </div>
            <pre className="bg-gray-950 p-3 rounded text-xs text-gray-300 overflow-x-auto font-mono">
              {doc.query}
            </pre>
          </div>

          <div className="flex items-center gap-2 text-xs">
            <span className="text-gray-500">Index:</span>
            <code className="text-yellow-400 bg-gray-800 px-2 py-0.5 rounded">{doc.index}</code>
          </div>
        </div>
      )}
    </div>
  );
}

export function QueryReference() {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 p-6">
      <div className="flex items-center gap-3 mb-6">
        <div className="w-10 h-10 rounded-lg bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center">
          <Database className="w-5 h-5 text-purple-600 dark:text-purple-400" />
        </div>
        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
            Query Reference
          </h2>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            API endpoints and their MongoDB queries
          </p>
        </div>
      </div>

      <div className="space-y-2">
        {QUERY_DOCS.map((doc, index) => (
          <QueryCard key={index} doc={doc} />
        ))}
      </div>

      <div className="mt-6 p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
        <p className="text-sm text-blue-800 dark:text-blue-300">
          <strong>Benchmark Mode:</strong> During load testing, these queries run with{' '}
          <code className="bg-blue-100 dark:bg-blue-800 px-1 rounded">explain("executionStats")</code>{' '}
          to capture actual MongoDB execution time (excluding network latency).
        </p>
      </div>
    </div>
  );
}
