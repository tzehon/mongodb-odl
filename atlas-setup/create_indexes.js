// MongoDB Atlas Index Setup for Banking ODL Demo
// Run this script using MongoDB Shell (mongosh) connected to your Atlas cluster
//
// Usage:
//   mongosh "mongodb+srv://your-cluster.mongodb.net/banking_odl" --username your_user create_indexes.js
//
// Or paste into MongoDB Compass or Atlas Data Explorer

// Switch to the banking_odl database
use("banking_odl");

print("Creating indexes for account_statements collection...");
print("=" .repeat(60));

// ============================================================================
// Primary Lookup Index
// Used for: Fetching latest statement for an account
// Query pattern: db.account_statements.find({ accountNumber: "SG-XXX" })
//               .sort({ "statementPeriod.endDate": -1 }).limit(1)
// ============================================================================
print("\n1. Creating account_statement_lookup index...");
db.account_statements.createIndex(
  {
    "accountNumber": 1,
    "statementPeriod.endDate": -1
  },
  {
    name: "account_statement_lookup",
    background: true
  }
);
print("   ✓ account_statement_lookup created");

// ============================================================================
// Transaction Date Range Index
// Used for: Fetching transactions within a date range
// Query pattern: db.account_statements.find({
//   accountNumber: "SG-XXX",
//   "transactions.date": { $gte: startDate, $lte: endDate }
// })
// ============================================================================
print("\n2. Creating account_transactions_by_date index...");
db.account_statements.createIndex(
  {
    "accountNumber": 1,
    "transactions.date": -1
  },
  {
    name: "account_transactions_by_date",
    background: true
  }
);
print("   ✓ account_transactions_by_date created");

// ============================================================================
// Category Filter Index
// Used for: Filtering transactions by category
// Query pattern: db.account_statements.find({
//   accountNumber: "SG-XXX",
//   "transactions.category": "groceries"
// })
// ============================================================================
print("\n3. Creating account_transactions_by_category index...");
db.account_statements.createIndex(
  {
    "accountNumber": 1,
    "transactions.category": 1
  },
  {
    name: "account_transactions_by_category",
    background: true
  }
);
print("   ✓ account_transactions_by_category created");

// ============================================================================
// Amount Range Index
// Used for: Filtering transactions by amount
// Query pattern: db.account_statements.find({
//   accountNumber: "SG-XXX",
//   "transactions.amount": { $gte: 100, $lte: 1000 }
// })
// ============================================================================
print("\n4. Creating account_transactions_by_amount index...");
db.account_statements.createIndex(
  {
    "accountNumber": 1,
    "transactions.amount": 1
  },
  {
    name: "account_transactions_by_amount",
    background: true
  }
);
print("   ✓ account_transactions_by_amount created");

// ============================================================================
// Account Type Index
// Used for: Filtering accounts by type
// Query pattern: db.account_statements.find({ accountType: "savings" })
// ============================================================================
print("\n5. Creating account_type index...");
db.account_statements.createIndex(
  {
    "accountType": 1
  },
  {
    name: "account_type",
    background: true
  }
);
print("   ✓ account_type created");

// ============================================================================
// Branch Region Index
// Used for: Analytics by branch/region
// Query pattern: db.account_statements.find({ "branch.region": "Central" })
// ============================================================================
print("\n6. Creating branch_region index...");
db.account_statements.createIndex(
  {
    "branch.region": 1,
    "branch.code": 1
  },
  {
    name: "branch_region",
    background: true
  }
);
print("   ✓ branch_region created");

// ============================================================================
// Statement Metadata Index (for streaming/sync monitoring)
// Used for: Tracking recent syncs, finding documents by source
// ============================================================================
print("\n7. Creating statement_metadata index...");
db.account_statements.createIndex(
  {
    "statementMetadata.generatedAt": -1,
    "statementMetadata.source": 1
  },
  {
    name: "statement_metadata",
    background: true
  }
);
print("   ✓ statement_metadata created");

// ============================================================================
// Sync Timestamp Index (for sync status monitoring)
// Used for: Finding most recently synced documents
// Query pattern: db.account_statements.find().sort({ "_syncedAt": -1 }).limit(1)
// ============================================================================
print("\n8. Creating synced_at index...");
db.account_statements.createIndex(
  {
    "_syncedAt": -1
  },
  {
    name: "synced_at",
    background: true
  }
);
print("   ✓ synced_at created");

// ============================================================================
// Compound Index for Complex Queries
// Used for: Common dashboard queries combining account + type + date
// ============================================================================
print("\n9. Creating compound_dashboard index...");
db.account_statements.createIndex(
  {
    "accountNumber": 1,
    "accountType": 1,
    "statementPeriod.startDate": -1,
    "statementPeriod.endDate": -1
  },
  {
    name: "compound_dashboard",
    background: true
  }
);
print("   ✓ compound_dashboard created");

// ============================================================================
// Verify All Indexes
// ============================================================================
print("\n" + "=".repeat(60));
print("Index creation complete. Verifying indexes...\n");

const indexes = db.account_statements.getIndexes();
print(`Total indexes on account_statements: ${indexes.length}`);
print("\nIndex details:");
indexes.forEach((idx, i) => {
  print(`  ${i + 1}. ${idx.name}`);
  print(`     Keys: ${JSON.stringify(idx.key)}`);
});

// ============================================================================
// Create Resume Token Collection for Change Streams
// ============================================================================
print("\n" + "=".repeat(60));
print("Creating change_stream_resume_tokens collection...\n");

db.createCollection("change_stream_resume_tokens", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "resumeToken", "updatedAt"],
      properties: {
        _id: {
          bsonType: "string",
          description: "Identifier for the change stream consumer"
        },
        resumeToken: {
          bsonType: "object",
          description: "MongoDB resume token for change stream"
        },
        updatedAt: {
          bsonType: "date",
          description: "Last update timestamp"
        }
      }
    }
  }
});

print("   ✓ change_stream_resume_tokens collection created");

// ============================================================================
// Summary
// ============================================================================
print("\n" + "=".repeat(60));
print("SETUP COMPLETE");
print("=".repeat(60));
print("\nNext steps:");
print("1. Create Atlas Search index using create_search_index.json");
print("2. Verify network access (IP whitelist) is configured");
print("3. Run the Databricks streaming job to start syncing data");
print("4. Start the FastAPI backend to serve queries");
