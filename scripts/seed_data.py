#!/usr/bin/env python3
"""
Seed script to populate MongoDB Atlas with sample banking data.

This allows the demo to work without Databricks initially.
Run this script before docker-compose if you want sample data.

Usage:
    python scripts/seed_data.py --uri "mongodb+srv://..." --count 50

Or set MONGODB_URI environment variable:
    export MONGODB_URI="mongodb+srv://..."
    python scripts/seed_data.py --count 50
"""

import argparse
import os
import random
import sys
from datetime import datetime, timedelta
from decimal import Decimal
import uuid

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
except ImportError:
    print("Error: pymongo is required. Install with: pip install pymongo")
    sys.exit(1)

# Singapore-specific constants
BRANCHES = [
    {"code": "001", "name": "Marina Bay Branch", "region": "Central"},
    {"code": "002", "name": "Orchard Road Branch", "region": "Central"},
    {"code": "003", "name": "Jurong East Branch", "region": "West"},
    {"code": "004", "name": "Tampines Branch", "region": "East"},
    {"code": "005", "name": "Woodlands Branch", "region": "North"},
]

MERCHANTS = {
    "groceries": [
        ("NTUC FairPrice", "NETS PURCHASE - FAIRPRICE"),
        ("FairPrice Finest", "NETS PURCHASE - FAIRPRICE FINEST"),
        ("Cold Storage", "NETS PURCHASE - COLD STORAGE"),
        ("Giant", "NETS PURCHASE - GIANT"),
        ("Sheng Siong", "NETS PURCHASE - SHENG SIONG"),
    ],
    "food_delivery": [
        ("GrabFood", "GRAB*GRABFOOD"),
        ("FoodPanda", "FOODPANDA SG"),
        ("Deliveroo", "DELIVEROO SG"),
    ],
    "transport": [
        ("Grab", "GRAB*GRABTRANSPORT"),
        ("ComfortDelGro", "CDG ZIG"),
        ("SMRT", "SIMPLYGO TRANSIT"),
        ("Gojek", "GOJEK SG"),
    ],
    "utilities": [
        ("SP Group", "GIRO - SP SERVICES"),
        ("Singtel", "GIRO - SINGTEL"),
        ("StarHub", "GIRO - STARHUB"),
        ("M1", "GIRO - M1 LIMITED"),
    ],
    "entertainment": [
        ("Netflix", "NETFLIX.COM"),
        ("Spotify", "SPOTIFY SINGAPORE"),
        ("Disney+", "DISNEY PLUS"),
        ("Apple", "APPLE.COM/BILL"),
    ],
    "shopping": [
        ("Uniqlo", "UNIQLO SINGAPORE"),
        ("H&M", "H&M SINGAPORE"),
        ("Courts", "COURTS SINGAPORE"),
        ("Challenger", "CHALLENGER"),
    ],
    "dining": [
        ("Toast Box", "BREADTALK - TOAST BOX"),
        ("Ya Kun Kaya Toast", "YA KUN KAYA TOAST"),
        ("Din Tai Fung", "DIN TAI FUNG"),
        ("McDonald's", "MCD SINGAPORE"),
        ("Starbucks", "STARBUCKS SINGAPORE"),
    ],
}

SALARY_EMPLOYERS = [
    "DBS BANK LTD", "OCBC BANK", "UOB LTD", "SINGTEL",
    "GRAB HOLDINGS", "SEA LIMITED", "SHOPEE SINGAPORE",
    "GOOGLE SINGAPORE", "AMAZON SINGAPORE", "MICROSOFT SG",
]

FIRST_NAMES = [
    "Wei Ming", "Xiu Ying", "Jun Jie", "Mei Ling", "Zhi Wei",
    "Siti", "Muhammad", "Nurul", "Ahmad", "Fatimah",
    "Priya", "Raj", "Arun", "Kavitha", "Sanjay",
    "Brandon", "Rachel", "Justin", "Michelle", "Kevin"
]

LAST_NAMES = [
    "Tan", "Lim", "Lee", "Ng", "Wong", "Goh", "Chua", "Ong",
    "Abdullah", "Mohamed", "Rahman", "Hassan",
    "Kumar", "Singh", "Sharma", "Patel"
]

STREETS = [
    "Orchard Road", "Bukit Timah Road", "Clementi Road",
    "Tampines Street", "Jurong East Street", "Woodlands Drive",
    "Bedok North Road", "Ang Mo Kio Avenue", "Toa Payoh Lorong"
]

CHANNELS = ["ATM", "Mobile", "Branch", "NETS", "PayNow", "GIRO", "Internet Banking"]


def generate_account_number():
    return f"SG-{random.randint(1000000000, 9999999999)}"


def generate_account_holder():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"
    email = f"{first.lower().replace(' ', '.')}.{last.lower()}{random.randint(1, 99)}@gmail.com"

    return {
        "name": name,
        "email": email,
        "phone": f"+65 {random.choice(['8', '9'])}{random.randint(1000000, 9999999)}",
        "address": {
            "line1": f"Blk {random.randint(1, 999)} {random.choice(STREETS)} #{random.randint(1, 30):02d}-{random.randint(1, 500):03d}",
            "line2": "",
            "city": "Singapore",
            "postalCode": f"{random.randint(10, 82):02d}{random.randint(0, 9999):04d}",
            "country": "Singapore"
        }
    }


def generate_transaction(date, running_balance, tx_num):
    if random.random() < 0.25:  # 25% credits
        tx_type = "credit"
        category = random.choice(["salary", "transfer", "refund"])

        if category == "salary":
            employer = random.choice(SALARY_EMPLOYERS)
            description = f"GIRO SALARY - {employer}"
            merchant = employer
            amount = round(random.uniform(3000, 15000), 2)
            channel = "GIRO"
        elif category == "transfer":
            description = f"PAYNOW TRANSFER FROM {random.choice(FIRST_NAMES).upper()}"
            merchant = "PayNow"
            amount = round(random.uniform(10, 2000), 2)
            channel = "PayNow"
        else:
            merchant_info = random.choice(list(MERCHANTS.values()))[0]
            description = f"REFUND - {merchant_info[1]}"
            merchant = merchant_info[0]
            amount = round(random.uniform(5, 200), 2)
            channel = "Mobile"

        new_balance = running_balance + amount
    else:  # 75% debits
        tx_type = "debit"
        category = random.choice(list(MERCHANTS.keys()))
        merchant_info = random.choice(MERCHANTS[category])
        merchant = merchant_info[0]
        description = merchant_info[1]

        if category in ["groceries", "shopping"]:
            amount = round(random.uniform(10, 300), 2)
        elif category in ["utilities"]:
            amount = round(random.uniform(30, 500), 2)
        elif category in ["food_delivery", "dining"]:
            amount = round(random.uniform(8, 80), 2)
        elif category in ["transport"]:
            amount = round(random.uniform(3, 50), 2)
        else:
            amount = round(random.uniform(10, 100), 2)

        channel = random.choice(["NETS", "Mobile", "Internet Banking", "PayNow"])
        new_balance = running_balance - amount

    return {
        "transactionId": f"TXN{date.strftime('%Y%m%d')}{tx_num:06d}{random.randint(1000, 9999)}",
        "date": date,
        "description": description,
        "amount": amount,
        "type": tx_type,
        "category": category,
        "merchant": merchant,
        "referenceNumber": f"REF{random.randint(100000000, 999999999)}",
        "runningBalance": round(new_balance, 2),
        "metadata": {
            "channel": channel,
            "location": "Singapore"
        }
    }, new_balance


def generate_statement(num_transactions=100):
    now = datetime.now()
    start_date = datetime(now.year, now.month, 1)
    if now.month == 12:
        end_date = datetime(now.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(now.year, now.month + 1, 1) - timedelta(days=1)

    opening_balance = round(random.uniform(1000, 50000), 2)
    running_balance = opening_balance

    transactions = []
    for i in range(num_transactions):
        days_in_period = (min(end_date, now) - start_date).days
        tx_day = random.randint(0, max(1, days_in_period))
        tx_date = start_date + timedelta(
            days=tx_day,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        tx, running_balance = generate_transaction(tx_date, running_balance, i)
        transactions.append(tx)

    transactions.sort(key=lambda x: x["date"])

    # Recalculate running balances after sort
    running_balance = opening_balance
    for tx in transactions:
        if tx["type"] == "credit":
            running_balance += tx["amount"]
        else:
            running_balance -= tx["amount"]
        tx["runningBalance"] = round(running_balance, 2)

    return {
        "accountNumber": generate_account_number(),
        "accountHolder": generate_account_holder(),
        "accountType": random.choice(["savings", "checking", "credit"]),
        "branch": random.choice(BRANCHES),
        "currency": "SGD",
        "statementPeriod": {
            "startDate": start_date,
            "endDate": end_date
        },
        "openingBalance": opening_balance,
        "closingBalance": round(running_balance, 2),
        "transactions": transactions,
        "statementMetadata": {
            "generatedAt": datetime.now(),
            "version": 1,
            "source": "seed_script"
        }
    }


def main():
    parser = argparse.ArgumentParser(description="Seed MongoDB with sample banking data")
    parser.add_argument("--uri", help="MongoDB connection URI (or set MONGODB_URI env var)")
    parser.add_argument("--database", default="banking_odl", help="Database name")
    parser.add_argument("--collection", default="account_statements", help="Collection name")
    parser.add_argument("--count", type=int, default=50, help="Number of accounts to generate")
    parser.add_argument("--transactions", type=int, default=100, help="Transactions per account (50-200)")
    parser.add_argument("--drop", action="store_true", help="Drop existing collection first")

    args = parser.parse_args()

    uri = args.uri or os.environ.get("MONGODB_URI")
    if not uri:
        print("Error: MongoDB URI required. Use --uri or set MONGODB_URI environment variable")
        print("\nExample:")
        print('  python seed_data.py --uri "mongodb+srv://user:pass@cluster.mongodb.net"')
        sys.exit(1)

    transactions_count = max(50, min(200, args.transactions))

    print(f"Connecting to MongoDB...")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("Connected successfully!")
    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)

    db = client[args.database]
    collection = db[args.collection]

    if args.drop:
        print(f"Dropping existing collection {args.collection}...")
        collection.drop()

    print(f"Generating {args.count} account statements with ~{transactions_count} transactions each...")

    statements = []
    for i in range(args.count):
        stmt = generate_statement(transactions_count)
        statements.append(stmt)
        if (i + 1) % 10 == 0:
            print(f"  Generated {i + 1}/{args.count} statements...")

    print(f"Inserting {len(statements)} statements into MongoDB...")
    result = collection.insert_many(statements)
    print(f"Inserted {len(result.inserted_ids)} documents")

    # Show summary
    total_docs = collection.count_documents({})
    print(f"\nDone! Total documents in collection: {total_docs}")
    print(f"\nSample account numbers:")
    for stmt in statements[:5]:
        print(f"  - {stmt['accountNumber']}")

    print(f"\nYou can now run: docker-compose up --build")


if __name__ == "__main__":
    main()
