# Databricks notebook source
# MAGIC %md
# MAGIC # Banking Data Generator for ODL Demo
# MAGIC
# MAGIC This notebook generates realistic Singapore banking account statements and transactions.
# MAGIC Data is written to Delta Lake tables for streaming to MongoDB Atlas.
# MAGIC
# MAGIC ## Features
# MAGIC - Realistic Singapore merchant names and transaction descriptions
# MAGIC - Proper balance calculations with running balances
# MAGIC - Configurable generation rate for load testing
# MAGIC - Support for both batch and streaming generation modes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "banking_odl", "Schema/Database Name")
dbutils.widgets.text("num_accounts", "100", "Number of Accounts")
dbutils.widgets.text("transactions_per_statement", "100", "Transactions per Statement (50-200)")
dbutils.widgets.text("generation_interval_seconds", "5", "Generation Interval (seconds)")
dbutils.widgets.dropdown("mode", "batch", ["batch", "continuous"], "Generation Mode")
dbutils.widgets.text("continuous_duration_minutes", "60", "Continuous Duration (minutes)")

# COMMAND ----------

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
num_accounts = int(dbutils.widgets.get("num_accounts"))
transactions_per_statement = int(dbutils.widgets.get("transactions_per_statement"))
generation_interval = int(dbutils.widgets.get("generation_interval_seconds"))
mode = dbutils.widgets.get("mode")
continuous_duration = int(dbutils.widgets.get("continuous_duration_minutes"))

# Validate transactions per statement
transactions_per_statement = max(50, min(200, transactions_per_statement))

print(f"Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Number of Accounts: {num_accounts}")
print(f"  Transactions per Statement: {transactions_per_statement}")
print(f"  Mode: {mode}")
if mode == "continuous":
    print(f"  Generation Interval: {generation_interval}s")
    print(f"  Duration: {continuous_duration} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Singapore-Specific Data Constants

# COMMAND ----------

import random
from datetime import datetime, timedelta
from decimal import Decimal
import uuid
from pyspark.sql.types import *
from pyspark.sql import Row
import time

# Singapore-specific constants
SINGAPORE_BANKS = ["DBS", "OCBC", "UOB", "Standard Chartered", "HSBC", "Citibank", "Maybank"]

BRANCHES = [
    {"code": "001", "name": "Marina Bay Branch", "region": "Central"},
    {"code": "002", "name": "Orchard Road Branch", "region": "Central"},
    {"code": "003", "name": "Jurong East Branch", "region": "West"},
    {"code": "004", "name": "Tampines Branch", "region": "East"},
    {"code": "005", "name": "Woodlands Branch", "region": "North"},
    {"code": "006", "name": "Bedok Branch", "region": "East"},
    {"code": "007", "name": "Toa Payoh Branch", "region": "Central"},
    {"code": "008", "name": "Ang Mo Kio Branch", "region": "North"},
    {"code": "009", "name": "Clementi Branch", "region": "West"},
    {"code": "010", "name": "Bishan Branch", "region": "Central"},
]

# Transaction categories and merchants
MERCHANTS = {
    "groceries": [
        ("NTUC FairPrice", "NETS PURCHASE - FAIRPRICE"),
        ("FairPrice Finest", "NETS PURCHASE - FAIRPRICE FINEST"),
        ("Cold Storage", "NETS PURCHASE - COLD STORAGE"),
        ("Giant", "NETS PURCHASE - GIANT"),
        ("Sheng Siong", "NETS PURCHASE - SHENG SIONG"),
        ("Prime Supermarket", "NETS PURCHASE - PRIME SUPERMARKET"),
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
        ("SBS Transit", "SIMPLYGO TRANSIT"),
        ("Gojek", "GOJEK SG"),
    ],
    "utilities": [
        ("SP Group", "GIRO - SP SERVICES"),
        ("Singtel", "GIRO - SINGTEL"),
        ("StarHub", "GIRO - STARHUB"),
        ("M1", "GIRO - M1 LIMITED"),
        ("PUB", "GIRO - PUB"),
    ],
    "entertainment": [
        ("Netflix", "NETFLIX.COM"),
        ("Spotify", "SPOTIFY SINGAPORE"),
        ("Disney+", "DISNEY PLUS"),
        ("Amazon Prime", "AMAZON PRIME SG"),
        ("Apple", "APPLE.COM/BILL"),
        ("Golden Village", "GOLDEN VILLAGE"),
        ("Shaw Theatres", "SHAW THEATRES"),
    ],
    "shopping": [
        ("Uniqlo", "UNIQLO SINGAPORE"),
        ("H&M", "H&M SINGAPORE"),
        ("Zara", "ZARA SINGAPORE"),
        ("Courts", "COURTS SINGAPORE"),
        ("Best Denki", "BEST DENKI"),
        ("Challenger", "CHALLENGER"),
        ("Popular", "POPULAR BOOKSTORE"),
        ("Decathlon", "DECATHLON SG"),
    ],
    "dining": [
        ("Toast Box", "BREADTALK - TOAST BOX"),
        ("Ya Kun Kaya Toast", "YA KUN KAYA TOAST"),
        ("Din Tai Fung", "DIN TAI FUNG"),
        ("Tim Ho Wan", "TIM HO WAN"),
        ("Jumbo Seafood", "JUMBO SEAFOOD"),
        ("Paradise Group", "PARADISE GRP"),
        ("Crystal Jade", "CRYSTAL JADE"),
        ("Swensen's", "SWENSENS"),
        ("McDonald's", "MCD SINGAPORE"),
        ("KFC", "KFC SINGAPORE"),
        ("Starbucks", "STARBUCKS SINGAPORE"),
    ],
    "healthcare": [
        ("Guardian", "GUARDIAN PHARMACY"),
        ("Watsons", "WATSONS SINGAPORE"),
        ("Unity", "UNITY PHARMACY"),
        ("Raffles Medical", "RAFFLES MEDICAL"),
        ("Mount Elizabeth", "MOUNT ELIZABETH"),
    ],
    "education": [
        ("MOE", "GIRO - MOE"),
        ("NUS", "NUS PAYMENT"),
        ("NTU", "NTU PAYMENT"),
        ("SMU", "SMU PAYMENT"),
    ],
}

SALARY_EMPLOYERS = [
    "DBS BANK LTD",
    "OCBC BANK",
    "UOB LTD",
    "SINGTEL",
    "GRAB HOLDINGS",
    "SEA LIMITED",
    "SHOPEE SINGAPORE",
    "LAZADA SG",
    "GOOGLE SINGAPORE",
    "META PLATFORMS",
    "AMAZON SINGAPORE",
    "MICROSOFT SG",
    "PSA INTERNATIONAL",
    "SINGAPORE AIRLINES",
    "CAPITALAND",
    "KEPPEL CORP",
    "SEMBCORP",
    "TEMASEK",
    "GIC",
]

CHANNELS = ["ATM", "Mobile", "Branch", "NETS", "PayNow", "GIRO", "Internet Banking"]

SINGAPORE_FIRST_NAMES = [
    "Wei Ming", "Xiu Ying", "Jun Jie", "Mei Ling", "Zhi Wei", "Li Hua", "Jia Yi", "Kai Xiang",
    "Hui Min", "Yi Xuan", "Siti", "Muhammad", "Nurul", "Ahmad", "Fatimah", "Ismail",
    "Priya", "Raj", "Arun", "Kavitha", "Sanjay", "Lakshmi", "Venkat", "Deepa",
    "Brandon", "Rachel", "Justin", "Michelle", "Kevin", "Samantha", "Ryan", "Nicole"
]

SINGAPORE_LAST_NAMES = [
    "Tan", "Lim", "Lee", "Ng", "Wong", "Goh", "Chua", "Ong", "Koh", "Chen",
    "Abdullah", "Mohamed", "Rahman", "Hassan", "Ibrahim",
    "Kumar", "Singh", "Sharma", "Patel", "Menon",
    "Johnson", "Williams", "Smith", "Brown", "Davis"
]

SINGAPORE_STREETS = [
    "Orchard Road", "Bukit Timah Road", "Clementi Road", "Tampines Street",
    "Jurong East Street", "Woodlands Drive", "Bedok North Road", "Ang Mo Kio Avenue",
    "Toa Payoh Lorong", "Bishan Street", "Yishun Ring Road", "Hougang Avenue",
    "Pasir Ris Drive", "Punggol Place", "Sengkang East Way", "Serangoon Avenue"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Functions

# COMMAND ----------

def generate_account_number():
    """Generate a Singapore-style account number."""
    return f"SG-{random.randint(1000000000, 9999999999)}"


def generate_phone():
    """Generate a Singapore phone number."""
    prefix = random.choice(["8", "9"])
    return f"+65 {prefix}{random.randint(1000000, 9999999)}"


def generate_email(name):
    """Generate an email address."""
    name_parts = name.lower().replace(" ", ".")
    domain = random.choice(["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "singnet.com.sg"])
    return f"{name_parts}{random.randint(1, 99)}@{domain}"


def generate_address():
    """Generate a Singapore address."""
    return {
        "line1": f"Blk {random.randint(1, 999)} {random.choice(SINGAPORE_STREETS)} #{random.randint(1, 30):02d}-{random.randint(1, 500):03d}",
        "line2": random.choice(["", f"Unit {random.randint(1, 50)}"]),
        "city": "Singapore",
        "postal_code": f"{random.randint(10, 82):02d}{random.randint(0, 9999):04d}",
        "country": "Singapore"
    }


def generate_account_holder():
    """Generate account holder information."""
    first_name = random.choice(SINGAPORE_FIRST_NAMES)
    last_name = random.choice(SINGAPORE_LAST_NAMES)
    full_name = f"{first_name} {last_name}"

    return {
        "name": full_name,
        "email": generate_email(full_name),
        "phone": generate_phone(),
        "address": generate_address()
    }


def generate_transaction(date, running_balance, transaction_num):
    """Generate a single transaction."""
    # Determine transaction type (70% debit, 30% credit for realistic distribution)
    if random.random() < 0.3:
        # Credit transaction
        tx_type = "credit"
        category = random.choice(["salary", "transfer", "refund", "interest"])

        if category == "salary":
            employer = random.choice(SALARY_EMPLOYERS)
            description = f"GIRO SALARY - {employer}"
            merchant = employer
            amount = Decimal(str(round(random.uniform(3000, 15000), 2)))
            channel = "GIRO"
        elif category == "transfer":
            description = f"PAYNOW TRANSFER FROM {random.choice(SINGAPORE_FIRST_NAMES).upper()}"
            merchant = "PayNow"
            amount = Decimal(str(round(random.uniform(10, 2000), 2)))
            channel = "PayNow"
        elif category == "refund":
            merchant_info = random.choice(list(MERCHANTS.values()))[0]
            description = f"REFUND - {merchant_info[1]}"
            merchant = merchant_info[0]
            amount = Decimal(str(round(random.uniform(5, 200), 2)))
            channel = random.choice(["Mobile", "Internet Banking"])
        else:  # interest
            description = "INTEREST CREDIT"
            merchant = "Bank"
            amount = Decimal(str(round(random.uniform(0.5, 50), 2)))
            channel = "GIRO"

        new_balance = running_balance + amount
    else:
        # Debit transaction
        tx_type = "debit"
        category = random.choice(list(MERCHANTS.keys()))
        merchant_info = random.choice(MERCHANTS[category])
        merchant = merchant_info[0]
        description = merchant_info[1]

        # Amount based on category
        if category in ["groceries", "shopping"]:
            amount = Decimal(str(round(random.uniform(10, 300), 2)))
        elif category in ["utilities", "education"]:
            amount = Decimal(str(round(random.uniform(30, 500), 2)))
        elif category in ["food_delivery", "dining"]:
            amount = Decimal(str(round(random.uniform(8, 80), 2)))
        elif category in ["transport"]:
            amount = Decimal(str(round(random.uniform(3, 50), 2)))
        elif category in ["entertainment"]:
            amount = Decimal(str(round(random.uniform(10, 30), 2)))
        elif category in ["healthcare"]:
            amount = Decimal(str(round(random.uniform(15, 200), 2)))
        else:
            amount = Decimal(str(round(random.uniform(5, 100), 2)))

        channel = random.choice(["NETS", "Mobile", "Internet Banking", "PayNow"])
        new_balance = running_balance - amount

    # Location based on channel
    if channel == "ATM":
        location = f"{random.choice(['DBS', 'OCBC', 'UOB'])} ATM {random.choice(SINGAPORE_STREETS)}"
    elif channel == "Branch":
        location = f"{random.choice(BRANCHES)['name']}"
    else:
        location = "Singapore"

    return {
        "transaction_id": f"TXN{date.strftime('%Y%m%d')}{transaction_num:06d}{random.randint(1000, 9999)}",
        "date": date,
        "description": description,
        "amount": amount,  # Keep as Decimal for DecimalType schema
        "type": tx_type,
        "category": category,
        "merchant": merchant,
        "reference_number": f"REF{random.randint(100000000, 999999999)}",
        "running_balance": new_balance,  # Keep as Decimal for DecimalType schema
        "metadata": {
            "channel": channel,
            "location": location
        }
    }, new_balance


def generate_statement(account_number, account_holder, account_type, branch, num_transactions):
    """Generate a complete account statement."""
    # Statement period (current month)
    now = datetime.now()
    start_date = datetime(now.year, now.month, 1)
    if now.month == 12:
        end_date = datetime(now.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(now.year, now.month + 1, 1) - timedelta(days=1)

    # Opening balance
    opening_balance = Decimal(str(round(random.uniform(1000, 50000), 2)))
    running_balance = opening_balance

    # Generate transactions
    transactions = []
    for i in range(num_transactions):
        # Spread transactions across the statement period
        days_in_period = (end_date - start_date).days
        tx_day = random.randint(0, min(days_in_period, (now - start_date).days))
        tx_date = start_date + timedelta(days=tx_day, hours=random.randint(0, 23), minutes=random.randint(0, 59))

        tx, running_balance = generate_transaction(tx_date, running_balance, i)
        transactions.append(tx)

    # Sort transactions by date
    transactions.sort(key=lambda x: x["date"])

    # Recalculate running balances after sorting
    running_balance = opening_balance
    for tx in transactions:
        if tx["type"] == "credit":
            running_balance += tx["amount"]
        else:
            running_balance -= tx["amount"]
        tx["running_balance"] = running_balance  # Keep as Decimal

    closing_balance = running_balance

    return {
        "account_number": account_number,
        "account_holder": account_holder,
        "account_type": account_type,
        "branch": branch,
        "currency": "SGD",
        "statement_period": {
            "start_date": start_date,
            "end_date": end_date
        },
        "opening_balance": opening_balance,  # Keep as Decimal
        "closing_balance": closing_balance,  # Keep as Decimal
        "transactions": transactions,
        "statement_metadata": {
            "generated_at": datetime.now(),
            "version": 1,
            "source": "databricks_lakehouse"
        },
        "_created_at": datetime.now(),
        "_updated_at": datetime.now()
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

# Define the schema matching our Delta table
# Using DecimalType(18, 2) to match the table definition in notebook 01
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("date", TimestampType(), True),
    StructField("description", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("reference_number", StringType(), True),
    StructField("running_balance", DecimalType(18, 2), True),
    StructField("metadata", StructType([
        StructField("channel", StringType(), True),
        StructField("location", StringType(), True)
    ]), True)
])

statement_schema = StructType([
    StructField("account_number", StringType(), False),
    StructField("account_holder", StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StructType([
            StructField("line1", StringType(), True),
            StructField("line2", StringType(), True),
            StructField("city", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True)
        ]), True)
    ]), True),
    StructField("account_type", StringType(), True),
    StructField("branch", StructType([
        StructField("code", StringType(), True),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True)
    ]), True),
    StructField("currency", StringType(), True),
    StructField("statement_period", StructType([
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True)
    ]), True),
    StructField("opening_balance", DecimalType(18, 2), True),
    StructField("closing_balance", DecimalType(18, 2), True),
    StructField("transactions", ArrayType(transaction_schema), True),
    StructField("statement_metadata", StructType([
        StructField("generated_at", TimestampType(), True),
        StructField("version", IntegerType(), True),
        StructField("source", StringType(), True)
    ]), True),
    StructField("_created_at", TimestampType(), True),
    StructField("_updated_at", TimestampType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Generation

# COMMAND ----------

def generate_batch(num_accounts, transactions_per_statement):
    """Generate a batch of account statements."""
    statements = []

    for i in range(num_accounts):
        account_number = generate_account_number()
        account_holder = generate_account_holder()
        account_type = random.choice(["savings", "checking", "credit"])
        branch = random.choice(BRANCHES)
        num_tx = random.randint(max(50, transactions_per_statement - 50), min(200, transactions_per_statement + 50))

        statement = generate_statement(account_number, account_holder, account_type, branch, num_tx)
        statements.append(statement)

        if (i + 1) % 10 == 0:
            print(f"Generated {i + 1}/{num_accounts} statements...")

    return statements

# COMMAND ----------

# Set the catalog and schema
try:
    spark.sql(f"USE CATALOG {catalog}")
    print(f"Using catalog: {catalog}")
except Exception as e:
    print(f"Could not use catalog '{catalog}' (may not be using Unity Catalog): {e}")
    print("Continuing with default catalog...")

spark.sql(f"USE SCHEMA {schema}")
print(f"Using schema: {schema}")

# COMMAND ----------

if mode == "batch":
    print(f"Generating {num_accounts} account statements in batch mode...")
    statements = generate_batch(num_accounts, transactions_per_statement)

    # Create DataFrame
    df = spark.createDataFrame(statements, schema=statement_schema)

    # Write to Delta table
    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("account_statements")

    print(f"Successfully wrote {num_accounts} statements to Delta table")

    # Display sample
    display(spark.sql("SELECT account_number, account_type, opening_balance, closing_balance, size(transactions) as tx_count FROM account_statements LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous Generation Mode

# COMMAND ----------

if mode == "continuous":
    print(f"Starting continuous generation mode...")
    print(f"Will run for {continuous_duration} minutes, generating every {generation_interval} seconds")

    start_time = time.time()
    end_time = start_time + (continuous_duration * 60)
    batch_num = 0

    while time.time() < end_time:
        batch_num += 1
        batch_size = random.randint(5, 15)  # Generate 5-15 statements per batch

        print(f"\nBatch {batch_num}: Generating {batch_size} statements...")
        statements = generate_batch(batch_size, transactions_per_statement)

        # Create DataFrame and write
        df = spark.createDataFrame(statements, schema=statement_schema)
        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("account_statements")

        elapsed = time.time() - start_time
        remaining = (end_time - time.time()) / 60
        print(f"Batch {batch_num} complete. Elapsed: {elapsed/60:.1f} min, Remaining: {remaining:.1f} min")

        # Wait for next interval
        time.sleep(generation_interval)

    print(f"\nContinuous generation complete. Generated {batch_num} batches.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

# Show total records
total = spark.sql("SELECT COUNT(*) as total FROM account_statements").collect()[0].total
print(f"Total statements in Delta table: {total}")

# Show sample data
display(spark.sql("""
    SELECT
        account_number,
        account_holder.name as holder_name,
        account_type,
        currency,
        opening_balance,
        closing_balance,
        size(transactions) as transaction_count,
        statement_metadata.generated_at
    FROM account_statements
    ORDER BY statement_metadata.generated_at DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Sample Transactions

# COMMAND ----------

# Explode transactions to view them individually
display(spark.sql("""
    SELECT
        account_number,
        exploded.transaction_id,
        exploded.date,
        exploded.description,
        exploded.amount,
        exploded.type,
        exploded.category,
        exploded.merchant,
        exploded.running_balance
    FROM account_statements
    LATERAL VIEW explode(transactions) as exploded
    ORDER BY exploded.date DESC
    LIMIT 50
"""))
