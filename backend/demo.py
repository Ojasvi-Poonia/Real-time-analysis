"""
Cassandra Real-Time Streaming Demo
Demonstrates Apache Cassandra concepts through live transaction processing
"""

import csv
import time
import uuid
import random
import os
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

# Configuration
FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "transactions.csv")
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cassandra')
STREAM_DELAY = float(os.environ.get('STREAM_DELAY', '0.5'))
MAX_ROWS_IN_MEMORY = int(os.environ.get('MAX_ROWS', '50000'))
DEMO_USER_ID = "User_1"

# Formatting helpers
def print_header(text):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

def print_section(title):
    print(f"\n[{title}]")
    print("-" * 50)

def print_concept(concept, description):
    print(f"\n>> CONCEPT: {concept}")
    print(f"   {description}")

# Main Execution
print_header("CASSANDRA REAL-TIME ANALYTICS DEMO")
print("Demonstrating distributed database concepts through live transaction processing")

# Step 1: Connect to Cassandra
print_section("STEP 1: CLUSTER CONNECTION")
print_concept(
    "Masterless Architecture",
    "Cassandra has no master node. Any node can accept reads/writes."
)

print(f"\nConnecting to Cassandra at {CASSANDRA_HOST}:9042...")

session = None
cluster = None
retry_count = 0

while True:
    try:
        cluster = Cluster([CASSANDRA_HOST], port=9042)
        session = cluster.connect()
        print(f"   Connected successfully")
        print(f"   Cluster: {cluster.metadata.cluster_name}")
        break
    except Exception as e:
        retry_count += 1
        print(f"   Waiting for Cassandra (attempt {retry_count})...")
        time.sleep(5)

# Step 2: Create Keyspace
print_section("STEP 2: KEYSPACE CREATION")
print_concept(
    "Keyspace",
    "Top-level container (like a database). Defines replication strategy."
)

keyspace_cql = """
CREATE KEYSPACE IF NOT EXISTS payment_analytics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
"""
print(f"   CQL: {keyspace_cql.strip()[:60]}...")
session.execute(keyspace_cql)
session.set_keyspace('payment_analytics')
print("   Keyspace 'payment_analytics' ready")

# Step 3: Create Tables
print_section("STEP 3: TABLE CREATION (Query-First Design)")
print_concept(
    "Query-First Modeling", 
    "Design one table per query pattern. Denormalize for read performance."
)

# Drop existing tables
print("\n   Cleaning existing tables...")
tables = ['transactions_by_user', 'transactions_by_category', 'spending_by_category',
          'spending_by_user_category', 'hourly_transactions', 'merchant_statistics',
          'payment_method_stats']
for table in tables:
    try:
        session.execute(f"DROP TABLE IF EXISTS {table}")
    except:
        pass

# Table 1: transactions_by_user
print("\n   Creating: transactions_by_user")
print("   - Partition Key: user_id")
print("   - Clustering: transaction_time DESC")
print("   - Purpose: Query recent transactions for a user")
session.execute("""
    CREATE TABLE transactions_by_user (
        user_id TEXT,
        transaction_time TIMESTAMP,
        transaction_id UUID,
        amount DECIMAL,
        category TEXT,
        merchant TEXT,
        payment_method TEXT,
        PRIMARY KEY ((user_id), transaction_time)
    ) WITH CLUSTERING ORDER BY (transaction_time DESC)
""")

# Table 2: transactions_by_category
print("\n   Creating: transactions_by_category")
print("   - Partition Key: (user_id, category)")
print("   - Purpose: Query transactions by category")
session.execute("""
    CREATE TABLE transactions_by_category (
        user_id TEXT,
        category TEXT,
        transaction_time TIMESTAMP,
        transaction_id UUID,
        amount DECIMAL,
        merchant TEXT,
        PRIMARY KEY ((user_id, category), transaction_time)
    ) WITH CLUSTERING ORDER BY (transaction_time DESC)
""")

# Table 3: spending_by_category (Counter)
print("\n   Creating: spending_by_category (COUNTER TABLE)")
print("   - Counter columns for atomic distributed aggregation")
print("   - No read-before-write required")
session.execute("""
    CREATE TABLE spending_by_category (
        category TEXT PRIMARY KEY,
        total_amount COUNTER,
        transaction_count COUNTER
    )
""")

# Table 4: spending_by_user_category (Counter)
print("\n   Creating: spending_by_user_category (COUNTER)")
session.execute("""
    CREATE TABLE spending_by_user_category (
        user_id TEXT,
        category TEXT,
        total_amount COUNTER,
        transaction_count COUNTER,
        PRIMARY KEY ((user_id), category)
    )
""")

# Table 5: hourly_transactions (Time-Series with TTL)
print("\n   Creating: hourly_transactions (TIME-SERIES)")
print("   - Time bucketing: partition by hour")
print("   - TTL: 7 days (auto-delete)")
session.execute("""
    CREATE TABLE hourly_transactions (
        hour_bucket TEXT,
        transaction_time TIMESTAMP,
        transaction_id UUID,
        user_id TEXT,
        amount DECIMAL,
        category TEXT,
        PRIMARY KEY ((hour_bucket), transaction_time, transaction_id)
    ) WITH CLUSTERING ORDER BY (transaction_time DESC)
      AND default_time_to_live = 604800
""")

# Table 6: merchant_statistics (Counter)
print("\n   Creating: merchant_statistics (COUNTER)")
session.execute("""
    CREATE TABLE merchant_statistics (
        merchant TEXT PRIMARY KEY,
        total_amount COUNTER,
        transaction_count COUNTER
    )
""")

# Table 7: payment_method_stats (Counter)
print("\n   Creating: payment_method_stats (COUNTER)")
session.execute("""
    CREATE TABLE payment_method_stats (
        payment_method TEXT PRIMARY KEY,
        total_amount COUNTER,
        transaction_count COUNTER
    )
""")

print("\n   All 7 tables created successfully")

# Step 4: Prepare Statements
print_section("STEP 4: PREPARED STATEMENTS")
print_concept(
    "Prepared Statements",
    "Pre-compiled CQL for better performance. Reuse for multiple executions."
)

insert_txn_by_user = session.prepare("""
    INSERT INTO transactions_by_user 
    (user_id, transaction_time, transaction_id, amount, category, merchant, payment_method)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")

insert_txn_by_category = session.prepare("""
    INSERT INTO transactions_by_category 
    (user_id, category, transaction_time, transaction_id, amount, merchant)
    VALUES (?, ?, ?, ?, ?, ?)
""")

insert_hourly = session.prepare("""
    INSERT INTO hourly_transactions 
    (hour_bucket, transaction_time, transaction_id, user_id, amount, category)
    VALUES (?, ?, ?, ?, ?, ?)
""")

print("   3 prepared statements ready")

# Step 5: Load Data
print_section("STEP 5: DATA LOADING")

if not os.path.exists(FILE_PATH):
    print(f"   ERROR: File not found: {FILE_PATH}")
    exit(1)

# Count rows
with open(FILE_PATH, 'r', encoding='utf-8') as f:
    total_rows = sum(1 for _ in f) - 1

print(f"   Source file: {total_rows:,} transactions")

# Load with reservoir sampling
all_transactions = []
with open(FILE_PATH, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    if total_rows > MAX_ROWS_IN_MEMORY:
        print(f"   Large file - sampling {MAX_ROWS_IN_MEMORY:,} rows...")
        for i, row in enumerate(reader):
            if i < MAX_ROWS_IN_MEMORY:
                all_transactions.append(row)
            else:
                j = random.randint(0, i)
                if j < MAX_ROWS_IN_MEMORY:
                    all_transactions[j] = row
    else:
        all_transactions = list(reader)

print(f"   Loaded {len(all_transactions):,} transaction templates")

# Helper functions
def clean_merchant(merchant):
    return merchant[6:] if merchant.startswith("fraud_") else merchant

def format_category(category):
    return category.replace("_", " ").title()

def get_hour_bucket(dt):
    return dt.strftime('%Y-%m-%d-%H')

# Step 6: Start Streaming
print_header("STREAMING TRANSACTIONS")

print("""
DENORMALIZATION IN ACTION:
Each transaction is written to 7 different tables:

  1. transactions_by_user       -> Query by user
  2. transactions_by_category   -> Query by user + category
  3. hourly_transactions        -> Time-series analysis
  4. spending_by_category       -> Counter: category totals
  5. spending_by_user_category  -> Counter: user category totals
  6. merchant_statistics        -> Counter: merchant totals
  7. payment_method_stats       -> Counter: payment totals

Trade-off: 7x writes, but each READ is optimized for its query pattern.
""")

print("-" * 70)
print(f"Stream interval: {STREAM_DELAY}s | Press Ctrl+C to stop")
print("-" * 70)
print(f"{'Time':<10} {'#':<7} {'Amount':>10} {'Category':<16} {'Merchant':<28}")
print("-" * 70)

transaction_count = 0
start_time = time.time()

try:
    while True:
        row = random.choice(all_transactions)
        
        txn_time = datetime.now()
        txn_id = uuid.uuid4()
        amount = float(row['amount'])
        category = row['category']
        merchant = clean_merchant(row['merchant'])
        payment_method = row['payment_method']
        hour_bucket = get_hour_bucket(txn_time)
        
        # Write to all tables (denormalization)
        
        # 1. Main transaction log
        session.execute(insert_txn_by_user, (
            DEMO_USER_ID, txn_time, txn_id, amount, category, merchant, payment_method
        ))
        
        # 2. Category index
        session.execute(insert_txn_by_category, (
            DEMO_USER_ID, category, txn_time, txn_id, amount, merchant
        ))
        
        # 3. Time-series
        session.execute(insert_hourly, (
            hour_bucket, txn_time, txn_id, DEMO_USER_ID, amount, category
        ))
        
        # 4. Category counter
        session.execute(
            "UPDATE spending_by_category SET total_amount = total_amount + %s, "
            "transaction_count = transaction_count + 1 WHERE category = %s",
            (int(amount * 100), category)
        )
        
        # 5. User-category counter
        session.execute(
            "UPDATE spending_by_user_category SET total_amount = total_amount + %s, "
            "transaction_count = transaction_count + 1 WHERE user_id = %s AND category = %s",
            (int(amount * 100), DEMO_USER_ID, category)
        )
        
        # 6. Merchant counter
        session.execute(
            "UPDATE merchant_statistics SET total_amount = total_amount + %s, "
            "transaction_count = transaction_count + 1 WHERE merchant = %s",
            (int(amount * 100), merchant)
        )
        
        # 7. Payment method counter
        session.execute(
            "UPDATE payment_method_stats SET total_amount = total_amount + %s, "
            "transaction_count = transaction_count + 1 WHERE payment_method = %s",
            (int(amount * 100), payment_method)
        )
        
        transaction_count += 1
        
        # Output
        print(f"{txn_time.strftime('%H:%M:%S'):<10} "
              f"{transaction_count:<7} "
              f"${amount:>9.2f} "
              f"{format_category(category):<16} "
              f"{merchant[:28]:<28}")
        
        # Periodic stats
        if transaction_count % 50 == 0:
            elapsed = time.time() - start_time
            rate = transaction_count / elapsed
            print("-" * 70)
            print(f"Stats: {transaction_count} txns | {rate:.1f} txn/sec | 7 tables/txn")
            print("-" * 70)
        
        time.sleep(STREAM_DELAY)

except KeyboardInterrupt:
    elapsed = time.time() - start_time
    print("\n")
    print_header("STREAM STOPPED")
    print(f"""
SUMMARY
-------
Total Transactions: {transaction_count:,}
Duration: {elapsed:.1f} seconds
Rate: {transaction_count/elapsed:.2f} txn/sec
Tables Updated: 7 per transaction
Total Writes: {transaction_count * 7:,}

CASSANDRA CONCEPTS DEMONSTRATED
-------------------------------
- Query-first data modeling
- Partition keys and clustering columns
- Denormalization (7 tables for different queries)
- Counter tables for real-time aggregation
- Time-series bucketing
- TTL for automatic data expiration
- Prepared statements for performance
""")
    cluster.shutdown()
