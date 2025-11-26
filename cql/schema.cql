-- ============================================================================
-- CASSANDRA DATA MODEL FOR REAL-TIME PAYMENT ANALYTICS
-- ============================================================================
-- This file demonstrates key Cassandra concepts:
--   1. Keyspace creation with replication strategies
--   2. Query-first data modeling (denormalization)
--   3. Partition keys vs Clustering columns
--   4. Counter tables for real-time aggregations
--   5. Time-series data patterns
--   6. TTL (Time To Live) for data expiration
-- ============================================================================

-- ============================================================================
-- CONCEPT 1: KEYSPACE - The Top-Level Container
-- ============================================================================
-- A keyspace is like a database in RDBMS. It defines:
--   - Replication Strategy: How data is replicated across nodes
--   - Replication Factor: How many copies of data to maintain
--
-- SimpleStrategy: Used for single datacenter (development/testing)
-- NetworkTopologyStrategy: Used for production multi-datacenter deployments

CREATE KEYSPACE IF NOT EXISTS payment_analytics 
WITH replication = {
    'class': 'SimpleStrategy',    -- Single datacenter strategy
    'replication_factor': 1       -- One copy (use 3 in production)
};

-- For production with multiple datacenters:
-- CREATE KEYSPACE payment_analytics WITH replication = {
--     'class': 'NetworkTopologyStrategy',
--     'dc1': 3,    -- 3 replicas in datacenter 1
--     'dc2': 3     -- 3 replicas in datacenter 2
-- };

USE payment_analytics;

-- ============================================================================
-- CONCEPT 2: QUERY-FIRST DATA MODELING
-- ============================================================================
-- Unlike RDBMS where you model data first, Cassandra requires you to:
--   1. Identify your queries FIRST
--   2. Design tables to support those specific queries
--   3. Denormalize data (duplicate it) for read performance
--
-- Our queries:
--   Q1: Get recent transactions for a user (sorted by time)
--   Q2: Get spending totals by category
--   Q3: Get transactions by category for a user
--   Q4: Get merchant statistics
--   Q5: Get hourly transaction trends
-- ============================================================================

-- ============================================================================
-- TABLE 1: transactions_by_user
-- ============================================================================
-- PURPOSE: Answer Q1 - "Get the last N transactions for a specific user"
--
-- PARTITION KEY: user_id
--   - All transactions for one user are stored together on the same node
--   - Enables fast reads: Cassandra goes directly to one node
--   - Choose partition keys that distribute data evenly
--
-- CLUSTERING COLUMN: transaction_time DESC
--   - Data within a partition is sorted by this column
--   - DESC means newest transactions come first
--   - Enables efficient range queries on time

CREATE TABLE transactions_by_user (
    -- Partition Key: Determines which node stores this data
    user_id TEXT,
    
    -- Clustering Column: Sorts data within the partition
    transaction_time TIMESTAMP,
    
    -- Regular columns: Stored with the row
    transaction_id UUID,
    amount DECIMAL,
    category TEXT,
    merchant TEXT,
    payment_method TEXT,
    description TEXT,
    is_fraud BOOLEAN,
    
    -- Primary Key = (Partition Key, Clustering Columns)
    PRIMARY KEY ((user_id), transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC)
  AND comment = 'Stores transactions partitioned by user, sorted by time (newest first)'
  AND gc_grace_seconds = 864000;  -- 10 days before tombstones are garbage collected

-- Example queries this table supports:
-- SELECT * FROM transactions_by_user WHERE user_id = 'user123' LIMIT 50;
-- SELECT * FROM transactions_by_user WHERE user_id = 'user123' 
--        AND transaction_time > '2024-01-01' AND transaction_time < '2024-02-01';


-- ============================================================================
-- TABLE 2: transactions_by_category
-- ============================================================================
-- PURPOSE: Answer Q3 - "Get transactions in a specific category for a user"
--
-- COMPOUND PARTITION KEY: (user_id, category)
--   - Groups data by both user AND category
--   - Must provide BOTH values in queries (Cassandra requirement)
--
-- This demonstrates DENORMALIZATION:
--   - Same transaction data exists in multiple tables
--   - Each table optimized for a specific query pattern
--   - Trade-off: More storage space, but faster reads

CREATE TABLE transactions_by_category (
    user_id TEXT,
    category TEXT,
    transaction_time TIMESTAMP,
    transaction_id UUID,
    amount DECIMAL,
    merchant TEXT,
    payment_method TEXT,
    
    -- Compound partition key: both user_id AND category required
    PRIMARY KEY ((user_id, category), transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC)
  AND comment = 'Query transactions by user and category';

-- Example query:
-- SELECT * FROM transactions_by_category 
-- WHERE user_id = 'user123' AND category = 'grocery_pos' LIMIT 20;


-- ============================================================================
-- TABLE 3: spending_by_category (COUNTER TABLE)
-- ============================================================================
-- PURPOSE: Answer Q2 - "What is the total spending per category?"
--
-- COUNTER COLUMNS: Special Cassandra feature for distributed counting
--   - Atomic increment/decrement operations
--   - No read-before-write required
--   - Eventually consistent but highly performant
--   - Perfect for real-time metrics
--
-- LIMITATIONS of Counter Tables:
--   - Cannot mix counter and non-counter columns (except primary key)
--   - Cannot update counters with INSERT, must use UPDATE
--   - Cannot delete individual counter values (only entire rows)

CREATE TABLE spending_by_category (
    category TEXT PRIMARY KEY,
    total_amount COUNTER,
    transaction_count COUNTER
) WITH comment = 'Real-time spending aggregates using distributed counters';

-- Usage (note: UPDATE, not INSERT):
-- UPDATE spending_by_category SET total_amount = total_amount + 100, 
--        transaction_count = transaction_count + 1 WHERE category = 'grocery_pos';


-- ============================================================================
-- TABLE 4: spending_by_user_category
-- ============================================================================
-- PURPOSE: Per-user category spending with counter columns
--
-- Demonstrates COMPOSITE PRIMARY KEY with counters:
--   - Partition by user
--   - Cluster by category within user

CREATE TABLE spending_by_user_category (
    user_id TEXT,
    category TEXT,
    total_amount COUNTER,
    transaction_count COUNTER,
    PRIMARY KEY ((user_id), category)
) WITH comment = 'Per-user category spending using counters';


-- ============================================================================
-- TABLE 5: hourly_transactions (TIME-SERIES PATTERN)
-- ============================================================================
-- PURPOSE: Answer Q5 - "Show transaction trends by hour"
--
-- TIME-SERIES BEST PRACTICES:
--   - Use time bucketing in partition key to limit partition size
--   - Each hour's data in its own partition
--   - Prevents unbounded partition growth
--
-- BUCKETING STRATEGY:
--   - hour_bucket format: 'YYYY-MM-DD-HH' (e.g., '2024-01-15-14')
--   - Each bucket contains ~1 hour of data
--   - Can also use daily buckets: 'YYYY-MM-DD'

CREATE TABLE hourly_transactions (
    -- Time bucket as partition key limits partition size
    hour_bucket TEXT,
    
    -- Clustering by transaction_time for ordering within bucket
    transaction_time TIMESTAMP,
    transaction_id UUID,
    user_id TEXT,
    amount DECIMAL,
    category TEXT,
    merchant TEXT,
    
    PRIMARY KEY ((hour_bucket), transaction_time, transaction_id)
) WITH CLUSTERING ORDER BY (transaction_time DESC)
  AND comment = 'Time-bucketed transactions for trend analysis'
  AND default_time_to_live = 604800;  -- Auto-delete after 7 days (TTL)

-- Example: Get all transactions from a specific hour
-- SELECT * FROM hourly_transactions WHERE hour_bucket = '2024-01-15-14';


-- ============================================================================
-- TABLE 6: merchant_statistics
-- ============================================================================
-- PURPOSE: Answer Q4 - "Show top merchants by volume"

CREATE TABLE merchant_statistics (
    merchant TEXT PRIMARY KEY,
    total_amount COUNTER,
    transaction_count COUNTER
) WITH comment = 'Merchant-level aggregated statistics';


-- ============================================================================
-- TABLE 7: transactions_with_ttl (DEMONSTRATING TTL)
-- ============================================================================
-- PURPOSE: Demonstrate Time-To-Live (TTL) feature
--
-- TTL automatically deletes data after specified seconds
-- Use cases:
--   - Session data
--   - Temporary caches
--   - Audit logs that expire
--   - Compliance with data retention policies

CREATE TABLE recent_transactions_cache (
    user_id TEXT,
    transaction_time TIMESTAMP,
    transaction_id UUID,
    amount DECIMAL,
    category TEXT,
    merchant TEXT,
    
    PRIMARY KEY ((user_id), transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC)
  AND default_time_to_live = 3600  -- All data expires in 1 hour
  AND comment = 'Short-lived transaction cache with automatic TTL expiration';

-- Can also set TTL per-insert:
-- INSERT INTO recent_transactions_cache (...) VALUES (...) USING TTL 1800;


-- ============================================================================
-- TABLE 8: payment_method_stats
-- ============================================================================
-- PURPOSE: Track payment method usage patterns

CREATE TABLE payment_method_stats (
    payment_method TEXT PRIMARY KEY,
    total_amount COUNTER,
    transaction_count COUNTER
) WITH comment = 'Payment method usage statistics';


-- ============================================================================
-- CASSANDRA VS RDBMS: KEY DIFFERENCES DEMONSTRATED
-- ============================================================================
--
-- 1. NO JOINS: Each table is self-contained with denormalized data
--    RDBMS: SELECT * FROM transactions t JOIN users u ON t.user_id = u.id
--    Cassandra: Data already denormalized, no joins needed
--
-- 2. PARTITION KEY REQUIRED: Must always provide partition key in WHERE clause
--    RDBMS: SELECT * FROM transactions WHERE amount > 100  ✓
--    Cassandra: SELECT * FROM transactions WHERE amount > 100  ✗ (no partition key)
--
-- 3. NO AD-HOC QUERIES: Tables designed for specific query patterns
--    RDBMS: Flexible queries with indexes
--    Cassandra: One table per query pattern
--
-- 4. EVENTUAL CONSISTENCY: Writes propagate asynchronously
--    RDBMS: ACID transactions
--    Cassandra: Tunable consistency (ONE, QUORUM, ALL)
--
-- 5. LINEAR SCALABILITY: Add nodes to increase capacity
--    RDBMS: Vertical scaling (bigger machines)
--    Cassandra: Horizontal scaling (more machines)
--
-- ============================================================================
