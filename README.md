# 🗄️ Apache Cassandra Real-Time Analytics Demo

> **An educational project demonstrating Apache Cassandra's capabilities for real-time data processing**

![Cassandra](https://img.shields.io/badge/Apache%20Cassandra-3.11-blue?logo=apache-cassandra)
![Python](https://img.shields.io/badge/Python-3.11-green?logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

---

## 🎯 Project Purpose

This project showcases **why and how** to use Apache Cassandra for real-time analytics applications. It demonstrates:

| Concept | What You'll Learn |
|---------|------------------|
| **Query-First Design** | How to model data around access patterns |
| **Partition Keys** | How Cassandra distributes data across nodes |
| **Clustering Columns** | How to sort data within partitions |
| **Counter Tables** | Real-time aggregation without reading |
| **Denormalization** | Why we duplicate data for performance |
| **Time-Series Pattern** | How to handle time-bucketed data with TTL |

---

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        REAL-TIME ANALYTICS PIPELINE                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌───────────────┐       ┌───────────────┐       ┌─────────────┐  │
│   │               │       │               │       │             │  │
│   │ transactions  │──────▶│   demo.py     │──────▶│  CASSANDRA  │  │
│   │    .csv       │       │  (Producer)   │       │  DATABASE   │  │
│   │  (1.2M rows)  │       │               │       │             │  │
│   └───────────────┘       └───────────────┘       └──────┬──────┘  │
│                                                          │         │
│                                                          │         │
│                           ┌──────────────────────────────┘         │
│                           │                                        │
│                           ▼                                        │
│                    ┌─────────────┐                                 │
│                    │             │                                 │
│                    │ dashboard.py│──────▶ http://localhost:8501    │
│                    │ (Streamlit) │                                 │
│                    │             │                                 │
│                    └─────────────┘                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Project Structure

```
realtime-analytics/
├── backend/
│   ├── demo.py              # Transaction stream producer
│   └── dashboard.py         # Real-time visualization dashboard
├── cql/
│   └── schema.cql           # Annotated CQL schema with explanations
├── data/
│   └── transactions.csv     # Your transaction data (add your file here)
├── docs/
│   └── CASSANDRA_CONCEPTS.md # Complete Cassandra learning guide
├── docker-compose.yml       # Container orchestration
├── Dockerfile               # Application image
├── requirements.txt         # Python dependencies
├── run.sh                   # Startup script
└── README.md                # This file
```

---

## 🚀 Quick Start

### 1. Clone/Download the Project

```bash
cd your-project-directory
```

### 2. Add Your Data

Place your `transactions.csv` in the `data/` folder.

**Expected format:**
```csv
date,amount,category,merchant,description,payment_method,is_recurring
2019-01-01 00:00:18,4.97,misc_net,"fraud_Rippin, Kub and Mann",Purchase,Credit Card,0
```

### 3. Start Everything

```bash
docker-compose up --build
```

### 4. Open the Dashboard

Navigate to: **http://localhost:8501**

### 5. Stop

```bash
docker-compose down      # Keep data
docker-compose down -v   # Remove all data
```

---

## 📚 Cassandra Concepts Demonstrated

### 1️⃣ Query-First Data Modeling

**RDBMS Approach:** Design entities → Add indexes → Write queries  
**Cassandra Approach:** Define queries → Design ONE table per query

```sql
-- Query: "Get recent transactions for a user"
-- Table designed specifically for this query:
CREATE TABLE transactions_by_user (
    user_id TEXT,                    -- Partition Key
    transaction_time TIMESTAMP,      -- Clustering Column
    ...
    PRIMARY KEY ((user_id), transaction_time)
) WITH CLUSTERING ORDER BY (transaction_time DESC);
```

### 2️⃣ Partition Keys

The partition key determines **which node** stores the data:

```
user_id = "User_1" → hash("User_1") = token 42 → Node 2
user_id = "User_2" → hash("User_2") = token 78 → Node 4
```

**Result:** All transactions for one user are on the SAME node = fast reads!

### 3️⃣ Counter Tables

Special columns for distributed atomic counting:

```sql
CREATE TABLE spending_by_category (
    category TEXT PRIMARY KEY,
    total_amount COUNTER,       -- Atomic increment!
    transaction_count COUNTER
);

-- No read-before-write needed:
UPDATE spending_by_category 
SET total_amount = total_amount + 5000 
WHERE category = 'grocery_pos';
```

### 4️⃣ Time-Series with TTL

Automatic data expiration:

```sql
CREATE TABLE hourly_transactions (
    hour_bucket TEXT,           -- '2024-01-15-14'
    transaction_time TIMESTAMP,
    ...
    PRIMARY KEY ((hour_bucket), transaction_time)
) WITH default_time_to_live = 604800;  -- 7 days auto-delete!
```

### 5️⃣ Denormalization

One transaction written to **7 tables**:

| Table | Purpose |
|-------|---------|
| `transactions_by_user` | Query by user |
| `transactions_by_category` | Query by user + category |
| `spending_by_category` | Global category totals |
| `spending_by_user_category` | Per-user category totals |
| `hourly_transactions` | Time-series trends |
| `merchant_statistics` | Merchant analytics |
| `payment_method_stats` | Payment method analytics |

---

## 🖥️ Dashboard Features

The Streamlit dashboard shows:

1. **Live Transaction Feed** - Real-time updates from Cassandra
2. **Category Spending Chart** - Powered by counter tables
3. **Payment Method Distribution** - Counter table visualization
4. **Top Merchants** - Aggregated statistics
5. **Hourly Trends** - Time-series data with TTL
6. **CQL Examples** - Shows the queries behind each visualization

---

## ⚙️ Configuration

Edit `docker-compose.yml` to customize:

```yaml
environment:
  # Transaction streaming speed
  STREAM_DELAY: "0.5"      # 0.5 = 2 transactions/second
  
  # Max rows to load from large files
  MAX_ROWS: "50000"
```

---

## 📝 Key Files Explained

### `cql/schema.cql`

Heavily commented CQL schema explaining:
- Keyspace creation and replication
- Table design decisions
- Primary key composition
- Counter column usage
- TTL configuration

### `docs/CASSANDRA_CONCEPTS.md`

Complete learning guide covering:
- Cassandra vs RDBMS comparison
- Architecture (ring topology, tokens)
- Data modeling principles
- CQL examples with explanations
- Best practices and anti-patterns

---

## 🔧 Troubleshooting

### Cassandra Takes Too Long to Start

Cassandra needs ~60-90 seconds to initialize. Watch the logs:

```bash
docker-compose logs -f cassandra
```

Wait for: `Startup complete`

### Dashboard Shows "Cannot Connect"

1. Check if Cassandra is healthy:
   ```bash
   docker-compose ps
   ```

2. Verify the schema was created:
   ```bash
   docker exec -it cassandra-demo cqlsh -e "DESCRIBE KEYSPACES"
   ```

### Port Already in Use

```bash
# Find what's using port 9042
lsof -i :9042

# Or change the port in docker-compose.yml
ports:
  - "9043:9042"
```

---

## 🎓 Learning Resources

- 📖 [Official Cassandra Documentation](https://cassandra.apache.org/doc/latest/)
- 🎥 [DataStax Academy (Free Courses)](https://academy.datastax.com/)
- 📊 [Data Modeling Best Practices](https://www.datastax.com/learn/cassandra-fundamentals/data-modeling)

---

## 📄 License

MIT License - Feel free to use for learning and projects!

---

## 🤝 Contributing

Suggestions for additional Cassandra concepts to demonstrate? Open an issue or PR!
