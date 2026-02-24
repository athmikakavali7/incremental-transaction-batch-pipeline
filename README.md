Incremental Idempotent Batch Transaction Processing Pipeline

Overview:
This project implements an end-to-end incremental, idempotent batch data pipeline using PySpark and PostgreSQL.
The pipeline ingests transaction data files, enforces strict schema validation, performs window-based deduplication, executes idempotent upsert operations into a relational database, tracks processed files using metadata, and archives successfully processed inputs.

Large-scale transaction systems require reliable batch processing mechanisms that ensure:

-No duplicate records
-Safe reprocessing without data corruption
-Consistent and accurate data state
-Operational traceability and observability

This project demonstrates how to design and implement such a system using distributed data processing principles and relational database upsert strategies.



Architecture:

```
+---------------------------+
|       Raw CSV Files       |
|        data/raw/          |
+-------------+-------------+
              |
              v
+---------------------------+
|     PySpark Ingestion     |
|    (Schema Enforcement)   |
+-------------+-------------+
              |
              v
+---------------------------+
|    Deduplication Layer    |
|   (Window Functions)      |
+-------------+-------------+
              |
              v
+---------------------------+
|  PostgreSQL Upsert Layer  |
|     (Idempotent Load)     |
+-------------+-------------+
              |
              v
+---------------------------+
|  Metadata Tracking Table  |
|      processed_files      |
+-------------+-------------+
              |
              v
+---------------------------+
|       File Archiving      |
|       data/archive/       |
+-------------+-------------+
              |
              v
+---------------------------+
|     Structured Logging    |
|     logs/pipeline.log     |
+---------------------------+
```


Architectural Characteristic:
- Modular pipeline design (separation of ingestion, transformation, load, metadata)
- Idempotent database operations using primary key constraints
- Incremental processing via metadata-driven orchestration
- Re-runnable batch job design
- Production-safe file handling with archiving
- Structured logging for operational visibility


Tech Stack:
-Python
-PySpark
-PostgreSQL
-SQL
-psycopg2
-Logging module


Execution Flow:
1.Detect CSV files in data/raw/
2.Check metadata table to skip already processed files
3.Read file using strict Spark schema
4.Deduplicate transactions using window function
5.Perform idempotent upsert into PostgreSQL
6.Insert file name into metadata table
7.Move file to data/archive/
8.Log execution details to logs/pipeline.log


Key Features:
-Strict schema enforcement using PySpark
-Deduplication using ROW_NUMBER() window function
-Idempotent database upsert using ON CONFLICT
-Incremental file processing via metadata tracking
-Automatic file archiving
-Absolute path resolution (production-safe)
-Defensive file filtering (CSV-only processing)
-Structured logging (console + file)
-Modular project architecture

Project Structure:
```
incremental-transaction-batch-pipeline/
│
├── data/
│   ├── raw/
│   └── archive/
│
├── logs/
│   └── pipeline.log
│
├── src/
│   ├── main.py
│   ├── transformer.py
│   ├── loader.py
│   ├── metadata.py
│   ├── spark_session.py
│   ├── logger_config.py
│   └── config.py
│
├── requirements.txt
├── README.md
└── .gitignore
```

Database Schema:
fact_transactions
transaction_id (Primary Key)
user_id
transaction_time
amount
status
updated_at

processed_files
file_name (Primary Key)
processed_at


Sample Input:
transaction_id,user_id,transaction_time,amount,status  
t1,u1,2026-01-01 10:00:00,100,completed  
t1,u1,2026-01-01 10:20:00,110,updated  


How to Run:

1️. Clone repository
git clone <your-repo-url>
cd incremental-transaction-batch-pipeline

2️. Create virtual environment
python -m venv venv
venv\Scripts\activate

3️. Install dependencies
pip install -r requirements.txt

4️. Configure database
Edit:
src/config.py
Set your PostgreSQL credentials.

5️. Create required tables in PostgreSQL
Run:

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    amount NUMERIC NOT NULL,
    status TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_files (
    file_name TEXT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

6️. Place CSV file in:
data/raw/

7️. Execute pipeline
python src/main.py


Idempotency Strategy:
The pipeline ensures idempotent behavior using: SQL
ON CONFLICT (transaction_id)
DO UPDATE
SET
  user_id = EXCLUDED.user_id,
  transaction_time = EXCLUDED.transaction_time,
  amount = EXCLUDED.amount,
  status = EXCLUDED.status,
  updated_at = CURRENT_TIMESTAMP
WHERE fact_transactions.transaction_time < EXCLUDED.transaction_time;

This guarantees:
-No duplicate records
-Only latest transaction updates are applied
-Safe re-execution of pipeline



Observability
Structured logging enabled

Logs written to:
logs/pipeline.log

Each execution logs:
Start time
Files processed
Records loaded
Errors
Completion status


Production Considerations:
- Designed for re-runnable batch jobs
- Handles partial failures safely
- Avoids duplicate writes via database constraints
- Separates transformation and load logic
- Structured logging for operational debugging


What This Demonstrates:
-This project demonstrates practical data engineering concepts:
-Batch ETL design
-Incremental processing
-Metadata-driven control
-Window-based deduplication
-Database upsert patterns
-Production-safe file handling
-Logging and observability


Design Decisions:
- Used window function for deduplication to retain latest transaction.
- Used ON CONFLICT upsert to ensure idempotent loads.
- Used metadata table to enable incremental batch processing.
- Implemented absolute path resolution to avoid environment-specific issues.
- Structured logging added for observability.


Possible Extensions:
-Partitioned data lake storage
-Dockerization
-Airflow orchestration
-AWS S3 integration
-EMR migration
-Data quality checks
-Metrics table for pipeline runs


