# Multi-Cloud Financial ETL Pipeline

## Overview
This is an end-to-end data engineering project built in phases to simulate a real-world financial ETL pipeline.
It ingests data from multiple cloud sources, processes and transforms it using Snowflake, and visualizes it through Power BI.
Drop a file into a folder and everything runs automatically.

---

## Phases
-  Phase 1: Folder watcher (Python, file detection, cleaning, multi-cloud upload)
-  Phase 2: Snowflake setup (database, tables, stages)
-  Phase 3: Snowpipe setup (auto ingest from AWS, Azure, GCP)
-  Phase 4: Streams, Stored Procedures and Tasks (automated processing)
-  Phase 5: Python pipeline automation
-  Phase 6: Power BI dashboard

---

## Phase 1 Features
- Watches local folders for new files automatically
- Cleans and validates data (removes nulls, duplicates, invalid rows)
- Uploads clean CSV to correct cloud based on folder
- Logging and error handling

**Tech Stack:**
- Python
- watchdog
- AWS S3 (boto3)
- Azure Blob Storage
- Google Cloud Storage (GCP)
- pandas

**How it works:**
1. Drop file into aws/ azure/ or gcp/ folder
2. Watcher detects file instantly
3. Cleans and validates data
4. Uploads clean file to correct cloud bucket

---

## Phase 2 Features
- Snowflake database, schema and warehouse setup
- 10 tables created (RAW, BACKUP, PRODUCTION, SUMMARY)
- External stages connected to AWS S3, Azure Blob, GCP
- CSV file format configured
- GCP Storage Integration setup

**Tech Stack:**
- Snowflake
- AWS S3 Stage
- Azure Blob Stage
- GCP Stage (Storage Integration)

**Tables Created:**
- RAW_ACCOUNTS, RAW_TRANSACTIONS, RAW_SHARES
- BACKUP_ACCOUNTS, BACKUP_TRANSACTIONS, BACKUP_SHARES
- PROD_ACCOUNTS, PROD_TRANSACTIONS, PROD_SHARES
- COMPANY_SUMMARY

---

## Phase 3 Features
- 3 Snowpipes created (one per cloud source)
- Auto ingests files from stages into RAW tables
- PIPE_ACCOUNTS (AWS S3 → RAW_ACCOUNTS)
- PIPE_TRANSACTIONS (Azure Blob → RAW_TRANSACTIONS)
- PIPE_SHARES (GCP → RAW_SHARES)

**Tech Stack:**
- Snowflake Snowpipe
- AWS S3
- Azure Blob Storage
- GCP Cloud Storage

**How it works:**
1. File lands in cloud bucket
2. Snowpipe detects file automatically
3. Loads data into correct RAW table
4. Data ready for processing

---

## Phase 4 Features
- 3 Streams created to detect new data in RAW tables
- 4 Stored Procedures created to process data
- 4 Tasks created to run on CRON schedule (daily at midnight IST)
- Data flows RAW to BACKUP and PRODUCTION automatically
- RAW tables truncated after processing
- Company Summary updated with latest figures

**Tech Stack:**
- Snowflake Streams
- Snowflake Stored Procedures
- Snowflake Tasks

**Stored Procedures:**
- SP_PROCESS_ACCOUNTS
- SP_PROCESS_TRANSACTIONS
- SP_PROCESS_SHARES
- SP_UPDATE_SUMMARY

**How it works:**
1. Stream detects new rows in RAW table
2. Task triggers Stored Procedure on schedule
3. Procedure copies data to BACKUP table
4. Procedure inserts clean data to PRODUCTION table
5. RAW table is truncated
6. Company Summary is updated

---

## Setup Instructions
1. Clone the repository
2. Install dependencies
```bash
pip install -r requirements.txt
```
3. Create `.env` file from `.env.example` and fill in credentials
4. Run the watcher
```bash
python watcher.py
```
5. Run Snowflake SQL files in order (01 to 06)
6. Drop files into watch-folder and pipeline runs automatically

---

## Phase 5 - Python Pipeline Automation

- Connects to Snowflake using snowflake-connector-python
- Loads data from all 3 cloud stages into RAW tables
- Truncates PROD tables before processing (no duplicates)
- Calls stored procedures to backup and process data
- Updates Company Summary automatically
- RAW tables stay till midnight for reprocessing if needed
- Midnight Task truncates RAW and resets for next day
- Full logging for every step

**Tech Stack:**
- Python
- snowflake-connector-python
- python-dotenv

**How it works:**
1. SP_LOAD procedures load files from cloud stages to RAW
2. PROD tables truncated before fresh load
3. SP_PROCESS procedures copy RAW to BACKUP and insert to PROD
4. SP_UPDATE_SUMMARY updates Company Summary
5. RAW stays all day as safety net
6. Midnight Task truncates RAW automatically

**Pipeline Steps:**
- Step 1: Load data from AWS S3, Azure Blob, GCP to RAW tables
- Step 2: Check RAW row counts
- Step 3: Truncate PROD tables
- Step 4: Backup and process to PRODUCTION
- Step 5: Update Company Summary
- Step 6: Final row count verification
- Step 7: Display Company Summary figures

## Note
Secrets are stored in `.env` (not pushed to GitHub)
Snowflake credentials are stored in a private worksheet (not pushed)
GCP service account key is not pushed to GitHub