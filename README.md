# Multi-Cloud ETL Pipeline

## Overview
This is an end-to-end data engineering project built in phases to simulate a real-world ETL pipeline.  
It starts with file ingestion and processing, then evolves into a complete data pipeline using Snowflake, automation, and visualization tools.

---

## Phases
Phase 1: Folder watcher (Python, file detection, cleaning, multi-cloud upload)  (Done)
Phase 2: Snowflake setup (database, tables, stages)  (Done)
Phase 3: Snowpipe setup  
Phase 4: Streams + Tasks  
Phase 5: Python pipeline automation  
⏳ Phase 6: Power BI dashboard  

---

## Phase 1 Features
- File watcher automation  
- Data cleaning (Excel → CSV)  
- Multi-cloud upload (AWS S3, Azure Blob, GCP Storage)  
- Logging & error handling

**Tech Stack:**
- Python
- AWS S3 (boto3)
- Azure Blob Storage
- Google Cloud Storage

**How it works:**
1. Drop file in folder
2. Script detects file
3. Cleans data
4. Uploads to cloud

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

## Note
Secrets are stored in `.env` (not pushed to GitHub)  
Snowflake credentials are stored in a private worksheet (not pushed)