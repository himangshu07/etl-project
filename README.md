# Multi-Cloud ETL Pipeline

## Overview
This is an end-to-end data engineering project built in phases to simulate a real-world ETL pipeline.  
It starts with file ingestion and processing, then evolves into a complete data pipeline using Snowflake, automation, and visualization tools.

---

## Phases

✅ Phase 1: Folder watcher (Python, file detection, cleaning, multi-cloud upload)  
⏳ Phase 2: Snowflake tables + stages  
⏳ Phase 3: Snowpipe setup  
⏳ Phase 4: Streams + Tasks  
⏳ Phase 5: Python pipeline automation  
⏳ Phase 6: Power BI dashboard  

---

## Phase 1 Features
- File watcher automation  
- Data cleaning (Excel → CSV)  
- Multi-cloud upload (AWS S3, Azure Blob, GCP Storage)  
- Logging & error handling

**Tech Stack** :
- Python  
- AWS S3 (boto3)  
- Azure Blob Storage  
- Google Cloud Storage

**How it works**
1. Drop file in folder  
2. Script detects file  
3. Cleans data  
4. Uploads to cloud

 **  Note**
Secrets are stored in `.env` (not pushed)
