# Multi-Cloud ETL File Watcher

## Overview
This project monitors folders and uploads cleaned data to AWS S3, Azure Blob, and GCP Storage.

## Features
- File watcher automation
- Data cleaning (Excel → CSV)
- Multi-cloud upload (AWS, Azure, GCP)
- Logging & error handling

## Tech Stack
- Python
- AWS S3 (boto3)
- Azure Blob Storage
- Google Cloud Storage

## How it works
1. Drop file in folder
2. Script detects file
3. Cleans data
4. Uploads to cloud

## Note
Secrets are stored in `.env` (not pushed)