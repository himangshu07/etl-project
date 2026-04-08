import logging
import snowflake.connector
from dotenv import load_dotenv
import os

# SETUP
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


# SNOWFLAKE CONNECTION
def get_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )
    log.info("Snowflake connection established")
    return conn


# EXECUTE PROCEDURE
def call_procedure(cursor, procedure_name):
    log.info(f"Calling {procedure_name}...")
    cursor.execute(f"CALL {procedure_name}()")
    result = cursor.fetchone()
    log.info(f"{procedure_name} result: {result[0]}")


# CHECK ROW COUNTS
def check_counts(cursor):
    tables = [
        "RAW_ACCOUNTS",
        "RAW_TRANSACTIONS",
        "RAW_SHARES",
        "PROD_ACCOUNTS",
        "PROD_TRANSACTIONS",
        "PROD_SHARES"
    ]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        log.info(f"{table}: {count} rows")


# MAIN PIPELINE
def run_pipeline():
    log.info("="*50)
    log.info("ETL PIPELINE STARTED")
    log.info("="*50)

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Step 1: Load from cloud to RAW
        log.info("--- STEP 1: Loading data from cloud to RAW ---")
        call_procedure(cursor, "SP_LOAD_ACCOUNTS")
        call_procedure(cursor, "SP_LOAD_TRANSACTIONS")
        call_procedure(cursor, "SP_LOAD_SHARES")

        # Step 2: Check RAW counts
        log.info("--- STEP 2: Checking RAW counts ---")
        check_counts(cursor)

        # Step 3: Truncate PROD only
        log.info("--- STEP 3: Truncating PROD tables ---")
        cursor.execute("TRUNCATE TABLE PROD_ACCOUNTS")
        cursor.execute("TRUNCATE TABLE PROD_TRANSACTIONS")
        cursor.execute("TRUNCATE TABLE PROD_SHARES")
        cursor.execute("TRUNCATE TABLE COMPANY_SUMMARY")
        log.info("PROD tables truncated successfully")

        # Step 4: Copy RAW to BACKUP and insert to PROD
        log.info("--- STEP 4: Backup and Process to PRODUCTION ---")
        call_procedure(cursor, "SP_PROCESS_ACCOUNTS")
        call_procedure(cursor, "SP_PROCESS_TRANSACTIONS")
        call_procedure(cursor, "SP_PROCESS_SHARES")

        # Step 5: Update Summary
        log.info("--- STEP 5: Updating Company Summary ---")
        call_procedure(cursor, "SP_UPDATE_SUMMARY")

        # Step 6: Final counts
        log.info("--- STEP 6: Final table counts ---")
        check_counts(cursor)

        # Step 7: Show summary
        log.info("--- STEP 7: Company Summary ---")
        cursor.execute("""
            SELECT * FROM COMPANY_SUMMARY
            ORDER BY CREATED_AT DESC LIMIT 1
        """)
        row = cursor.fetchone()
        log.info(f"Total Accounts:     {row[1]}")
        log.info(f"Total AUM:          {row[2]}")
        log.info(f"Total Cash:         {row[3]}")
        log.info(f"Total Shares:       {row[4]}")
        log.info(f"Total Transactions: {row[5]}")
        log.info(f"Total TXN Amount:   {row[6]}")

        log.info("="*50)
        log.info("PIPELINE COMPLETED - PROD READY FOR DASHBOARD")
        log.info("RAW STAYS TILL MIDNIGHT - MIDNIGHT TASK TRUNCATES RAW")
        log.info("="*50)

    except Exception as e:
        log.error(f"Pipeline failed: {e}")

    finally:
        if conn:
            conn.close()
            log.info("Snowflake connection closed")


if __name__ == "__main__":
    run_pipeline()