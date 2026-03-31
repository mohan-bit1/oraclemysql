import cx_Oracle
import pyodbc
import time
import logging
from datetime import datetime

# ====================== CONFIGURATION ======================
ORACLE_DSN = "your_oracle_host:1521/your_service_name"   # or TNS name
ORACLE_USER = "your_oracle_username"
ORACLE_PASSWORD = "your_oracle_password"

# MSSQL Configuration
MSSQL_SERVER = "your_mssql_server"          # e.g., localhost, 192.168.1.100\SQLEXPRESS
MSSQL_DATABASE = "your_database_name"
MSSQL_USER = "your_mssql_username"          # Use Windows Auth? Set to None
MSSQL_PASSWORD = "your_mssql_password"      # Leave empty for Windows Authentication

TABLE_NAME = "hr_rawdata_inter"

# Last sync time (will be managed automatically)
LAST_PUNCHTIME = None

# ====================== LOGGING ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("oracle_to_mssql_sync.log"),
        logging.StreamHandler()
    ]
)

def get_oracle_connection():
    return cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)

def get_mssql_connection():
    if MSSQL_USER and MSSQL_PASSWORD:
        # SQL Authentication
        conn_str = f"""
            DRIVER={{ODBC Driver 17 for SQL Server}};
            SERVER={MSSQL_SERVER};
            DATABASE={MSSQL_DATABASE};
            UID={MSSQL_USER};
            PWD={MSSQL_PASSWORD};
        """
    else:
        # Windows Authentication
        conn_str = f"""
            DRIVER={{ODBC Driver 17 for SQL Server}};
            SERVER={MSSQL_SERVER};
            DATABASE={MSSQL_DATABASE};
            Trusted_Connection=yes;
        """
    
    return pyodbc.connect(conn_str)

def create_mssql_table_if_not_exists():
    """Create the target table in MSSQL if it doesn't exist"""
    conn = get_mssql_connection()
    cursor = conn.cursor()
    
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{TABLE_NAME}') AND type in (N'U'))
    BEGIN
        CREATE TABLE {TABLE_NAME} (
            empcode CHAR(8) NOT NULL,
            punchtime DATETIME NOT NULL,
            machineip CHAR(20) NOT NULL,
            inoutmode CHAR(1) NOT NULL,
            status CHAR(1) NOT NULL DEFAULT 'P',
            deviceid CHAR(8) NOT NULL,
            CONSTRAINT PK_{TABLE_NAME} PRIMARY KEY (empcode, punchtime)
        )
    END
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"MSSQL table '{TABLE_NAME}' is ready.")

def get_max_punchtime_mssql():
    """Get the latest punchtime from MSSQL"""
    conn = get_mssql_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(punchtime) as max_time FROM {TABLE_NAME}")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return result[0] if result and result[0] else None

def sync_new_records():
    global LAST_PUNCHTIME
    
    try:
        # Initialize last punchtime on first run
        if LAST_PUNCHTIME is None:
            LAST_PUNCHTIME = get_max_punchtime_mssql()
            if LAST_PUNCHTIME is None:
                LAST_PUNCHTIME = datetime(1900, 1, 1)   # Very old date for initial sync
        
        # Connect to Oracle
        oracle_conn = get_oracle_connection()
        oracle_cursor = oracle_conn.cursor()
        
        # Fetch only new records from Oracle
        query = f"""
            SELECT empcode, punchtime, machineip, inoutmode, status, deviceid 
            FROM hr_rawdata_inter 
            WHERE punchtime > :last_time
            ORDER BY punchtime ASC
        """
        
        oracle_cursor.execute(query, {'last_time': LAST_PUNCHTIME})
        rows = oracle_cursor.fetchall()
        
        if not rows:
            logging.info("No new records to sync.")
            oracle_cursor.close()
            oracle_conn.close()
            return
        
        # Connect to MSSQL
        mssql_conn = get_mssql_connection()
        mssql_cursor = mssql_conn.cursor()
        
        insert_sql = f"""
            INSERT INTO {TABLE_NAME} (empcode, punchtime, machineip, inoutmode, status, deviceid)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        inserted_count = 0
        for row in rows:
            # Handle datetime conversion safely
            punchtime = row[1]
            if hasattr(punchtime, 'strftime'):
                punchtime = punchtime  # pyodbc handles datetime objects well
            
            values = (
                row[0],      # empcode
                punchtime,   # punchtime
                row[2],      # machineip
                row[3],      # inoutmode
                row[4],      # status
                row[5]       # deviceid
            )
            
            try:
                mssql_cursor.execute(insert_sql, values)
                inserted_count += 1
            except pyodbc.IntegrityError:
                # Skip if primary key violation (duplicate)
                pass
            except Exception as e:
                logging.error(f"Failed to insert row {row[0]} | Error: {e}")
        
        mssql_conn.commit()
        
        # Update last punchtime from the last inserted row
        if rows:
            LAST_PUNCHTIME = rows[-1][1]
        
        logging.info(f"Successfully synced {inserted_count} new records to MSSQL.")
        
        # Cleanup
        oracle_cursor.close()
        oracle_conn.close()
        mssql_cursor.close()
        mssql_conn.close()
        
    except Exception as e:
        logging.error(f"Error during sync: {str(e)}")

if __name__ == "__main__":
    logging.info("=== Oracle to MSSQL Sync Service Started ===")
    
    create_mssql_table_if_not_exists()
    
    while True:
        try:
            sync_new_records()
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
        
        logging.info("Waiting 60 seconds for next sync...")
        time.sleep(60)
