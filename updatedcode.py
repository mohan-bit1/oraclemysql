import cx_Oracle
import pyodbc
import time
import logging
from datetime import datetime

# ====================== CONFIGURATION ======================
# Oracle Configuration
ORACLE_HOST = "your_oracle_host_or_ip"           # e.g., 192.168.1.50
ORACLE_PORT = 1521
ORACLE_SERVICE_NAME = "your_actual_service_name" # e.g., ORCL, PROD, XE
ORACLE_USER = "your_oracle_username"
ORACLE_PASSWORD = "your_oracle_password"

# MSSQL Configuration
MSSQL_SERVER = "your_mssql_server"              # e.g., localhost or IP
MSSQL_DATABASE = "your_database_name"
MSSQL_USER = "your_mssql_username"              # Leave empty "" for Windows Auth
MSSQL_PASSWORD = "your_mssql_password"

TABLE_NAME = "hr_rawdata_inter"

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
    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"
    conn = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    # Optional: Set consistent date format
    conn.cursor().execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    return conn

def get_mssql_connection():
    if MSSQL_USER and MSSQL_PASSWORD:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={MSSQL_SERVER};"
            f"DATABASE={MSSQL_DATABASE};"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASSWORD};"
            "TrustServerCertificate=yes;"
        )
    else:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={MSSQL_SERVER};"
            f"DATABASE={MSSQL_DATABASE};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    return pyodbc.connect(conn_str)

def create_mssql_table_if_not_exists():
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
    conn = get_mssql_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(punchtime) FROM {TABLE_NAME}")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result and result[0] else None

def safe_to_datetime(value):
    """Convert various datetime formats safely to Python datetime object"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            # Handle ISO format: 2026-04-06T11:09:54
            if 'T' in value:
                return datetime.fromisoformat(value.replace('T', ' '))
            # Handle other common formats
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.warning(f"Failed to parse datetime: {value} | Error: {e}")
            return None
    return value

def sync_new_records():
    global LAST_PUNCHTIME
    
    try:
        if LAST_PUNCHTIME is None:
            LAST_PUNCHTIME = get_max_punchtime_mssql()
            if LAST_PUNCHTIME is None:
                LAST_PUNCHTIME = datetime(1900, 1, 1)
        
        oracle_conn = get_oracle_connection()
        oracle_cursor = oracle_conn.cursor()
        
        query = """
            SELECT empcode, punchtime, machineip, inoutmode, status, deviceid 
            FROM hr_rawdata_inter 
            WHERE punchtime > :last_time
            ORDER BY punchtime ASC
        """
        
        oracle_cursor.execute(query, {'last_time': LAST_PUNCHTIME})
        rows = oracle_cursor.fetchall()
        
        if not rows:
            logging.info("No new records to sync.")
            return
        
        mssql_conn = get_mssql_connection()
        mssql_cursor = mssql_conn.cursor()
        
        insert_sql = f"""
            INSERT INTO {TABLE_NAME} (empcode, punchtime, machineip, inoutmode, status, deviceid)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        inserted_count = 0
        for row in rows:
            punchtime = safe_to_datetime(row[1])   # Safe conversion here
            
            if punchtime is None:
                logging.warning(f"Skipping row with invalid punchtime: {row[0]}")
                continue
            
            values = (
                row[0],      # empcode
                punchtime,   # punchtime as datetime object
                row[2],      # machineip
                row[3],      # inoutmode
                row[4],      # status
                row[5]       # deviceid
            )
            
            try:
                mssql_cursor.execute(insert_sql, values)
                inserted_count += 1
            except pyodbc.IntegrityError:
                pass  # duplicate record
            except Exception as e:
                logging.error(f"Insert failed for empcode={row[0]} | Error: {e}")
        
        mssql_conn.commit()
        
        if rows:
            LAST_PUNCHTIME = safe_to_datetime(rows[-1][1])
        
        logging.info(f"Successfully synced {inserted_count} new records.")
        
    except Exception as e:
        logging.error(f"Error during sync: {e}")
    finally:
        try:
            oracle_cursor.close()
            oracle_conn.close()
        except:
            pass
        try:
            mssql_cursor.close()
            mssql_conn.close()
        except:
            pass

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
