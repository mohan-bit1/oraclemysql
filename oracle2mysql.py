import cx_Oracle
import pymysql
import time
import logging
from datetime import datetime

# ====================== CONFIGURATION ======================
ORACLE_DSN = "your_oracle_host:1521/your_service_name"  # or use TNS name
ORACLE_USER = "your_oracle_username"
ORACLE_PASSWORD = "your_oracle_password"

MYSQL_HOST = "localhost"
MYSQL_USER = "your_mysql_username"
MYSQL_PASSWORD = "your_mysql_password"
MYSQL_DB = "your_mysql_database"

TABLE_NAME = "hr_rawdata_inter"

# Last sync time (will be updated automatically)
LAST_PUNCHTIME = None

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("oracle_to_mysql_sync.log"),
        logging.StreamHandler()
    ]
)

def get_oracle_connection():
    return cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)

def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def create_mysql_table_if_not_exists():
    """Create the target table in MySQL if it doesn't exist"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS hr_rawdata_inter (
        empcode CHAR(8) NOT NULL,
        punchtime DATETIME NOT NULL,
        machineip CHAR(20) NOT NULL,
        inoutmode CHAR(1) NOT NULL,
        status CHAR(1) NOT NULL DEFAULT 'P',
        deviceid CHAR(8) NOT NULL,
        PRIMARY KEY (empcode, punchtime)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("MySQL table 'hr_rawdata_inter' is ready.")

def get_max_punchtime_mysql():
    """Get the latest punchtime from MySQL"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(punchtime) as max_time FROM {TABLE_NAME}")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return result['max_time'] if result and result['max_time'] else None

def sync_new_records():
    global LAST_PUNCHTIME
    
    try:
        # Get last synced time from MySQL (first run) or use cached value
        if LAST_PUNCHTIME is None:
            LAST_PUNCHTIME = get_max_punchtime_mysql()
            if LAST_PUNCHTIME is None:
                LAST_PUNCHTIME = datetime(1900, 1, 1)  # Very old date for first run
        
        oracle_conn = get_oracle_connection()
        oracle_cursor = oracle_conn.cursor()
        
        # Query to fetch only NEW records (punchtime > last synced time)
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
        
        mysql_conn = get_mysql_connection()
        mysql_cursor = mysql_conn.cursor()
        
        insert_sql = f"""
            INSERT IGNORE INTO {TABLE_NAME} 
            (empcode, punchtime, machineip, inoutmode, status, deviceid)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        inserted_count = 0
        for row in rows:
            # Convert Oracle datetime to Python datetime if needed
            punchtime = row[1]
            if hasattr(punchtime, 'strftime'):  # cx_Oracle returns datetime object
                punchtime = punchtime.strftime('%Y-%m-%d %H:%M:%S')
            
            values = (
                row[0],          # empcode
                punchtime,       # punchtime
                row[2],          # machineip
                row[3],          # inoutmode
                row[4],          # status
                row[5]           # deviceid
            )
            
            try:
                mysql_cursor.execute(insert_sql, values)
                inserted_count += 1
            except Exception as e:
                logging.error(f"Failed to insert row: {row} | Error: {e}")
        
        mysql_conn.commit()
        
        # Update last punchtime
        if rows:
            LAST_PUNCHTIME = rows[-1][1]  # Last row's punchtime
        
        logging.info(f"Successfully synced {inserted_count} new records.")
        
        oracle_cursor.close()
        oracle_conn.close()
        mysql_cursor.close()
        mysql_conn.close()
        
    except Exception as e:
        logging.error(f"Error during sync: {e}")

if __name__ == "__main__":
    logging.info("=== Oracle to MySQL Sync Service Started ===")
    
    # Create table if not exists
    create_mysql_table_if_not_exists()
    
    while True:
        try:
            sync_new_records()
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
        
        logging.info("Waiting 60 seconds for next sync...")
        time.sleep(60)  # Run every 1 minute
