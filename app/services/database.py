# app/services/database.py

import os
import pyodbc
import logging
from dotenv import load_dotenv

# Initialize logging
logger = logging.getLogger(__name__)
# A basic logging configuration in case it's not set up elsewhere
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# Load environment variables from .env file
load_dotenv()

class DatabaseConnector:
    """
    Handles connections to the SQL Server database using trusted connections.
    """
    def __init__(self):
        """
        Initializes the connector with settings from environment variables.
        """
        self.server = os.getenv("DB_SERVER")
        self.database = os.getenv("DB_NAME")
        self.driver = "{ODBC Driver 18 for SQL Server}"
        
        self.connection_string = self._build_connection_string()
        logger.info(f"DatabaseConnector initialized for server '{self.server}' and database '{self.database}'.")

        self._check_driver()

    def _build_connection_string(self):
        """Builds the connection string from environment variables."""
        return (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )

    def _check_driver(self):
        """Checks if the required ODBC driver is available on the system."""
        available_drivers = pyodbc.drivers()
        if self.driver.strip('{}') not in [d.strip('{}') for d in available_drivers]:
            logger.error(f"Required ODBC Driver '{self.driver}' not found.")
            logger.error(f"Available drivers: {available_drivers}")
            raise EnvironmentError(f"Required ODBC Driver '{self.driver}' not found.")

    def execute_query(self, query: str, params: tuple = None):
        """
        Connects, executes a query securely, and returns results.
        This single method handles connection, execution, and closing.
        """
        # --- FIX #1: ENHANCED LOGGING ---
        # We now log both the query and the parameters for better debugging.
        log_message = f"Executing query: {query}"
        if params:
            log_message += f" with params: {params}"
        logger.info(log_message)
        # --- END FIX #1 ---

        results = []
        try:
            # --- FIX #2: ENABLE AUTOCOMMIT ---
            # autocommit=True ensures that INSERT/UPDATE statements are saved immediately.
            with pyodbc.connect(self.connection_string, autocommit=True, timeout=30) as conn:
            # --- END FIX #2 ---
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # If the query is a SELECT statement, fetch results
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                
                # For INSERT/UPDATE/DELETE, rowcount will be > 0. For SELECT, it's often -1.
                if cursor.rowcount != -1:
                    logger.info(f"Query executed successfully, {cursor.rowcount} rows affected.")
                else:
                    logger.info(f"Query executed successfully, {len(results)} rows returned.")
                
                return results

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logger.error(f"Database query failed. SQLSTATE: {sqlstate}", exc_info=True)
            return {"error": f"Database query failed. The server said: {ex}"}
        except Exception as e:
            logger.error(f"An unexpected error occurred during query execution: {e}", exc_info=True)
            return {"error": f"An unexpected system error occurred: {e}"}

# Create a single, shared instance of the connector for the application to use.
db_connector = DatabaseConnector()