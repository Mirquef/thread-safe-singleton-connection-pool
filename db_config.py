import logging
import os
import threading

import psycopg2
from psycopg2 import InterfaceError, OperationalError, pool

from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()


class PostgresqlConnection:
    _instance = None
    _lock = threading.Lock()

    # We use thread local to find out which connection the current thread has.
    _thread_local = threading.local()  # This is the local storage for each thread

    # This is the singleton instance
    # This is the first instance that is created
    # The condition cls._instance is None is used to ensure that only one instance is created
    # The double-checked locking pattern is used to ensure that the instance is created only once
    # Once the instance is created, the __init__ method is called
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:  # This is the lock for the singleton instance. It is used to ensure that only one instance is created avoiding race conditions
                if (
                    cls._instance is None
                ):  # This is the double-checked locking pattern. This will be checked again after the lock is released
                    cls._instance = super(PostgresqlConnection, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        # We avoid re-initialization in the Singleton
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return
            self._init_config()
            self._pool = None  # The pool is initialized
            self._create_new_pool()  # Create the initial pool
            self._initialized = True

    def _init_config(self):
        """Loads and validates configuration."""
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.user = os.getenv("DB_USER")
        self.dbname = os.getenv("DB_NAME")
        self.password = os.getenv("DB_PASSWORD")  # Optional if the Azure identity is used
        self.identity_enabled = os.getenv("IDENTITY_ENABLED", "False").lower() == "true"

        self.min_conn = int(os.getenv("DB_MIN_CONN", "1"))
        self.max_conn = int(os.getenv("DB_MAX_CONN", "20"))

        if not all([self.host, self.user, self.dbname]):
            raise ValueError("Variables are missing: DB_HOST, DB_USER or DB_NAME")

    def _get_azure_token(self):
        """Obtains a fresh Azure Identity token."""
        try:
            credential = DefaultAzureCredential()
            # The scope for Azure SQL/PostgreSQL
            token_bytes = credential.get_token(
                "https://ossrdbms-aad.database.windows.net/.default"
            )
            return token_bytes.token
        except ClientAuthenticationError as e:
            logging.critical(f"Error obteniendo token de Azure: {e}")
            raise

    def _create_new_pool(self):
        """
        Creates a new instance of the pool with fresh credentials.
        Does not use internal lock, must be called within a block with lock.
        """
        try:
            password = self.password
            if self.identity_enabled:
                logging.info("[Info] Getting Azure Identity token for the new pool...")
                password = self._get_azure_token()

            conn_params = {
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "dbname": self.dbname,
                "password": password,
                "sslmode": "require",
                "connect_timeout": 10,
                "options": "-c statement_timeout=300000",
            }

            # Creates a new pool of connections before closing the old one
            new_pool = psycopg2.pool.ThreadedConnectionPool(
                self.min_conn, self.max_conn, **conn_params
            )

            # If there was an old pool, close it
            if self._pool:
                logging.info("[Info] Closing old pool...")
                try:
                    self._pool.closeall()
                except Exception as e:
                    logging.warning(
                        f"[Warning] Error closing old pool (normal if there are active connections): {e}"
                    )

            # Hot swap the pool (replace the old pool with the new one)
            self._pool = new_pool  # This is a memory reference change, so any new thread that requests a connection will use the new pool
            logging.info(
                f"[Info] New connection pool created successfully (PID: {os.getpid()})."
            )

        except Exception as e:
            logging.critical(
                f"[Critical] Failed to create connection pool: {e}", exc_info=True
            )
            raise

    def _refresh_pool(self):
        """
        Forces the recreation of the pool in a thread-safe manner.
        """
        with self._lock:
            # We check if another thread has already refreshed the pool while we were waiting for the lock.
            logging.warning(
                "[Warning] Starting pool refresh process due to token expiration..."
            )
            self._create_new_pool()

    def __enter__(self):
        """
        Gets a connection. If authentication fails, refreshes the pool and retries.
        This is a Retry pattern. It retries the connection if it fails (Reactive).
        Returns:
            conn: A connection object from the pool
        """
        # Local reference to the current pool for detection of changes during execution
        current_pool_ref = self._pool

        try:
            if not current_pool_ref:
                with self._lock:
                    if not self._pool:
                        self._create_new_pool()

            conn = self._pool.getconn()  # Get a connection from the pool
            self._thread_local.conn = (
                conn  # Store the connection in thread-local storage
            )
            self._thread_local.pool_origin = self._pool  # Record the pool origin
            return conn

        except (OperationalError, InterfaceError) as e:
            error_msg = str(e)
            # Detect authentication errors
            is_auth_error = (
                "password authentication failed" in error_msg
                or "FATAL" in error_msg
                or "expired" in error_msg
            )

            # If authentication error and identity is enabled, refresh the pool and retry
            if self.identity_enabled and is_auth_error:
                logging.warning(
                    f"[Warning] Connection error detected ({error_msg}). Attempting to refresh token and pool."
                )

                # Refresh the pool
                self._refresh_pool()

                # Single retry
                try:
                    conn = self._pool.getconn()
                    self._thread_local.conn = conn
                    self._thread_local.pool_origin = self._pool
                    return conn
                except Exception as retry_e:
                    logging.error(
                        "[Error] Failed to obtain connection even after refreshing the pool."
                    )
                    raise retry_e
            else:
                logging.error(f"[Error] Error obtaining connection: {e}")
                raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Returns the connection to the pool or closes it if the pool has changed.
        This function helps us to handle the connection pool and close the connection when the token expires.
        This follows the "Resource Ownership" pattern. If the
        """
        conn = getattr(self._thread_local, "conn", None)
        origin_pool = getattr(
            self._thread_local, "pool_origin", None
        )  # Get the pool origin

        if conn:
            try:
                # If there was an error, decide whether to close the connection
                close_conn = exc_type is not None

                if close_conn:
                    # If there was an exception in the 'with' block, discard this connection
                    origin_pool.putconn(conn, close=True)
                else:
                    # Commit before returning if everything went well
                    conn.commit()

                    # CRITICAL ATTEMPT: Return to the correct pool
                    # If the global pool (self._pool) changed while we were using the connection,
                    # origin_pool.putconn will work, but we are returning a connection
                    # to a pool that is theoretically already “closed/old.”
                    # Psycopg2 allows this, but if we call closeall() on the old pool,
                    # this connection will die.

                    if origin_pool != self._pool:
                        logging.info(
                            "[Info] Connection belongs to an old pool. Manually closing it instead of returning it."
                        )
                        conn.close()  # Close the connection manually
                    else:
                        origin_pool.putconn(conn)  # Return the connection to the pool

            except Exception as e:
                # This captures the case where we try to return a connection to a closed pool
                logging.warning(
                    f"[Warning] Could not return connection to pool (possibly pool rotated): {e}"
                )
                try:
                    conn.close()
                except:
                    pass
            finally:
                # Thread local cleanup
                self._thread_local.conn = None
                self._thread_local.pool_origin = None

    @classmethod
    def close_all(cls):
        if cls._instance and cls._instance._pool:
            with cls._lock:
                logging.info("[Info] Closing all connections and destroying the pool.")
                cls._instance._pool.closeall()
                cls._instance._pool = None


# --- Use Case ---
if __name__ == "__main__":
    db = PostgresqlConnection()

    print("--- Attemp 1 ---")
    try:
        with db as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print(f"Consulta exitosa: {cur.fetchone()}")
    except Exception as e:
        print(f"Error en main: {e}")

    # Here you could simulate expiration or force a refresh if you wanted to test the logic.
    # db._refresh_pool()

    print("--- Attempt 2 (post-expiration) ---")
    try:
        with db as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print(f"Consulta exitosa: {cur.fetchone()}")
    except Exception as e:
        print(f"Error en main 2: {e}")

    print("--- Attempt 3 (post-expiration) ---")
    try:
        center = "Colombia"
        with db as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT day_off FROM holiday WHERE LOWER(center) = %s",
                    (center.lower(),),
                )
                holidays = cur.fetchall()
                print([row[0] for row in holidays])
    except Exception as e:
        print(f"Error en main 3: {e}")

    PostgresqlConnection.close_all()
