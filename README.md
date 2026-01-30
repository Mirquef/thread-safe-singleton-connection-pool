# Resilient PostgreSQL Connection Pool for Python

This repository provides a production-ready implementation of a **Postgresql Connection Pool** using Python. It is designed to be efficient, thread-safe, and resilient, making it ideal for multi-threaded applications and microservices.

## 🚀 Key Features

- **Thread-Safe Singleton Pattern**: Ensures a single pool instance across the entire application using the Double-Checked Locking pattern.
- **Context Manager Support**: Uses the `with` statement for automatic resource management (opening/committing/closing connections).
- **Azure Identity Integration**: Supports passwordless authentication using `DefaultAzureCredential`, perfect for Azure-hosted environments.
- **Reactive Token Refresh (Hot Swap)**: Automatically detects expired tokens, refreshes them, and rotates the connection pool without downtime.
- **Thread-Local Storage**: Guarantees that each thread handles its own connection safely.

## 🛠️ Tech Stack

- **Language:** Python 3.8+
- **Driver:** `psycopg2-binary`
- **Identity:** `azure-identity` (Optional, for Azure AD/Entra ID support)

## ⚙️ Environment Variables

The implementation relies on the following environment variables for configuration:

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | Database server host | Required |
| `DB_PORT` | Database server port | 5432 |
| `DB_USER` | Database user | Required |
| `DB_NAME` | Database name | Required |
| `DB_PASSWORD` | Database password (if not using Identity) | Optional |
| `IDENTITY_ENABLED` | Set to `True` to use Azure Identity | `False` |
| `DB_MIN_CONN` | Minimum connections in pool | 1 |
| `DB_MAX_CONN` | Maximum connections in pool | 20 |

## 💻 Usage Example

```python
from db_connection import PostgresqlConnection

db = PostgresqlConnection()

def fetch_data():
    try:
        # The Context Manager handles getconn() and putconn() automatically
        with db as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users LIMIT 5;")
                return cur.fetchall()
    except Exception as e:
        print(f"Error: {e}")

# Shutdown the pool gracefully when the app closes
PostgresqlConnection.close_all()
🏗️ Architectural Patterns Used
Singleton: Guarantees a single point of access to the pool.

Double-Checked Locking: Ensures thread safety during initialization.

Resource Ownership: Proper cleanup of connections even in case of exceptions.

Retry/Refresh Pattern: Handles token expiration reactively by recreating the pool.
