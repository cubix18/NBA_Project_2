import snowflake.connector

SNOWFLAKE_CONFIG = {
    "user": "cubix199615",
    "password": "AbukSpox11..005",
    "account": "bdnuepx-uk12830",  # np. ab12345.eu-central-1
    "warehouse": "COMPUTE_WH",
    "database": "NBA_DB",
    "schema": "PLAYER_DATA"
}

try:
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print("Połączenie nawiązane pomyślnie!")
    conn.close()
except Exception as e:
    print(f"Błąd połączenia: {e}")
