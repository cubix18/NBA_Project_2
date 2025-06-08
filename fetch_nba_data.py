import requests
import snowflake.connector

# -----------------------------
# KONFIGURACJA
# -----------------------------
API_URL = "https://api.balldontlie.io/v1/games"
API_KEY = "dc25200b-a1c5-4280-bd10-1d9bb7ad4f14"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

# Snowflake - uzupełnij swoimi danymi
SNOWFLAKE_CONFIG = {
    "user": "cubix199615",
    "password": "AbukSpox11..11",
    "account": "bdnuepx-uk12830",
    "warehouse": "COMPUTE_WH",
    "database": "NBA_DB",
    "schema": "PLAYER_DATA"
}

# -----------------------------
# FUNKCJE
# -----------------------------

def fetch_nba_games():
    """Pobierz dane meczów NBA dla sezonu 2023/24."""
    games = []
    page = 1

    params_base = {
        "start_date": "2023-10-01",
        "end_date": "2024-06-30",
        "per_page": 100
    }

    while True:
        params = {**params_base, "page": page}
        response = requests.get(API_URL, headers=HEADERS, params=params)

        if response.status_code != 200:
            raise Exception(f"Błąd pobierania danych: {response.status_code} - {response.text}")

        data = response.json()
        games_batch = data.get("data", [])
        games.extend(games_batch)

        meta = data.get("meta", {})
        next_page = meta.get("next_page")

        if not next_page:
            break

        page += 1

    return games

def connect_to_snowflake():
    """Połącz z bazą Snowflake."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    return conn

def save_to_snowflake(conn, games):
    """Zapisz dane do Snowflake (tabela: nba_games)."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nba_games (
            id INT,
            date STRING,
            home_team STRING,
            visitor_team STRING,
            home_score INT,
            visitor_score INT
        )
    """)

    for game in games:
        try:
            cursor.execute("""
                INSERT INTO nba_games (id, date, home_team, visitor_team, home_score, visitor_score)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                game["id"],
                game["date"],
                game["home_team"]["full_name"],
                game["visitor_team"]["full_name"],
                game["home_team_score"],
                game["visitor_team_score"]
            ))
        except Exception as e:
            print(f"Błąd zapisu meczu ID {game['id']}: {e}")

    cursor.close()
    conn.close()

# -----------------------------
# GŁÓWNA LOGIKA
# -----------------------------

def main():
    print("Pobieram dane sezonu 2023/24...")
    games = fetch_nba_games()
    print(f"Pobrano {len(games)} meczów.")

    print("Łączenie z Snowflake...")
    conn = connect_to_snowflake()

    print("Zapisuję dane...")
    save_to_snowflake(conn, games)

    print("Zakończono!")

if __name__ == "__main__":
    main()
