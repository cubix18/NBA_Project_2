import requests
import snowflake.connector
import time  

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
    "password": "AbukSpox11..005",
    "account": "bdnuepx-uk12830",
    "warehouse": "COMPUTE_WH",
    "database": "NBA_DB",
    "schema": "PLAYER_DATA"
}

# -----------------------------
# FUNKCJE
# -----------------------------

import time  # dodaj na górze pliku

def fetch_all_nba_games_from_2020():
    """Pobierz mecze NBA od sezonu 2020 do dziś."""
    games = []
    cursor = None
    per_page = 100
    target_seasons = [2024]  # tylko 2024

    while True:
        params = {
            "per_page": per_page,
            "seasons[]": target_seasons
        }
        if cursor:
            params["cursor"] = cursor

        response = requests.get(API_URL, headers=HEADERS, params=params)

        if response.status_code == 429:
            print("Przekroczono limit zapytań. Czekam 5 sekund...")
            time.sleep(5)
            continue  # spróbuj ponownie

        if response.status_code != 200:
            raise Exception(f"Błąd pobierania danych: {response.status_code} - {response.text}")

        data = response.json()
        batch = data.get("data", [])
        meta = data.get("meta", {})
        cursor = meta.get("next_cursor")

        games.extend(batch)

        if not cursor:
            break

        time.sleep(1)  # <-- opóźnienie między kolejnymi żądaniami

    return games


def connect_to_snowflake():
    """Połączenie z Snowflake."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    return conn

def save_to_snowflake(conn, games):
    """Zapisz dane do Snowflake."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nba_games_full (
            id INT,
            date STRING,
            datetime STRING,
            season INT,
            status STRING,
            period INT,
            time STRING,
            postseason BOOLEAN,
            home_team_score INT,
            visitor_team_score INT,
            home_team_id INT,
            home_team_name STRING,
            home_team_abbreviation STRING,
            visitor_team_id INT,
            visitor_team_name STRING,
            visitor_team_abbreviation STRING
        )
    """)

    for game in games:
        try:
            cursor.execute("""
                INSERT INTO nba_games_full (
                    id, date, datetime, season, status, period, time, postseason,
                    home_team_score, visitor_team_score,
                    home_team_id, home_team_name, home_team_abbreviation,
                    visitor_team_id, visitor_team_name, visitor_team_abbreviation
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                game["id"],
                game["date"],
                game.get("datetime"),
                game["season"],
                game["status"],
                game["period"],
                game["time"],
                game["postseason"],
                game["home_team_score"],
                game["visitor_team_score"],
                game["home_team"]["id"],
                game["home_team"]["full_name"],
                game["home_team"]["abbreviation"],
                game["visitor_team"]["id"],
                game["visitor_team"]["full_name"],
                game["visitor_team"]["abbreviation"]
            ))
        except Exception as e:
            print(f"Błąd zapisu meczu ID {game['id']}: {e}")

    cursor.close()
    conn.close()

# -----------------------------
# GŁÓWNA LOGIKA
# -----------------------------

def main():
    print("Pobieram dane meczów NBA od 2000 roku...")
    games = fetch_all_nba_games_from_2020()
    print(f"Pobrano {len(games)} meczów.")

    print("Łączenie z Snowflake...")
    conn = connect_to_snowflake()

    print("Zapisuję dane do bazy...")
    save_to_snowflake(conn, games)

    print("Zakończono!")

if __name__ == "__main__":
    main()
