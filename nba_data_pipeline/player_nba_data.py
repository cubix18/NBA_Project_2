import requests
import snowflake.connector
import time

# ---------------------------
# KONFIGURACJA
# ---------------------------

API_KEY = "dc25200b-a1c5-4280-bd10-1d9bb7ad4f14"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

SNOWFLAKE_CONFIG = {
    "user": "cubix199615",
    "password": "AbukSpox11..005",
    "account": "bdnuepx-uk12830",
    "warehouse": "COMPUTE_WH",
    "database": "NBA_DB",
    "schema": "PLAYER_DATA"
}

# ---------------------------
# POBIERANIE DANYCH
# ---------------------------

def fetch_teams():
    url = "https://api.balldontlie.io/v1/teams"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Nie udało się pobrać drużyn: {response.status_code}")
    return response.json().get("data", [])

def fetch_all_players():
    all_players = []
    page = 1
    per_page = 100

    while True:
        print(f"Pobieranie graczy - strona {page}")
        url = f"https://api.balldontlie.io/v1/players"
        params = {"page": page, "per_page": per_page}
        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code == 429:
            print("Limit API. Czekam 5 sekund...")
            time.sleep(5)
            continue

        if response.status_code != 200:
            raise Exception(f"Błąd przy pobieraniu graczy: {response.status_code}")

        data = response.json().get("data", [])
        if not data:
            break

        all_players.extend(data)
        page += 1
        time.sleep(1)

    return all_players

# ---------------------------
# ZAPIS DO SNOWFLAKE
# ---------------------------

def connect_to_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def save_teams_to_snowflake(conn, teams):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nba_teams (
            id INT,
            abbreviation STRING,
            city STRING,
            conference STRING,
            division STRING,
            full_name STRING,
            name STRING
        )
    """)
    for t in teams:
        cursor.execute("""
            INSERT INTO nba_teams (id, abbreviation, city, conference, division, full_name, name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            t["id"], t["abbreviation"], t["city"], t["conference"],
            t["division"], t["full_name"], t["name"]
        ))
    cursor.close()

def save_players_to_snowflake(conn, players):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nba_players (
            id INT,
            first_name STRING,
            last_name STRING,
            position STRING,
            height_feet INT,
            height_inches INT,
            weight_pounds INT,
            team_id INT,
            team_abbreviation STRING,
            team_full_name STRING
        )
    """)
    for p in players:
        team = p.get("team", {})
        cursor.execute("""
            INSERT INTO nba_players (
                id, first_name, last_name, position,
                height_feet, height_inches, weight_pounds,
                team_id, team_abbreviation, team_full_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            p["id"],
            p["first_name"],
            p["last_name"],
            p["position"],
            p.get("height_feet"),
            p.get("height_inches"),
            p.get("weight_pounds"),
            team.get("id"),
            team.get("abbreviation"),
            team.get("full_name")
        ))
    cursor.close()

# ---------------------------
# MAIN
# ---------------------------

def main():
    print("Pobieranie drużyn NBA...")
    teams = fetch_teams()
    print(f"Pobrano {len(teams)} drużyn.")

    print("Pobieranie graczy NBA...")
    players = fetch_all_players()
    print(f"Pobrano {len(players)} graczy.")

    print("Łączenie z Snowflake...")
    conn = connect_to_snowflake()

    print("Zapisywanie drużyn...")
    save_teams_to_snowflake(conn, teams)

    print("Zapisywanie graczy...")
    save_players_to_snowflake(conn, players)

    conn.close()
    print("Zakończono!")

if __name__ == "__main__":
    main()
