import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo

# Lista zawodników (możesz wkleić swoją listę)
player_names = [
'Mike James',
'George Johnson',
'Bear, The Body Hoffman',
'Walker Russell',
'Willie Jones'
]

# Funkcja pobierająca dane gracza
def get_player_data(name):
    search = players.find_players_by_full_name(name)
    if not search:
        return None
    player_id = search[0]['id']
    info = commonplayerinfo.CommonPlayerInfo(player_id=player_id).get_data_frames()[0]
    return {
        "PLAYER_NAME": name,
        "DRAFT_YEAR": info.get("DRAFT_YEAR", [None])[0],
        "TEAM": info.get("TEAM_NAME", [None])[0],
        "COLLEGE": info.get("COLLEGE", [None])[0],
        "PICK_NUMBER": info.get("DRAFT_NUMBER", [None])[0],
        "POSITION": info.get("POSITION", [None])[0],
        "HEIGHT": info.get("HEIGHT", [None])[0],
        "WEIGHT": info.get("WEIGHT", [None])[0],
        "BIRTH_DATE": info.get("BIRTHDATE", [None])[0],
        "COUNTRY": info.get("COUNTRY", [None])[0],
        "TEAM_ABBREVIATION": info.get("TEAM_ABBREVIATION", [None])[0],
        "TEAM_CITY": info.get("TEAM_CITY", [None])[0]
    }

# Pobieranie danych dla całej listy
data = []
for name in player_names:
    row = get_player_data(name)
    if row:
        data.append(row)

# Tworzymy DataFrame
df = pd.DataFrame(data)

# Zapis do pliku CSV
df.to_csv("nba_players_data.csv", index=False)

print("Gotowe! Dane zapisane do nba_players_data.csv")
