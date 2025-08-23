import pandas as pd
import unidecode
from rapidfuzz import process, fuzz

# --- Funkcja do normalizacji nazwisk ---
def normalize_name(name: str) -> str:
    if pd.isna(name):
        return ""
    return (
        unidecode.unidecode(name)  # usuwa akcenty (ć → c, é → e, ñ → n)
        .lower()                   # małe litery
        .replace(".", "")          # usuwa kropki
        .replace("-", " ")         # myślniki na spacje
        .replace("'", "")          # usuwa apostrofy
        .strip()                   # usuwa spacje z początku/końca
    )

# --- Główna logika ---
def match_players(core_csv: str, awards_csv: str, out_csv: str):
    # Wczytaj dane
    core = pd.read_csv("CO_SEASON_PLAYER_STATS_WITH_DRAFT_AF_PLAYERS.csv")
    awards = pd.read_csv("CLS_ALL_COMMON_PLAYER_INFO_PLAYERS.csv")

    # Normalizacja nazwisk
    core["name_norm"] = core["PLAYER_FULL_NAME"].apply(normalize_name)
    awards["name_norm"] = awards["PLAYER_FULL_NAME"].apply(normalize_name)


    # Dopasowania 1:1 (dokładne po normalizacji)
    merged = pd.merge(core, awards, on="name_norm", how="left", suffixes=("_core", "_awards"))
    merged["match_confidence"] = 100  # dopasowania exact = 100%

    # Szukanie graczy z awards, którzy się nie dopasowali
    unmatched_awards = awards[~awards["name_norm"].isin(merged["name_norm"])]

    # Jeśli ktoś się nie dopasował → fuzzy matching
    suggestions = []
    for name in unmatched_awards["name_norm"].unique():
        best_match, score, _ = process.extractOne(
            name, core["name_norm"].unique(), scorer=fuzz.ratio
        )
        suggestions.append((name, best_match, round(score, 2)))

    # Tworzymy DataFrame z sugestiami
    suggestions_df = pd.DataFrame(suggestions, columns=["awards_name_norm", "core_match_norm", "match_probability"])
    suggestions_df["match_probability"] = suggestions_df["match_probability"].astype(float)

    # Zapis wyników
    merged.to_csv(out_csv, index=False)
    suggestions_df.to_csv("fuzzy_suggestions.csv", index=False)

    print(f"✅ Zapisano połączoną tabelę do {out_csv}")
    print(f"ℹ️  Nierozpoznane nazwiska i sugestie z % dopasowania zapisane do fuzzy_suggestions.csv")

# --- Uruchomienie ---
if __name__ == "__main__":
    # podaj swoje ścieżki do CSV
    match_players("core_players.csv", "awards_players.csv", "merged_players.csv")