import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment


# ---------------------------
# Pomocnicza funkcja do pobierania tabel (obsługuje komentarze <!-- -->)
# ---------------------------
def fetch_tables(url):
    response = requests.get(url)
    html = response.text

    soup = BeautifulSoup(html, "lxml")

    # szukamy tabel w komentarzach
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if "<table" in comment:
            try:
                tables = pd.read_html(comment)
                if tables:
                    return tables, comment
            except Exception:
                continue

    # fallback: normalne parsowanie
    return pd.read_html(html), html


# ---------------------------
# Scraper dla różnych nagród
# ---------------------------

def scrape_mvp():
    url = "https://www.basketball-reference.com/awards/mvp.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "MVP"
    return df


def scrape_dpoy():
    url = "https://www.basketball-reference.com/awards/dpoy.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "Defensive Player of the Year"
    return df


def scrape_sixth_man():
    url = "https://www.basketball-reference.com/awards/smoy.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "Sixth Man of the Year"
    return df


def scrape_mip():
    url = "https://www.basketball-reference.com/awards/mip.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "Most Improved Player"
    return df


def scrape_roy():
    url = "https://www.basketball-reference.com/awards/roy.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "Rookie of the Year"
    return df


def scrape_all_nba():
    url = "https://www.basketball-reference.com/awards/all_league.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "All-NBA Team"
    return df


def scrape_all_defensive():
    url = "https://www.basketball-reference.com/awards/all_defense.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "All-Defensive Team"
    return df


def scrape_all_rookie():
    url = "https://www.basketball-reference.com/awards/all_rookie.html"
    tables, _ = fetch_tables(url)
    df = tables[0]
    df["AWARD"] = "All-Rookie Team"
    return df


# ---------------------------
# Budowanie pełnego DataFrame
# ---------------------------
def build(core_csv=None):
    dfs = [
        scrape_mvp(),
        scrape_dpoy(),
        scrape_sixth_man(),
        scrape_mip(),
        scrape_roy(),
        scrape_all_nba(),
        scrape_all_defensive(),
        scrape_all_rookie()
    ]

    df = pd.concat(dfs, ignore_index=True)

    # Jeżeli mamy core CSV z zawodnikami, można połączyć po imieniu/nazwisku
    if core_csv:
        core = pd.read_csv(core_csv)
        df = df.merge(
            core[["PLAYER_FULL_NAME", "DRAFT_NUMBER", "DRAFT_YEAR"]],
            left_on="Player",
            right_on="PLAYER_FULL_NAME",
            how="left"
        )
        df = df.drop(columns=["PLAYER_FULL_NAME"])

    return df


# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Ścieżka wyjściowego pliku CSV")
    parser.add_argument("--core", required=False, help="Opcjonalnie: plik CSV z danymi zawodników (do draft info)")
    args = parser.parse_args()

    df = build(core_csv=args.core)
    df.to_csv(args.out, index=False)
    print(f"Plik zapisany: {args.out}")
