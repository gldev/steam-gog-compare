import sqlite3
import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup


def init_db(conn: sqlite3.Connection):
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS gog_games(
                id INTEGER PRIMARY KEY,
                gog_id INTEGER UNIQUE,
                name TEXT NOT NULL,
                price REAL NOT NULL DEFAULT 0,
                steamgame INTEGER UNIQUE,
                last_updated_utc TEXT NOT NULL,
                found INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (steamgame) REFERENCES steam_games(appid)
            )
        """
    )
    conn.commit()


def parse_gogdb_search_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("#product-table")
    if not table:
        return []
    results = []
    rows = table.select("tr")
    for row in rows:
        if row.find("th"):
            continue
        id_el = row.select_one("td.col-id a")
        name_el = row.select_one("td.col-name a")
        type_el = row.select_one("td.col-type")
        if not id_el or not name_el:
            continue
        product_id = id_el.get_text(strip=True)
        name = name_el.get_text(strip=True)
        ptype = type_el.get_text(strip=True) if type_el else ""
        results.append(
            {
                "product_id": product_id,
                "name": name,
                "type": ptype,
                "gogdb_path": name_el.get("href"),
            }
        )
    return results


def search_gogdb(name):
    params = {
        "search": name,
    }
    result = requests.get("https://www.gogdb.org/products", params=params, timeout=10)
    result.raise_for_status()
    return result


def get_games_list_from_db(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT appid, name
            FROM steam_games
            ORDER BY playtime_forever_min DESC
        """)
        games = cur.fetchall()
    finally:
        conn.close()

    return games


def look_for_games(db_path: str, limit: int | None = None):
    conn = sqlite3.connect(db_path)

    games = get_games_list_from_db(db_path)

    if len(games) == 0:
        print("No games found in steam_games table")
        return

    if limit is not None:
        games = games[:limit]

    try:
        init_db(conn)
        print("Steam games to process:", len(games))
        for app_id, steam_name in games:
            time.sleep(2)
            result = search_gogdb(steam_name)
            if not result.text:
                print("Nothing was found for game: ", steam_name)
                continue
            result_list = parse_gogdb_search_html(result.text)
            now = datetime.now().isoformat()

            games_only = [r for r in result_list if (r.get("type") or "").strip().lower() == "game"]
            if len(games_only) == 0:
                print("Results not found for: ", steam_name)
                gog_name = steam_name
                gog_id = None
                found = 0
            else:
                gog_db_result = games_only[0]  # Only first result for now
                gog_id = int(gog_db_result["product_id"])
                gog_name = gog_db_result["name"]
                found = 1

            try:
                conn.execute(
                    """
                    INSERT INTO gog_games (gog_id, name, steamgame, last_updated_utc, found)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(steamgame) DO UPDATE SET
                        gog_id=excluded.gog_id,
                        name=excluded.name,
                        last_updated_utc=excluded.last_updated_utc,
                        found=excluded.found
                """,
                    (gog_id, gog_name, app_id, now, found),
                )
                conn.commit()
                print(f"  Saved GOG {gog_id} {gog_name}")
            except sqlite3.IntegrityError:
                print(f"  GOG ID already used ({gog_id}), skipping for: {steam_name}")
                conn.execute(
                    """
                    INSERT INTO gog_games (name, steamgame, last_updated_utc, found)
                    VALUES (?, ?, ?, 0)
                    ON CONFLICT(steamgame) DO UPDATE SET
                        last_updated_utc=excluded.last_updated_utc,
                        found=0
                """,
                    (steam_name, app_id, now),
                )
                conn.commit()

    finally:
        conn.close()
