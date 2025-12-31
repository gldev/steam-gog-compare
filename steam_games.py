import datetime
import requests
import csv
import sqlite3


def get_owned_games(KEY, steamid) -> list[dict]:
    params = {"key": KEY, "steamid": steamid, "include_appinfo": 1, "format": "json"}
    result = requests.get(
        "https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/",
        params=params,
        timeout=30,
    )
    result.raise_for_status()
    json_result = result.json()

    return json_result.get("response", {}).get("games", [])


def get_steamid(key, vanity_id) -> str:
    params = {"key": key, "vanityurl": vanity_id}
    response = requests.get(
        "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/", params=params
    )
    result = response.json()
    result_dict = result.get("response", {})
    if result_dict.get("success") != 1:
        raise RuntimeError(f"Could not resolve vanity '{vanity_id}': {result}")
    return result_dict["steamid"]


def write_csv(games: list[dict], output_path: str) -> None:
    fieldnames = ["appid", "name", "playtime_min"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for g in games:
            w.writerow(
                {
                    "appid": g.get("appid"),
                    "name": g.get("name"),
                    "playtime_min": g.get("playtime_forever", 0),
                }
            )


def create_steam_games_table(conn: sqlite3.Connection):
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS steam_games(
                appid INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                playtime_forever_min INTEGER NOT NULL DEFAULT 0,
                playtime_2weeks_min INTEGER NOT NULL DEFAULT 0,
                last_updated_utc TEXT NOT NULL
            )
        """
    )
    conn.commit()


def write_to_sql(games: list[dict], db_path: str):
    conn = sqlite3.connect(db_path)

    try:
        crate_steam_games_table(conn)

        now = datetime.datetime.now().isoformat()
        rows = []
        for g in games:
            rows.append(
                (
                    int(g.get("appid")),
                    (g.get("name") or ""),
                    int(g.get("playtime_forever", 0) or 0),
                    now,
                )
            )

        conn.executemany(
            """
            INSERT INTO steam_games (appid, name, playtime_forever_min, last_updated_utc)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(appid) DO UPDATE SET
                name=excluded.name,
                playtime_forever_min=excluded.playtime_forever_min,
                last_updated_utc=excluded.last_updated_utc
        """,
            rows,
        )
        conn.commit()
        print(f"Saved {len(rows)} games into {db_path}")

    finally:
        conn.close()
