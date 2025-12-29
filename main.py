import datetime
import requests
import os
import csv
import argparse
import sqlite3
from dotenv import load_dotenv


def get_games(KEY, steamid) -> list[dict]:
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


def init_db(conn: sqlite3.Connection):
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
        init_db(conn)

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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Exports your steam library to CSV / Sqlite and lets you load gog prices"
    )
    group = p.add_mutually_exclusive_group()
    group.add_argument("--vanity", help="Vanity slug Eg. meme_sommelier (steam alias)")
    group.add_argument("--steamid", help="SteamID64")

    p.add_argument("--sqlite", help="Indicates that the result will be sabed to a sqlite database")
    p.add_argument(
        "--out_csv", default="steam_csv.csv", type=str, help="Output route for the csv file"
    )
    p.add_argument(
        "--print_sample",
        type=int,
        help="Skips saving to anything and just shows the number of rows you indicate",
    )

    return p.parse_args()


def main():
    load_dotenv()
    KEY = os.getenv("KEY")

    if not KEY:
        raise RuntimeError("Could not load steam api key")

    args = parse_args()

    vanity = args.vanity
    steamid64 = args.steamid

    if not vanity and not steamid64:
        raise RuntimeError("Vanity or steamid must be provided")

    if vanity and not steamid64:
        steamid64 = get_steamid(KEY, vanity_id=vanity)

    games = get_games(KEY, steamid=steamid64)

    if args.print_sample:
        print(f"Total games: {len(games)}")
        for g in games[: args.print_sample]:
            print(g.get("appid"), g.get("name"), g.get("playtime_forever", 0))
        return

    if args.sqlite is not None:
        write_to_sql(games, args.sqlite)
    else:
        write_csv(games, args.out_csv)


if __name__ == "__main__":
    main()
