import os
import argparse

import steam_games
import gog_games

from dotenv import load_dotenv


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="steam-gog",
        description="Export Steam library and match against GOG (GOGDB, GOG API to be considered later).",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # ---- steam ----
    p_steam = sub.add_parser("steam", help="Export Steam library (CSV or SQLite).")
    g = p_steam.add_mutually_exclusive_group(required=True)
    g.add_argument("--vanity", help="Steam vanity slug (e.g. meme_sommelier)")
    g.add_argument("--steamid", help="SteamID64")

    p_steam.add_argument("--out-csv", default="steam_games.csv", help="CSV output path")
    p_steam.add_argument("--sqlite", help="SQLite DB path (if set, saves to DB)")
    p_steam.add_argument("--print-sample", type=int, help="Print N rows instead of saving")

    # ---- gog ----
    p_gog = sub.add_parser("gog", help="GOG related steps.")
    gog_sub = p_gog.add_subparsers(dest="gog_cmd", required=True)

    p_match = gog_sub.add_parser("match", help="Match Steam games to GOG product ids (via GOGDB).")
    p_match.add_argument(
        "--sqlite-db", required=True, help="SQLite DB path that contains steam_games table"
    )
    p_match.add_argument(
        "--limit",
        type=int,
        help="Limit number of Steam games to process (for testing)",
    )

    return p


def cmd_steam(args: argparse.Namespace) -> None:
    load_dotenv()
    key = os.getenv("KEY")
    if not key:
        raise RuntimeError("Could not load steam api key (KEY) from environment/.env")

    steamid = args.steamid
    if args.vanity and not steamid:
        steamid = steam_games.get_steamid(key, vanity_id=args.vanity)

    games = steam_games.get_owned_games(key, steamid=steamid)

    if args.print_sample:
        print(f"Total games: {len(games)}")
        for g in games[: args.print_sample]:
            print(g.get("appid"), g.get("name"), g.get("playtime_forever", 0))
        return

    if args.sqlite:
        steam_games.write_to_sql(games, args.sqlite)
    else:
        steam_games.write_csv(games, args.out_csv)


def cmd_gog_match(args: argparse.Namespace) -> None:
    db_path = args.sqlite_db
    if not os.path.exists(db_path):
        raise RuntimeError(f"No database found in path: {db_path}")
    gog_games.look_for_games(db_path, limit=args.limit)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "steam":
        cmd_steam(args)
        return

    if args.cmd == "gog":
        if args.gog_cmd == "match":
            cmd_gog_match(args)
            return

    raise RuntimeError("Unknown command")


if __name__ == "__main__":
    main()
