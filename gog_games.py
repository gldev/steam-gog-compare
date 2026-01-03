import re
import sys
import json
import tarfile
import sqlite3
import requests
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup


def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gogdb_products(
            gog_id INTEGER PRIMARY KEY,
            title TEXT,
            type TEXT,
            slug TEXT,
            raw_json TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS gogdb_prices(
            gog_id INTEGER NOT NULL,
            country TEXT,
            currency TEXT,
            base_price REAL,
            final_price REAL,
            discount_pct REAL,
            raw_json TEXT,
            PRIMARY KEY (gog_id, country, currency),
            FOREIGN KEY (gog_id) REFERENCES gogdb_products(gog_id)
        )
    """)

    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS gogdb_games(
                id INTEGER PRIMARY KEY,
                gog_id INTEGER UNIQUE,
                name TEXT NOT NULL,
                price REAL NOT NULL DEFAULT 0,
                latest_discounted_price REAL NOT NULL DEFAULT 0,
                steamgame INTEGER UNIQUE,
                last_updated_utc TEXT NOT NULL,
                found INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (steamgame) REFERENCES steam_games(appid)
            )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gogdb_products_title ON gogdb_products(title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gogdb_prices_gog_id ON gogdb_prices(gog_id)")
    conn.commit()


def _normalize_title(s: str) -> str:
    s = s.lower()
    s = s.replace("™", "").replace("®", "").replace("©", "")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9\s:.-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def seed_and_match_gogdb_games_by_name_safe(
    db_path: str,
    like_min_len: int = 6,
) -> dict:
    """
    Crea/actualiza gogdb_games desde steam_games y asigna gog_id usando matching seguro por nombre.
    Reglas:
      - No sobrescribe gog_id existente.
      - Solo considera productos GOGDB tipo 'game' (o type NULL).
      - LIKE solo si el candidato es único (para minimizar false positives).
    Devuelve métricas básicas.
    """

    conn = sqlite3.connect(db_path)

    now = datetime.now().isoformat()
    metrics = {
        "seed_inserted": 0,
        "matched_exact": 0,
        "matched_norm_exact": 0,
        "matched_like_unique": 0,
        "skipped_like_ambiguous": 0,
        "still_unmatched": 0,
    }

    cur = conn.cursor()

    # 1) Seed
    cur.execute(
        """
        INSERT INTO gogdb_games(steamgame, name, found, last_updated_utc, price, latest_discounted_price)
        SELECT sg.appid, sg.name, 0, ?, 0, 0
        FROM steam_games sg
        WHERE NOT EXISTS (
            SELECT 1 FROM gogdb_games gg WHERE gg.steamgame = sg.appid
        )
        """,
        (now,),
    )
    metrics["seed_inserted"] = cur.rowcount if cur.rowcount != -1 else 0

    try:
        cur.execute("ALTER TABLE gogdb_games ADD COLUMN match_method TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE gogdb_games ADD COLUMN match_score REAL")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE gogdb_products ADD COLUMN normalized_title TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        UPDATE gogdb_products
        SET normalized_title = LOWER(title)
        WHERE normalized_title IS NULL AND title IS NOT NULL
        """
    )
    cur.execute(
        """
        SELECT gog_id, title
        FROM gogdb_products
        WHERE (normalized_title IS NULL OR normalized_title = LOWER(title))
          AND title IS NOT NULL
        """
    )
    rows = cur.fetchall()
    for gog_id, title in rows:
        cur.execute(
            "UPDATE gogdb_products SET normalized_title = ? WHERE gog_id = ?",
            (_normalize_title(title), gog_id),
        )

    # 3) Exact match (case-insensitive)
    cur.execute(
        """
        UPDATE gogdb_games
        SET gog_id = (
            SELECT p.gog_id
            FROM gogdb_products p
            WHERE (p.type IS NULL OR LOWER(p.type) = 'game')
            AND LOWER(p.title) = LOWER(gogdb_games.name)
            AND NOT EXISTS (
                SELECT 1 FROM gogdb_games gg2 WHERE gg2.gog_id = p.gog_id
            )
            LIMIT 1
        ),
        found = CASE WHEN gog_id IS NULL THEN 0 ELSE 1 END,
        match_method = CASE WHEN gog_id IS NULL THEN match_method ELSE 'exact' END,
        match_score = CASE WHEN gog_id IS NULL THEN match_score ELSE 1.0 END,
        last_updated_utc = ?
        WHERE gog_id IS NULL
        AND name IS NOT NULL;
        """,
        (now,),
    )
    metrics["matched_exact"] = cur.rowcount if cur.rowcount != -1 else 0

    # 4) Normalized exact match
    cur.execute(
        """
        SELECT id, steamgame, name
        FROM gogdb_games
        WHERE gog_id IS NULL AND name IS NOT NULL
        """
    )
    unmatched = cur.fetchall()

    for gg_id, appid, name in unmatched:
        n = _normalize_title(name)
        cur.execute(
            """
            SELECT gog_id
            FROM gogdb_products
            WHERE (type IS NULL OR LOWER(type) = 'game')
              AND normalized_title = ?
            LIMIT 2
            """,
            (n,),
        )
        candidates = [r[0] for r in cur.fetchall()]
        if len(candidates) == 1:
            cur.execute("SELECT 1 FROM gogdb_games WHERE gog_id = ? LIMIT 1", (candidates[0],))
            if cur.fetchone() is not None:
                continue
            cur.execute(
                """
                UPDATE gogdb_games
                SET gog_id = ?, found = 1, match_method = 'norm_exact', match_score = 0.95, last_updated_utc = ?
                WHERE id = ? AND gog_id IS NULL
                """,
                (candidates[0], now, gg_id),
            )
            metrics["matched_norm_exact"] += 1

    # 5) LIKE unique (fallback)
    cur.execute(
        """
        SELECT id, name
        FROM gogdb_games
        WHERE gog_id IS NULL AND name IS NOT NULL
        """
    )
    still_unmatched = cur.fetchall()

    for gg_id, name in still_unmatched:
        n = _normalize_title(name)
        if len(n) < like_min_len:
            continue
        cur.execute(
            """
            SELECT gog_id
            FROM gogdb_products
            WHERE (type IS NULL OR LOWER(type) = 'game')
              AND normalized_title LIKE '%' || ? || '%'
            LIMIT 2
            """,
            (n,),
        )
        candidates = [r[0] for r in cur.fetchall()]

        if len(candidates) == 1:
            cur.execute("SELECT 1 FROM gogdb_games WHERE gog_id = ? LIMIT 1", (candidates[0],))
            if cur.fetchone() is not None:
                continue
            cur.execute(
                """
                UPDATE gogdb_games
                SET gog_id = ?, found = 1, match_method = 'like_unique', match_score = 0.7, last_updated_utc = ?
                WHERE id = ? AND gog_id IS NULL
                """,
                (candidates[0], now, gg_id),
            )
            metrics["matched_like_unique"] += 1
        elif len(candidates) > 1:
            metrics["skipped_like_ambiguous"] += 1

    # 6) No match found
    cur.execute("SELECT COUNT(*) FROM gogdb_games WHERE gog_id IS NULL")
    metrics["still_unmatched"] = int(cur.fetchone()[0])

    conn.commit()
    return metrics


def get_steam_games_list_from_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        SELECT appid, name
        FROM steam_games
        ORDER BY playtime_forever_min DESC
    """)
    return cur.fetchall()


def get_gog_games_list_from_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, gog_id, price, name
        FROM gogdb_games
        WHERE gog_id IS NOT NULL
        AND found = 1
        ORDER BY id DESC
    """)
    return cur.fetchall()


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        conn.commit()


def index_gogdb_dump_into_sqlite(db_path: str, dump_root: Path) -> None:
    products_dir = dump_root / "products"

    if not products_dir.is_dir():
        raise FileNotFoundError(f"{products_dir} Does not exist")

    conn = sqlite3.connect(db_path)
    try:
        init_db(conn)

        cur = conn.cursor()
        cur.execute("BEGIN")

        prod_upserts = 0
        price_upserts = 0

        for prod_dir in products_dir.iterdir():
            if not prod_dir.is_dir():
                continue

            product_json_path = prod_dir / "product.json"
            if not product_json_path.is_file():
                continue

            gog_id = int(prod_dir.name)

            product_obj = json.loads(product_json_path.read_text(encoding="utf-8"))
            title = product_obj.get("title") or product_obj.get("name")
            ptype = product_obj.get("type")
            slug = product_obj.get("slug")

            cur.execute(
                """
                INSERT INTO gogdb_products(gog_id, title, type, slug, raw_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(gog_id) DO UPDATE SET
                    title=excluded.title,
                    type=excluded.type,
                    slug=excluded.slug,
                    raw_json=excluded.raw_json
                """,
                (gog_id, title, ptype, slug, json.dumps(product_obj, ensure_ascii=False)),
            )
            prod_upserts += 1

            prices_json_path = prod_dir / "prices.json"
            if prices_json_path.is_file():
                prices_obj = json.loads(prices_json_path.read_text(encoding="utf-8"))

                def upsert_price(country, currency, base_price, final_price, discount_pct, raw):
                    nonlocal price_upserts
                    cur.execute(
                        """
                        INSERT INTO gogdb_prices(gog_id, country, currency, base_price, final_price, discount_pct, raw_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(gog_id, country, currency) DO UPDATE SET
                            base_price=excluded.base_price,
                            final_price=excluded.final_price,
                            discount_pct=excluded.discount_pct,
                            raw_json=excluded.raw_json
                        """,
                        (
                            gog_id,
                            country,
                            currency,
                            base_price,
                            final_price,
                            discount_pct,
                            json.dumps(raw, ensure_ascii=False),
                        ),
                    )
                    price_upserts += 1

                if isinstance(prices_obj, dict):
                    for country, entry in prices_obj.items():
                        if not isinstance(entry, dict):
                            continue

                        for currency, history in entry.items():
                            valid_history_items = (
                                h for h in history if isinstance(h, dict) and h.get("date")
                            )
                            latest = max(valid_history_items, key=lambda h: h["date"], default=None)

                            if not latest:
                                continue

                            payload = latest
                            currency = payload.get("currency")
                            base_price = payload.get("price_base")
                            final_price = payload.get("price_final")
                            discount_pct = payload.get("discount", 0)
                            upsert_price(
                                country, currency, base_price, final_price, discount_pct, payload
                            )
                else:
                    # Estructura desconocida: al menos lo guardamos en raw_json en una "fila marcador"
                    upsert_price(None, None, None, None, None, prices_obj)

        conn.commit()
        print(f"Indexed: products={prod_upserts}, prices={price_upserts}")
    finally:
        conn.close()


def find_valid_gogdb_dump_root(base_dir: str = ".") -> Path | None:
    _DUMP_DIR_RE = re.compile(r"^gogdb_\d{4}-\d{2}-\d{2}$")
    base = Path(base_dir)
    candidates = [p for p in base.iterdir() if p.is_dir() and _DUMP_DIR_RE.fullmatch(p.name)]
    candidates.sort(key=lambda p: p.name, reverse=True)

    for dump_root in candidates:
        products_dir = dump_root / "products"
        if not products_dir.is_dir():
            continue
        for prod_dir in products_dir.iterdir():
            if not prod_dir.is_dir():
                continue
            if (prod_dir / "product.json").is_file():
                return dump_root

    return None


def get_gogdb_latest_backup_url():
    result = requests.get("https://www.gogdb.org/backups_v3/products/")
    result.raise_for_status()
    soup = BeautifulSoup(result.text, "html.parser")
    latest_table = soup.find_all("table")[-1]
    addr = None
    pattern = r"\d{4}-(0[1-9]|1[0-2])/"
    for tr in reversed(latest_table.find_all("tr")):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        addr = tds[1].find("a").get_text()
        if re.fullmatch(pattern, addr):
            break
    return f"https://www.gogdb.org/backups_v3/products/{addr}"


def get_gogdb_latest_tar(backup_url):
    result = requests.get(backup_url)
    result.raise_for_status()
    soup = BeautifulSoup(result.text, "html.parser")
    latest_table = soup.find_all("table")[-1]
    addr = None
    pattern = r"gogdb_\d{4}-\d{2}-\d{2}\.tar\.xz"
    for tr in reversed(latest_table.find_all("tr")):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        print(tds[0], tds[1])
        addr = tds[1].find("a").get_text()
        if re.fullmatch(pattern, addr):
            break

    if addr is None:
        return

    sufix = backup_url.split("/")[-2]
    addr = addr.replace("/", "")
    return f"https://www.gogdb.org/backups_v3/products/{sufix}/{addr}"


def download_and_process_tar(tar_url):
    with requests.get(tar_url, stream=True) as r:
        local_file_name = tar_url.split("/")[-1]
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(local_file_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    progress_bar(downloaded, total_size, 50)

    print("\nDownloaded: ", local_file_name)
    print("\nDecompressing tar file: ", local_file_name)
    folder_name = local_file_name.split(".")[0]
    with tarfile.open(local_file_name, "r:xz") as tar:
        members = tar.getmembers()
        total = len(members)
        for i, member in enumerate(members, 1):
            tar.extract(member, path=f"./{folder_name}")
            progress_bar(i, total)
    print("\nFinished decompressing files to folder: ./", folder_name)


def progress_bar(current, total, width=50):
    percent = current / total
    filled = int(width * percent)
    bar = "=" * filled + "-" * (width - filled)
    sys.stdout.write(f"\r[{bar}] {percent:.1%}")
    sys.stdout.flush()


def load_gogdb_data():
    backup_url = get_gogdb_latest_backup_url()
    latest_tar_url = get_gogdb_latest_tar(backup_url)
    print("Found", latest_tar_url)
    download_and_process_tar(latest_tar_url)


def download_backup_from_gogdb(db_path: str):
    dump_root = find_valid_gogdb_dump_root()
    if not dump_root:
        load_gogdb_data()
        dump_root = find_valid_gogdb_dump_root()

    if dump_root is None:
        return

    index_gogdb_dump_into_sqlite(db_path, dump_root)

    seed_and_match_gogdb_games_by_name_safe(db_path)
