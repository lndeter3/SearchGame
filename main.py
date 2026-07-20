"""
SteamPeek API v6 — Railway Edition
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

• GET /cerca?q=NOME         → info complete di UN gioco
• GET /simili?q=NOME        → gioco + TUTTI i simili (multi-pagina)
• POST /simili/async        → job background per operazioni pesanti

TUTTO include automaticamente:
  - SteamSpy (owners, playtime, tags, reviews)
  - Steam Store (descrizione, screenshots, requisiti, DLC)
  - Steam Reviews summary
  - GameVault downloads (matching intelligente)
  - Immagini CDN, link esterni (SteamDB, ProtonDB, ITAD, ecc)

PAGINAZIONE: legge automaticamente TUTTE le pagine SteamPeek
(default 5, max 20) per ogni gioco → decine/centinaia di simili.
"""

import asyncio
import re
import json
import time
import os
import difflib
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from bs4 import BeautifulSoup as BS
from curl_cffi.requests import AsyncSession as CurlAsync
import httpx

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
BASE_URL = "https://steampeek.hu"
STEAMSPY_API = "https://steamspy.com/api.php"
GAMEVAULT_API = "https://halsbroken.s74zczkfgu.workers.dev"
STEAM_APPDETAILS = "https://store.steampowered.com/api/appdetails"
STEAM_REVIEWS = "https://store.steampowered.com/appreviews"
STEAM_SEARCH = "https://store.steampowered.com/api/storesearch"

IMPERSONATE = "chrome131"
DEFAULT_LANG = "italian"
DEFAULT_CC = "IT"

CONCURRENCY_PEEK = 16
CONCURRENCY_SPY = 40
CONCURRENCY_STORE = 20
CONCURRENCY_DL = 8
CONCURRENCY_REVIEWS = 15
CONCURRENCY_PAGES = 8

DELAY_PEEK = 0.04
DELAY_SPY = 0.02
DELAY_STORE = 0.05
DELAY_DL = 0.12

TIMEOUT = 25
RETRY_ATTEMPTS = 3

CACHE_DIR = Path("/tmp/steampeek_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_ALL = CACHE_DIR / "steamspy_all.json"
CACHE_ALL_TTL = 86400
CACHE_GAME_TTL = 3600

_memory_cache: dict[str, tuple[float, Any]] = {}
CACHE_MAX_ENTRIES = 1000

HDR = {
    "accept": "application/json,text/plain,*/*",
    "accept-language": "it-IT,it;q=0.9,en;q=0.8",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("steampeek")


# ═══════════════════════════════════════════════════════════════
# CACHE
# ═══════════════════════════════════════════════════════════════
def cache_get(key: str, ttl: int = CACHE_GAME_TTL):
    if key in _memory_cache:
        ts, val = _memory_cache[key]
        if time.time() - ts < ttl:
            return val
        del _memory_cache[key]
    return None


def cache_set(key: str, value: Any):
    if len(_memory_cache) >= CACHE_MAX_ENTRIES:
        sorted_keys = sorted(_memory_cache.items(), key=lambda x: x[1][0])
        for k, _ in sorted_keys[: CACHE_MAX_ENTRIES // 5]:
            del _memory_cache[k]
    _memory_cache[key] = (time.time(), value)


def make_cache_key(*parts) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


# ═══════════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════════
def safe_int(x, d=0):
    try:
        if x is None or x == "":
            return d
        return int(str(x).replace(",", ""))
    except Exception:
        return d


def money(x):
    try:
        v = int(str(x))
        if v <= 0:
            return None
        return round(v / 100, 2)
    except Exception:
        return None


def parse_steam_date(s: str | None) -> int:
    if not s:
        return 0
    s = str(s).strip()
    for fmt in ["%b %d, %Y", "%d %b, %Y", "%B %d, %Y", "%d %B, %Y"]:
        try:
            return int(datetime.strptime(s, fmt).timestamp())
        except Exception:
            pass
    m = re.search(r"Q([1-4])\s*(\d{4})", s)
    if m:
        q, y = int(m.group(1)), int(m.group(2))
        month = (q - 1) * 3 + 1
        try:
            return int(datetime(y, month, 1).timestamp())
        except Exception:
            pass
    for fmt in ["%B %Y", "%b %Y"]:
        try:
            return int(datetime.strptime(s, fmt).timestamp())
        except Exception:
            pass
    m = re.search(r"\b(19|20)\d{2}\b", s)
    if m:
        try:
            return int(datetime(int(m.group(0)), 6, 15).timestamp())
        except Exception:
            pass
    if any(k in s.lower() for k in ["coming", "tba", "announce", "soon"]):
        return 9999999999
    return 0


def clean_html(s: str | None) -> str:
    if not s:
        return ""
    return BS(s, "lxml").get_text(" ", strip=True)


# ═══════════════════════════════════════════════════════════════
# GAMEVAULT
# ═══════════════════════════════════════════════════════════════
def normalize_for_match(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"[™®©]", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    for w in [
        "edition", "deluxe", "ultimate", "goty", "complete", "definitive",
        "remastered", "remake", "game of the year", "anniversary", "collectors",
        "directors cut", "enhanced", "vr", "the", "a", "an", "and",
    ]:
        s = re.sub(rf"\b{re.escape(w)}\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def similarity(a: str, b: str) -> float:
    na = normalize_for_match(a)
    nb = normalize_for_match(b)
    if not na or not nb:
        return 0
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.85
    return difflib.SequenceMatcher(None, na, nb).ratio()


def make_search_queries(name: str) -> list[str]:
    queries = []
    original = name.strip()
    queries.append(original)
    clean = re.sub(r"[™®©]", "", original).strip()
    if clean != original:
        queries.append(clean)
    m = re.split(r"[:\-–—]", clean, 1)
    if m[0].strip() and m[0].strip() != clean:
        queries.append(m[0].strip())
    stripped = re.sub(
        r"\s*(deluxe|ultimate|goty|complete|definitive|remastered|"
        r"game of the year|anniversary|collectors|enhanced|"
        r"directors cut|edition)\s*$",
        "", clean, flags=re.I,
    ).strip()
    if stripped and stripped != clean:
        queries.append(stripped)
    seen: set[str] = set()
    out: list[str] = []
    for q in queries:
        if len(q) < 3:
            continue
        k = q.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(q)
    return out[:4]


async def _gv_try(client: httpx.AsyncClient, query: str):
    try:
        r = await client.get(
            f"{GAMEVAULT_API}/cercaTutto",
            params={"q": query},
            headers=HDR,
            timeout=25,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


async def gamevault_search(
    client: httpx.AsyncClient,
    game_name: str,
    sem: asyncio.Semaphore,
    max_variants: int = 8,
    min_similarity: float = 0.55,
):
    cache_key = make_cache_key("gv", game_name, max_variants)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    async with sem:
        queries = make_search_queries(game_name)
        best_result = None
        best_sim = 0.0

        for q in queries:
            data = await _gv_try(client, q)
            await asyncio.sleep(DELAY_DL)
            if not data or not data.get("risultati"):
                continue

            scored = []
            for r in data["risultati"]:
                title = r.get("titolo", "")
                clean_title = re.sub(
                    r"\b(build|v|version|update)\s*[\d.]+.*$",
                    "", title, flags=re.I,
                ).strip()
                clean_title = re.sub(r"\s+", " ", clean_title)
                sim = similarity(game_name, clean_title)
                scored.append((sim, r))

            scored.sort(key=lambda x: -x[0])

            if scored and scored[0][0] > best_sim:
                best_sim = scored[0][0]
                if scored[0][0] >= min_similarity:
                    threshold = max(0.5, scored[0][0] - 0.15)
                    valid = [r for sim, r in scored if sim >= threshold]
                    best_result = {
                        "search_query": q,
                        "matched_similarity": round(scored[0][0], 3),
                        "match_confidence": (
                            "perfect" if scored[0][0] >= 0.95 else
                            "high" if scored[0][0] >= 0.8 else
                            "medium" if scored[0][0] >= 0.65 else "low"
                        ),
                        "total_found": len(valid),
                        "variants": [
                            {
                                "title": r.get("titolo"),
                                "url": r.get("url"),
                                "cover": r.get("copertina"),
                                "download_links": r.get("links", [])[:20],
                                "num_links": len(r.get("links", [])),
                            }
                            for r in valid[:max_variants]
                        ],
                        "variants_shown": min(len(valid), max_variants),
                        "total_download_links": sum(
                            len(r.get("links", [])) for r in valid[:max_variants]
                        ),
                    }
                    if scored[0][0] >= 0.9:
                        break

        cache_set(cache_key, best_result)
        return best_result


# ═══════════════════════════════════════════════════════════════
# STEAMSPY
# ═══════════════════════════════════════════════════════════════
async def steamspy_all(client: httpx.AsyncClient) -> dict:
    if CACHE_ALL.exists():
        age = time.time() - CACHE_ALL.stat().st_mtime
        if age < CACHE_ALL_TTL:
            try:
                return json.loads(CACHE_ALL.read_text(encoding="utf-8"))
            except Exception:
                pass
    log.info("Fetching full SteamSpy catalog...")
    r = await client.get(
        STEAMSPY_API, params={"request": "all"}, headers=HDR, timeout=90
    )
    data = r.json()
    CACHE_ALL.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    log.info("SteamSpy catalog cached: %d games", len(data))
    return data


async def resolve_appid(client: httpx.AsyncClient, query: str):
    cache_key = make_cache_key("resolve", query)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    q = query.strip()

    try:
        r = await client.get(
            STEAM_SEARCH,
            params={"term": q, "l": "english", "cc": "US"},
            headers=HDR,
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            items = data.get("items", [])
            if items:
                scored = [(similarity(q, it.get("name", "")), it) for it in items]
                scored.sort(key=lambda x: -x[0])
                best_sim, best_item = scored[0]
                if best_sim >= 0.5:
                    result = (int(best_item["id"]), best_item["name"])
                    cache_set(cache_key, result)
                    return result
    except Exception as e:
        log.warning("Steam search failed: %s", e)

    try:
        data = await steamspy_all(client)
        ql = q.lower()
        exact, contains = [], []
        names: dict[str, tuple[int, str]] = {}
        for appid, item in data.items():
            name = (item.get("name") or "").strip()
            if not name:
                continue
            nl = name.lower()
            names[nl] = (int(appid), name)
            if nl == ql:
                exact.append((int(appid), name))
            elif ql in nl:
                contains.append((int(appid), name))
        if exact:
            cache_set(cache_key, exact[0])
            return exact[0]
        if contains:
            contains.sort(key=lambda x: len(x[1]))
            cache_set(cache_key, contains[0])
            return contains[0]
        close = difflib.get_close_matches(ql, names.keys(), n=1, cutoff=0.65)
        if close:
            cache_set(cache_key, names[close[0]])
            return names[close[0]]
    except Exception as e:
        log.warning("SteamSpy resolve failed: %s", e)

    return (None, None)


async def steamspy_details(client: httpx.AsyncClient, appid: int):
    cache_key = make_cache_key("spy", appid)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    for _ in range(RETRY_ATTEMPTS):
        try:
            r = await client.get(
                STEAMSPY_API,
                params={"request": "appdetails", "appid": appid},
                headers=HDR,
                timeout=TIMEOUT,
            )
            if r.status_code == 200:
                d = r.json()
                if d and d.get("appid"):
                    cache_set(cache_key, d)
                    return d
        except Exception:
            await asyncio.sleep(0.5)
    return None


async def steam_store_details(
    client: httpx.AsyncClient, appid: int, lang: str = "italian", cc: str = "IT"
):
    cache_key = make_cache_key("store", appid, lang, cc)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    for _ in range(RETRY_ATTEMPTS):
        try:
            r = await client.get(
                STEAM_APPDETAILS,
                params={"appids": appid, "l": lang, "cc": cc},
                headers=HDR,
                timeout=20,
            )
            if r.status_code == 200:
                d = r.json()
                entry = d.get(str(appid), {})
                if entry.get("success"):
                    data = entry.get("data", {})
                    cache_set(cache_key, data)
                    return data
                cache_set(cache_key, None)
                return None
        except Exception:
            await asyncio.sleep(0.5)
    return None


async def steam_reviews_summary(client: httpx.AsyncClient, appid: int):
    cache_key = make_cache_key("rev", appid)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        r = await client.get(
            f"{STEAM_REVIEWS}/{appid}",
            params={
                "json": 1, "language": "all",
                "purchase_type": "all", "num_per_page": 0,
            },
            headers=HDR,
            timeout=15,
        )
        if r.status_code == 200:
            d = r.json()
            summary = d.get("query_summary", {})
            cache_set(cache_key, summary)
            return summary
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════
# STEAMPEEK PARSING + PAGINAZIONE
# ═══════════════════════════════════════════════════════════════
def extract_name(cont) -> str:
    img = cont.find("img", alt=re.compile(r".+"))
    if img:
        alt = img.get("alt", "").strip()
        alt = re.sub(
            r"\s+(and\s+similar\s+games|thumbnail|logo|capsule).*$",
            "", alt, flags=re.I,
        ).strip()
        if alt and not alt.startswith("#") and len(alt) > 1:
            return alt
    for attr in ["data-appname", "data-name", "title", "aria-label"]:
        v = cont.get(attr)
        if v and len(v) > 1:
            return v.strip()
    for a in cont.find_all("a", href=re.compile(r"store\.steampowered\.com/app/\d+")):
        txt = a.get_text(" ", strip=True)
        if 2 < len(txt) < 100:
            return txt
        m = re.search(r"/app/\d+/([^/?#]+)", a.get("href", ""))
        if m:
            return m.group(1).replace("_", " ").strip()
    return ""


def parse_peek(html: str, src: int) -> list[dict]:
    soup = BS(html, "lxml")
    games: dict[int, dict] = {}
    containers = soup.select(".lister_item_cont[data-appid]")
    if not containers:
        raw = soup.select("[data-appid]")
        containers = []
        seen: set[str] = set()
        for el in raw:
            aid = el.get("data-appid")
            if not aid or aid in seen:
                continue
            seen.add(aid)
            parent = el.find_parent(class_=re.compile(r"lister_item_cont|lister_item"))
            containers.append(parent or el)
    for cont in containers:
        try:
            aid_el = cont.get("data-appid") or (
                cont.select_one("[data-appid]") or {}
            ).get("data-appid")
            aid = int(aid_el)
        except Exception:
            continue
        if aid == src or aid in games:
            continue
        games[aid] = {"appid": aid, "name": extract_name(cont) or f"#{aid}"}
    return list(games.values())


def parse_total_pages(html: str) -> int:
    """Rileva il numero totale di pagine da un HTML SteamPeek."""
    if not html:
        return 1
    try:
        soup = BS(html, "lxml")
        text = soup.get_text(" ", strip=True)
        # cerca "page X / Y"
        m = re.search(r"page\s*\d+\s*/\s*(\d+)", text, re.I)
        if m:
            return int(m.group(1))
        # cerca link ?page=N
        max_page = 1
        for a in soup.find_all("a", href=True):
            m = re.search(r"[?&]page=(\d+)", a["href"])
            if m:
                max_page = max(max_page, int(m.group(1)))
        # cerca select con options
        for opt in soup.select("select option"):
            val = opt.get("value", "")
            if val.isdigit():
                max_page = max(max_page, int(val))
        return max_page
    except Exception:
        return 1


async def fetch_peek_page(
    session: CurlAsync,
    appid: int,
    page: int,
    sem: asyncio.Semaphore,
) -> tuple[list[dict], str]:
    """Fetch UNA pagina di simili. Ritorna (games, raw_html)."""
    async with sem:
        for i in range(RETRY_ATTEMPTS):
            try:
                url = f"{BASE_URL}/?appid={appid}"
                if page > 1:
                    url += f"&page={page}"
                r = await session.get(
                    url,
                    headers={"referer": BASE_URL + "/"},
                    timeout=TIMEOUT,
                )
                await asyncio.sleep(DELAY_PEEK)
                return parse_peek(r.text, appid), r.text
            except Exception:
                await asyncio.sleep(0.4 * (2 ** i))
        return [], ""


async def fetch_all_similar(
    session: CurlAsync,
    appid: int,
    sem: asyncio.Semaphore,
    max_pages: int = 5,
) -> list[dict]:
    """
    Scarica TUTTE le pagine di simili per un appid.
    Rileva automaticamente quante pagine ci sono.
    """
    # 1. Pagina 1 + detect
    p1_games, p1_html = await fetch_peek_page(session, appid, 1, sem)
    total_pages = min(parse_total_pages(p1_html), max_pages)

    all_games: dict[int, dict] = {g["appid"]: g for g in p1_games}

    if total_pages <= 1:
        return list(all_games.values())

    # 2. Pagine 2..N in parallelo
    page_sem = asyncio.Semaphore(CONCURRENCY_PAGES)

    async def fetch_page(p: int):
        async with page_sem:
            games, _ = await fetch_peek_page(session, appid, p, sem)
            return games

    results = await asyncio.gather(
        *[fetch_page(p) for p in range(2, total_pages + 1)],
        return_exceptions=True,
    )

    for res in results:
        if isinstance(res, Exception):
            continue
        for g in res:
            if g["appid"] not in all_games:
                all_games[g["appid"]] = g

    return list(all_games.values())


# ═══════════════════════════════════════════════════════════════
# BFS DISCOVERY
# ═══════════════════════════════════════════════════════════════
async def bfs_discover(
    peek: CurlAsync,
    seed_id: int,
    seed_name: str,
    depth: int,
    max_total: int | None = None,
    max_pages_per_game: int = 5,
) -> list[dict]:
    all_g: dict[int, dict] = {
        seed_id: {"appid": seed_id, "name": seed_name, "depth": 0, "parent": None}
    }
    current = [seed_id]
    visited = {seed_id}
    sem = asyncio.Semaphore(CONCURRENCY_PEEK)
    unlimited = max_total is None

    for lvl in range(depth):
        if not current:
            break
        if not unlimited and len(all_g) >= max_total:
            break

        log.info("BFS L%d/%d — expanding %d nodes", lvl + 1, depth, len(current))

        async def job(parent: int):
            games = await fetch_all_similar(peek, parent, sem, max_pages=max_pages_per_game)
            return parent, games

        tasks = [job(a) for a in current]
        nxt: list[int] = []
        for coro in asyncio.as_completed(tasks):
            parent, games = await coro
            for g in games:
                gid = g["appid"]
                if gid in all_g:
                    continue
                if not unlimited and len(all_g) >= max_total:
                    break
                g["depth"] = lvl + 1
                g["parent"] = parent
                all_g[gid] = g
                if gid not in visited and lvl + 1 < depth:
                    nxt.append(gid)
                    visited.add(gid)
        log.info("BFS L%d done — total=%d, next=%d", lvl + 1, len(all_g), len(nxt))
        current = nxt

    return [g for g in all_g.values() if g["appid"] != seed_id]


# ═══════════════════════════════════════════════════════════════
# BUILD RECORD
# ═══════════════════════════════════════════════════════════════
def _owners_min(owners_str) -> int:
    if not owners_str:
        return 0
    m = re.search(r"([\d,]+)", str(owners_str))
    if m:
        return safe_int(m.group(1))
    return 0


def build_full_record(
    appid: int,
    spy: dict | None,
    store: dict | None,
    reviews_summary: dict | None,
    dl_info: dict | None,
    extra: dict | None = None,
) -> dict:
    spy = spy or {}
    store = store or {}
    reviews_summary = reviews_summary or {}
    extra = extra or {}
    appid = int(appid)

    name = store.get("name") or spy.get("name") or extra.get("name") or f"#{appid}"

    pos = safe_int(spy.get("positive"))
    neg = safe_int(spy.get("negative"))
    total = pos + neg
    score = round((pos / total) * 100, 2) if total else None

    steam_total = safe_int(reviews_summary.get("total_reviews"))
    steam_pos = safe_int(reviews_summary.get("total_positive"))
    steam_score = round((steam_pos / steam_total) * 100, 2) if steam_total > 0 else None

    if store.get("is_free"):
        price_info = {
            "final": 0, "initial": 0, "discount_percent": 0,
            "currency": "USD", "formatted": "FREE", "is_free": True,
        }
    elif store.get("price_overview"):
        po = store["price_overview"]
        price_info = {
            "final": po.get("final", 0) / 100,
            "initial": po.get("initial", 0) / 100,
            "discount_percent": po.get("discount_percent", 0),
            "currency": po.get("currency"),
            "formatted": po.get("final_formatted"),
            "initial_formatted": po.get("initial_formatted"),
            "is_free": False,
        }
    else:
        final = money(spy.get("price"))
        initial = money(spy.get("initialprice"))
        price_info = {
            "final": final, "initial": initial,
            "discount_percent": safe_int(spy.get("discount")),
            "currency": "USD",
            "formatted": (
                "FREE" if final == 0 else f"${final:.2f}"
                if final is not None else None
            ),
            "is_free": final == 0,
        }

    tags_raw = spy.get("tags") or {}
    tags = (
        [{"name": k, "votes": v} for k, v in sorted(tags_raw.items(), key=lambda x: -x[1])[:30]]
        if isinstance(tags_raw, dict) else []
    )

    store_genres = [g.get("description") for g in store.get("genres", []) if g.get("description")]
    store_categories = [c.get("description") for c in store.get("categories", []) if c.get("description")]
    spy_genres = [x.strip() for x in str(spy.get("genre") or "").split(",") if x.strip()]
    genres = list(dict.fromkeys(store_genres + spy_genres))

    rd = store.get("release_date", {})
    release_date = rd.get("date") or spy.get("release_date")
    release_ts = parse_steam_date(release_date)
    coming_soon = rd.get("coming_soon", False)

    screenshots = [
        {"id": s.get("id"), "thumbnail": s.get("path_thumbnail"), "full": s.get("path_full")}
        for s in (store.get("screenshots") or [])
    ]

    movies = [
        {
            "id": m.get("id"),
            "name": m.get("name"),
            "thumbnail": m.get("thumbnail"),
            "webm_480": (m.get("webm") or {}).get("480"),
            "webm_max": (m.get("webm") or {}).get("max"),
            "mp4_480": (m.get("mp4") or {}).get("480"),
            "mp4_max": (m.get("mp4") or {}).get("max"),
        }
        for m in (store.get("movies") or [])
    ]

    def parse_reqs(req):
        if not req or not isinstance(req, dict):
            return None
        return {
            "minimum": clean_html(req.get("minimum")),
            "recommended": clean_html(req.get("recommended")),
        }

    platforms = store.get("platforms") or {}
    dlc_list = store.get("dlc") or []
    achievements = store.get("achievements") or {}

    packages = []
    for pg in store.get("package_groups", []) or []:
        for sub in pg.get("subs", []) or []:
            packages.append({
                "packageid": sub.get("packageid"),
                "option_text": sub.get("option_text"),
                "price_final": sub.get("price_in_cents_with_discount", 0) / 100,
                "is_free_license": sub.get("is_free_license", False),
            })

    return {
        "appid": appid,
        "name": name,
        "type": store.get("type", "game"),
        "required_age": store.get("required_age", 0),
        "short_description": store.get("short_description"),
        "detailed_description": clean_html(store.get("detailed_description")),
        "about_the_game": clean_html(store.get("about_the_game")),
        "supported_languages": clean_html(store.get("supported_languages")),
        "languages_spy": spy.get("languages"),
        "website": store.get("website"),
        "developers": store.get("developers") or (
            [spy.get("developer")] if spy.get("developer") else []
        ),
        "publishers": store.get("publishers") or (
            [spy.get("publisher")] if spy.get("publisher") else []
        ),
        "genres": genres,
        "categories": store_categories,
        "tags": tags,
        "release_date": release_date,
        "release_ts": release_ts,
        "coming_soon": coming_soon,
        "price": price_info,
        "packages": packages,
        "reviews": {
            "steamspy": {
                "positive": pos, "negative": neg, "total": total,
                "score_percent": score,
                "userscore": safe_int(spy.get("userscore")),
            },
            "steam": {
                "total_reviews": steam_total,
                "total_positive": steam_pos,
                "total_negative": safe_int(reviews_summary.get("total_negative")),
                "score_percent": steam_score,
                "review_score": reviews_summary.get("review_score"),
                "review_score_desc": reviews_summary.get("review_score_desc"),
            },
        },
        "owners_estimate": spy.get("owners"),
        "owners_min": _owners_min(spy.get("owners")),
        "ccu": safe_int(spy.get("ccu")),
        "playtime": {
            "average_forever_min": safe_int(spy.get("average_forever")),
            "average_2weeks_min": safe_int(spy.get("average_2weeks")),
            "median_forever_min": safe_int(spy.get("median_forever")),
            "median_2weeks_min": safe_int(spy.get("median_2weeks")),
        },
        "platforms": {
            "windows": platforms.get("windows", False),
            "mac": platforms.get("mac", False),
            "linux": platforms.get("linux", False),
        },
        "requirements": {
            "pc": parse_reqs(store.get("pc_requirements")),
            "mac": parse_reqs(store.get("mac_requirements")),
            "linux": parse_reqs(store.get("linux_requirements")),
        },
        "images": {
            "header": store.get("header_image") or f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg",
            "capsule_616x353": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_616x353.jpg",
            "capsule_231x87": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_231x87.jpg",
            "library_600x900": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/library_600x900.jpg",
            "library_hero": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/library_hero.jpg",
            "logo": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/logo.png",
            "page_bg": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/page_bg_raw.jpg",
            "background": store.get("background"),
        },
        "screenshots": screenshots,
        "movies": movies,
        "metacritic": store.get("metacritic"),
        "achievements_total": achievements.get("total", 0),
        "achievements_highlighted": achievements.get("highlighted", []),
        "dlc_ids": dlc_list,
        "dlc_count": len(dlc_list),
        "content_descriptors": store.get("content_descriptors") or {},
        "support_info": store.get("support_info") or {},
        "links": {
            "steam_store": f"https://store.steampowered.com/app/{appid}/",
            "steam_community": f"https://steamcommunity.com/app/{appid}/",
            "steampeek": f"{BASE_URL}/?appid={appid}",
            "steamspy": f"https://steamspy.com/app/{appid}",
            "steamdb": f"https://steamdb.info/app/{appid}/",
            "protondb": f"https://www.protondb.com/app/{appid}",
            "steamcharts": f"https://steamcharts.com/app/{appid}",
            "itad": f"https://isthereanydeal.com/steam/app/{appid}/",
        },
        "downloads": dl_info,
        "has_downloads": bool(dl_info),
        "download_variants_count": (dl_info or {}).get("total_found", 0) if dl_info else 0,
        "download_links_count": (dl_info or {}).get("total_download_links", 0) if dl_info else 0,
        "_bfs_depth": extra.get("depth"),
        "_bfs_parent": extra.get("parent"),
    }


# ═══════════════════════════════════════════════════════════════
# ENRICH
# ═══════════════════════════════════════════════════════════════
async def enrich_single_game(
    http_client: httpx.AsyncClient,
    appid: int,
    name_hint: str | None = None,
    include_downloads: bool = True,
    dl_max: int = 8,
    lang: str = DEFAULT_LANG,
    cc: str = DEFAULT_CC,
    extra: dict | None = None,
) -> dict:
    sem_dl = asyncio.Semaphore(CONCURRENCY_DL)

    spy = await steamspy_details(http_client, appid)
    clean_name = (spy or {}).get("name") or name_hint or ""

    tasks = [
        steam_store_details(http_client, appid, lang, cc),
        steam_reviews_summary(http_client, appid),
    ]
    if include_downloads and clean_name and not clean_name.startswith("#"):
        tasks.append(gamevault_search(http_client, clean_name, sem_dl, max_variants=dl_max))
    else:
        tasks.append(asyncio.sleep(0, result=None))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    store = results[0] if not isinstance(results[0], Exception) else None
    rev_sum = results[1] if not isinstance(results[1], Exception) else None
    dl_info = results[2] if not isinstance(results[2], Exception) else None

    return build_full_record(
        appid=appid, spy=spy, store=store,
        reviews_summary=rev_sum, dl_info=dl_info,
        extra=extra or {"name": name_hint},
    )


async def enrich_batch(
    http_client: httpx.AsyncClient,
    games: list[dict],
    include_downloads: bool = True,
    dl_max: int = 5,
    lang: str = DEFAULT_LANG,
    cc: str = DEFAULT_CC,
    full_details: bool = False,
) -> list[dict]:
    sem_spy = asyncio.Semaphore(CONCURRENCY_SPY)
    sem_store = asyncio.Semaphore(CONCURRENCY_STORE)
    sem_rev = asyncio.Semaphore(CONCURRENCY_REVIEWS)
    sem_dl = asyncio.Semaphore(CONCURRENCY_DL)

    results: list[dict] = []
    done = 0
    total = len(games)

    async def worker(g: dict):
        nonlocal done
        async with sem_spy:
            spy = await steamspy_details(http_client, g["appid"])
            await asyncio.sleep(DELAY_SPY)

        clean_name = (spy or {}).get("name") or g.get("name", "")

        tasks = []
        if full_details:
            async def _store():
                async with sem_store:
                    await asyncio.sleep(DELAY_STORE)
                    return await steam_store_details(http_client, g["appid"], lang, cc)
            async def _rev():
                async with sem_rev:
                    return await steam_reviews_summary(http_client, g["appid"])
            tasks.append(_store())
            tasks.append(_rev())
        else:
            tasks.append(asyncio.sleep(0, result=None))
            tasks.append(asyncio.sleep(0, result=None))

        if include_downloads and clean_name and not clean_name.startswith("#"):
            tasks.append(gamevault_search(http_client, clean_name, sem_dl, max_variants=dl_max))
        else:
            tasks.append(asyncio.sleep(0, result=None))

        outs = await asyncio.gather(*tasks, return_exceptions=True)
        store = outs[0] if not isinstance(outs[0], Exception) else None
        rev = outs[1] if not isinstance(outs[1], Exception) else None
        dl = outs[2] if not isinstance(outs[2], Exception) else None

        done += 1
        if done % 25 == 0 or done == total:
            log.info("Enriched %d/%d", done, total)

        return build_full_record(g["appid"], spy, store, rev, dl, extra=g)

    tasks = [worker(g) for g in games]
    for coro in asyncio.as_completed(tasks):
        try:
            results.append(await coro)
        except Exception as e:
            log.warning("Enrich worker error: %s", e)

    return results


# ═══════════════════════════════════════════════════════════════
# JOBS STORE
# ═══════════════════════════════════════════════════════════════
jobs: dict[str, dict] = {}


def create_job(query: str) -> str:
    import uuid
    job_id = str(uuid.uuid4())[:12]
    jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "query": query,
        "started_at": datetime.utcnow().isoformat(),
        "progress": "Initializing...",
        "result": None,
        "error": None,
    }
    now = time.time()
    to_del = [
        jid for jid, j in jobs.items()
        if (now - datetime.fromisoformat(j["started_at"]).timestamp()) > 3600
    ]
    for jid in to_del:
        del jobs[jid]
    return job_id


# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 SteamPeek API v6 starting")

    async def prewarm():
        try:
            async with httpx.AsyncClient(timeout=90, headers=HDR) as c:
                await steamspy_all(c)
        except Exception as e:
            log.warning("Prewarm failed: %s", e)

    asyncio.create_task(prewarm())
    yield
    log.info("👋 Shutdown")


app = FastAPI(
    title="SteamPeek API v6",
    version="6.0.0",
    description=(
        "Discovery Steam intelligente con paginazione automatica.\n\n"
        "• `/cerca?q=NOME` — info complete di UN gioco\n"
        "• `/simili?q=NOME` — gioco + TUTTI i simili (multi-pagina automatica)"
    ),
    lifespan=lifespan,
    default_response_class=ORJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════
@app.get("/")
async def root():
    return {
        "service": "SteamPeek API v6",
        "version": "6.0.0",
        "endpoints": {
            "cerca": "GET /cerca?q=NOME",
            "simili": "GET /simili?q=NOME&depth=2",
            "simili_async": "POST /simili/async?q=NOME&depth=3",
            "resolve": "GET /resolve?q=NOME",
        },
        "docs": "/docs",
        "health": "/health",
        "features": [
            "Paginazione automatica SteamPeek",
            "Match intelligente GameVault",
            "SteamSpy + Steam Store + Reviews",
            "Cache in-memory + file",
        ],
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "6.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "cache_entries": len(_memory_cache),
        "active_jobs": sum(1 for j in jobs.values() if j["status"] == "running"),
    }


@app.get("/resolve")
async def endpoint_resolve(q: str = Query(...)):
    async with httpx.AsyncClient(timeout=60, headers=HDR) as c:
        appid, name = await resolve_appid(c, q)
    if not appid:
        raise HTTPException(404, detail=f"Non trovato: '{q}'")
    return {
        "query": q,
        "appid": appid,
        "name": name,
        "steam_url": f"https://store.steampowered.com/app/{appid}/",
    }


@app.get("/cerca")
async def endpoint_cerca(
    q: str = Query(..., description="Nome gioco o AppID"),
    lang: str = Query(DEFAULT_LANG),
    cc: str = Query(DEFAULT_CC),
    downloads: bool = Query(True),
    dl_max: int = Query(10, ge=1, le=30),
):
    """Info complete di UN gioco (SteamSpy + Store + Reviews + Downloads)."""
    t0 = time.time()

    try:
        appid = int(q.strip())
        name_hint = None
    except ValueError:
        async with httpx.AsyncClient(timeout=60, headers=HDR) as c:
            appid, name_hint = await resolve_appid(c, q)
        if not appid:
            raise HTTPException(404, detail=f"Non trovato: '{q}'")

    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT, headers=HDR, follow_redirects=True
        ) as c:
            record = await enrich_single_game(
                c, appid,
                name_hint=name_hint,
                include_downloads=downloads,
                dl_max=dl_max,
                lang=lang, cc=cc,
            )

        return {
            "ok": True,
            "query": q,
            "resolved_appid": appid,
            "elapsed_seconds": round(time.time() - t0, 2),
            "game": record,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Errore /cerca")
        raise HTTPException(500, detail={"error": str(e), "query": q})


@app.get("/simili")
async def endpoint_simili(
    q: str = Query(..., description="Nome gioco o AppID"),
    depth: int = Query(2, ge=1, le=4, description="Profondità BFS 1-4"),
    max_total: Optional[int] = Query(
        None, ge=1, alias="max",
        description="Max giochi totali (URL: ?max=200)",
    ),
    max_pages_per_game: int = Query(
        5, ge=1, le=20,
        description="Max pagine SteamPeek per gioco (default 5)",
    ),
    lang: str = Query(DEFAULT_LANG),
    cc: str = Query(DEFAULT_CC),
    downloads: bool = Query(True),
    dl_max: int = Query(5, ge=1, le=20),
    full: bool = Query(False, description="Fetch Steam Store per ogni simile"),
    sort: str = Query("depth"),
    desc: bool = Query(False),
    min_score: float = Query(0, ge=0, le=100),
    min_reviews: int = Query(0, ge=0),
    free_only: bool = Query(False),
    paid_only: bool = Query(False),
    dl_only: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1),
):
    """
    Gioco + TUTTI i simili trovati (paginazione automatica SteamPeek).
    Esempi:
      /simili?q=DayZ&depth=1
      /simili?q=DayZ&depth=2&max=200
      /simili?q=DayZ&depth=2&full=true&dl_only=true
    """
    t0 = time.time()

    try:
        async with (
            httpx.AsyncClient(
                timeout=TIMEOUT, headers=HDR, follow_redirects=True
            ) as http_client,
            CurlAsync(impersonate=IMPERSONATE) as peek_client,
        ):
            try:
                await peek_client.get(BASE_URL + "/", timeout=TIMEOUT)
            except Exception as e:
                log.warning("Warmup failed: %s", e)

            try:
                appid = int(q.strip())
                seed_name = q
            except ValueError:
                appid, seed_name = await resolve_appid(http_client, q)
                if not appid:
                    raise HTTPException(404, detail=f"Non trovato: '{q}'")

            log.info("Seed: %s appid=%s", seed_name, appid)

            t1 = time.time()
            discovered = await bfs_discover(
                peek_client, appid, seed_name, depth, max_total,
                max_pages_per_game=max_pages_per_game,
            )
            t2 = time.time()
            log.info("BFS: %d games in %.2fs", len(discovered), t2 - t1)

            seed_dl_max = dl_max if dl_max >= 10 else 10

            seed_task = enrich_single_game(
                http_client, appid,
                name_hint=seed_name,
                include_downloads=downloads,
                dl_max=seed_dl_max,
                lang=lang, cc=cc,
                extra={"depth": 0, "parent": None, "name": seed_name},
            )
            similar_task = enrich_batch(
                http_client, discovered,
                include_downloads=downloads,
                dl_max=dl_max,
                lang=lang, cc=cc,
                full_details=full,
            )
            seed_record, similar_records = await asyncio.gather(
                seed_task, similar_task
            )
            t3 = time.time()
            log.info(
                "Enrichment: %d records in %.2fs",
                len(similar_records) + 1, t3 - t2,
            )

        def filter_ok(g: dict) -> bool:
            rev = ((g.get("reviews") or {}).get("steamspy") or {})
            if min_reviews and (rev.get("total") or 0) < min_reviews:
                return False
            if min_score and (rev.get("score_percent") or 0) < min_score:
                return False
            pr = g.get("price") or {}
            if free_only and not pr.get("is_free"):
                return False
            if paid_only and pr.get("is_free"):
                return False
            if dl_only and not g.get("has_downloads"):
                return False
            return True

        filtered = [g for g in similar_records if filter_ok(g)]

        SORT_KEYS = {
            "depth": lambda x: (
                x.get("_bfs_depth") if x.get("_bfs_depth") is not None else 99,
                (x.get("name") or "").lower(),
            ),
            "name": lambda x: (x.get("name") or "").lower(),
            "date": lambda x: x.get("release_ts") or 0,
            "score": lambda x: (
                ((x.get("reviews") or {}).get("steamspy") or {}).get("score_percent") or 0
            ),
            "reviews": lambda x: (
                ((x.get("reviews") or {}).get("steamspy") or {}).get("total") or 0
            ),
            "price": lambda x: (x.get("price") or {}).get("final") or 0,
            "downloads": lambda x: (x.get("downloads") or {}).get("total_found") or 0,
            "owners": lambda x: x.get("owners_min") or 0,
        }
        key = SORT_KEYS.get(sort, SORT_KEYS["depth"])
        filtered.sort(key=key, reverse=desc)

        if limit:
            filtered = filtered[:limit]

        with_dl = sum(1 for r in filtered if r.get("has_downloads"))

        return {
            "ok": True,
            "query": q,
            "resolved_appid": appid,
            "seed": seed_record,
            "similar": filtered,
            "stats": {
                "total_discovered": len(discovered),
                "total_after_filter": len(filtered),
                "with_downloads": with_dl,
                "download_rate_pct": round(with_dl * 100 / max(len(filtered), 1), 1),
                "elapsed_seconds": round(time.time() - t0, 2),
                "params": {
                    "depth": depth,
                    "max": max_total,
                    "max_pages_per_game": max_pages_per_game,
                    "sort": sort,
                    "desc": desc,
                    "full_details": full,
                    "downloads": downloads,
                },
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        log.exception("Errore /simili")
        raise HTTPException(
            500,
            detail={"error": "simili_failed", "message": str(e), "query": q},
        )


@app.post("/simili/async")
async def endpoint_simili_async(
    background: BackgroundTasks,
    q: str = Query(...),
    depth: int = Query(3, ge=1, le=4),
    max_total: Optional[int] = Query(None, ge=1, alias="max"),
    max_pages_per_game: int = Query(5, ge=1, le=20),
    lang: str = Query(DEFAULT_LANG),
    cc: str = Query(DEFAULT_CC),
    downloads: bool = Query(True),
    dl_max: int = Query(5, ge=1, le=20),
    full: bool = Query(False),
    sort: str = Query("depth"),
    desc: bool = Query(False),
):
    """Job background per ricerche pesanti. Poll /jobs/{id}."""
    job_id = create_job(q)

    async def run():
        try:
            jobs[job_id]["progress"] = "Resolving..."
            async with (
                httpx.AsyncClient(
                    timeout=TIMEOUT, headers=HDR, follow_redirects=True
                ) as http_c,
                CurlAsync(impersonate=IMPERSONATE) as peek_c,
            ):
                try:
                    await peek_c.get(BASE_URL + "/", timeout=TIMEOUT)
                except Exception:
                    pass

                try:
                    appid = int(q.strip())
                    seed_name = q
                except ValueError:
                    appid, seed_name = await resolve_appid(http_c, q)
                    if not appid:
                        raise RuntimeError(f"Non trovato: {q}")

                jobs[job_id]["progress"] = f"BFS depth={depth}..."
                discovered = await bfs_discover(
                    peek_c, appid, seed_name, depth, max_total,
                    max_pages_per_game=max_pages_per_game,
                )

                jobs[job_id]["progress"] = f"Enriching {len(discovered)+1} games..."
                seed_dl_max = dl_max if dl_max >= 10 else 10
                seed_task = enrich_single_game(
                    http_c, appid, name_hint=seed_name,
                    include_downloads=downloads, dl_max=seed_dl_max,
                    lang=lang, cc=cc,
                    extra={"depth": 0, "parent": None, "name": seed_name},
                )
                sim_task = enrich_batch(
                    http_c, discovered,
                    include_downloads=downloads, dl_max=dl_max,
                    lang=lang, cc=cc, full_details=full,
                )
                seed_rec, sim_recs = await asyncio.gather(seed_task, sim_task)

            SORT_KEYS = {
                "depth": lambda x: (
                    x.get("_bfs_depth") if x.get("_bfs_depth") is not None else 99,
                    (x.get("name") or "").lower(),
                ),
                "name": lambda x: (x.get("name") or "").lower(),
                "date": lambda x: x.get("release_ts") or 0,
                "score": lambda x: (
                    ((x.get("reviews") or {}).get("steamspy") or {}).get("score_percent") or 0
                ),
                "reviews": lambda x: (
                    ((x.get("reviews") or {}).get("steamspy") or {}).get("total") or 0
                ),
                "downloads": lambda x: (x.get("downloads") or {}).get("total_found") or 0,
            }
            sim_recs.sort(key=SORT_KEYS.get(sort, SORT_KEYS["depth"]), reverse=desc)

            jobs[job_id]["status"] = "completed"
            jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
            jobs[job_id]["result"] = {
                "seed": seed_rec,
                "similar": sim_recs,
                "stats": {
                    "total": len(sim_recs),
                    "with_downloads": sum(
                        1 for r in sim_recs if r.get("has_downloads")
                    ),
                },
            }
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            log.exception("Job %s failed", job_id)

    background.add_task(run)
    return {
        "job_id": job_id,
        "status": "running",
        "poll_url": f"/jobs/{job_id}",
    }


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, detail="Job not found")
    return jobs[job_id]


@app.get("/jobs")
async def list_jobs():
    return {
        "total": len(jobs),
        "jobs": [
            {
                "id": j["id"],
                "status": j["status"],
                "query": j["query"],
                "started_at": j["started_at"],
                "progress": j.get("progress"),
            }
            for j in jobs.values()
        ],
    }


# ═══════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
