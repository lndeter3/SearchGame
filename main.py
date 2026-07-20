"""
SteamPeek API v4 — Railway Edition
API REST completa con FastAPI.
BFS discovery + SteamSpy enrichment + GameVault auto-download matching.
"""

import asyncio
import re
import json
import time
import os
import difflib
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from bs4 import BeautifulSoup as BS
from curl_cffi.requests import AsyncSession as CurlAsync
import httpx

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
BASE_URL = "https://steampeek.hu"
STEAMSPY_API = "https://steamspy.com/api.php"
GAMEVAULT_API = "https://halsbroken.s74zczkfgu.workers.dev"
STEAM_APPDETAILS = "https://store.steampowered.com/api/appdetails"

IMPERSONATE = "chrome131"
DEFAULT_LANG = "it"
DEFAULT_CC = "IT"
DEFAULT_DEPTH = 2
DEFAULT_DL_MAX = 5

CONCURRENCY_PEEK = 10
CONCURRENCY_SPY = 30
CONCURRENCY_DL = 6
CONCURRENCY_DATE = 12
DELAY_PEEK = 0.05
DELAY_SPY = 0.02
DELAY_DL = 0.15
TIMEOUT = 25

CACHE_DIR = Path("/tmp/steampeek_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_ALL = CACHE_DIR / "steamspy_all_cache.json"
CACHE_TTL = 86400

HDR = {
    "accept": "application/json,text/plain,*/*",
    "accept-language": "it-IT,it;q=0.9",
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

# ─────────────────────────────────────────────────────────────
# IN-MEMORY JOB STORE (per async jobs)
# ─────────────────────────────────────────────────────────────
jobs: dict[str, dict] = {}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────
# GAMEVAULT — MATCHING INTELLIGENTE
# ─────────────────────────────────────────────────────────────
def normalize_for_match(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"[™®©]", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    for w in [
        "edition", "deluxe", "ultimate", "goty", "complete",
        "definitive", "remastered", "remake", "game of the year",
        "anniversary", "collectors", "directors cut", "enhanced",
        "vr", "the", "a", "an", "&", "and",
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
    return out[:3]


async def _gv_try_search(client: httpx.AsyncClient, query: str):
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


async def gamevault_search_smart(
    client: httpx.AsyncClient,
    game_name: str,
    sem: asyncio.Semaphore,
    max_variants: int = 5,
):
    async with sem:
        queries = make_search_queries(game_name)
        best_result = None
        best_similarity = 0.0

        for q in queries:
            data = await _gv_try_search(client, q)
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

            if scored and scored[0][0] > best_similarity:
                best_similarity = scored[0][0]
                if scored[0][0] >= 0.55:
                    threshold = max(0.5, scored[0][0] - 0.15)
                    valid = [r for sim, r in scored if sim >= threshold]
                    best_result = {
                        "search_query": q,
                        "matched_similarity": round(scored[0][0], 2),
                        "total_found": len(valid),
                        "variants": [
                            {
                                "title": r.get("titolo"),
                                "url": r.get("url"),
                                "cover": r.get("copertina"),
                                "download_links": r.get("links", [])[:15],
                            }
                            for r in valid[:max_variants]
                        ],
                        "variants_shown": min(len(valid), max_variants),
                    }
                    if scored[0][0] >= 0.85:
                        break

        return best_result


# ─────────────────────────────────────────────────────────────
# STEAMSPY
# ─────────────────────────────────────────────────────────────
async def steamspy_all(client: httpx.AsyncClient) -> dict:
    if CACHE_ALL.exists():
        age = time.time() - CACHE_ALL.stat().st_mtime
        if age < CACHE_TTL:
            try:
                return json.loads(CACHE_ALL.read_text(encoding="utf-8"))
            except Exception:
                pass
    r = await client.get(
        STEAMSPY_API, params={"request": "all"}, headers=HDR, timeout=60
    )
    data = r.json()
    CACHE_ALL.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


async def resolve_appid(client: httpx.AsyncClient, query: str):
    data = await steamspy_all(client)
    q = query.lower().strip()
    exact = []
    contains = []
    names: dict[str, tuple[int, str]] = {}
    for appid, item in data.items():
        name = (item.get("name") or "").strip()
        if not name:
            continue
        nl = name.lower()
        names[nl] = (int(appid), name)
        if nl == q:
            exact.append((int(appid), name))
        elif q in nl:
            contains.append((int(appid), name))
    if exact:
        return exact[0]
    if contains:
        contains.sort(key=lambda x: len(x[1]))
        return contains[0]
    close = difflib.get_close_matches(q, names.keys(), n=1, cutoff=0.65)
    if close:
        return names[close[0]]
    common = {
        "hollow knight": (367520, "Hollow Knight"),
        "elden ring": (1245620, "ELDEN RING"),
        "escape from tarkov": (3932890, "Escape from Tarkov"),
        "tarkov": (3932890, "Escape from Tarkov"),
        "cyberpunk 2077": (1091500, "Cyberpunk 2077"),
    }
    for k, v in common.items():
        if q in k or k in q:
            return v
    return None, None


async def steamspy_details(client: httpx.AsyncClient, appid: int):
    for _ in range(3):
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
                    return d
        except Exception:
            await asyncio.sleep(0.5)
    return None


async def steam_release_date(
    client: httpx.AsyncClient, appid: int, lang: str, cc: str
):
    try:
        r = await client.get(
            STEAM_APPDETAILS,
            params={
                "appids": appid,
                "l": lang,
                "cc": cc,
                "filters": "basic,release_date",
            },
            headers=HDR,
            timeout=15,
        )
        if r.status_code == 200:
            d = r.json()
            entry = d.get(str(appid), {})
            if entry.get("success"):
                rd = entry.get("data", {}).get("release_date", {})
                return {
                    "release_date": rd.get("date"),
                    "coming_soon": rd.get("coming_soon", False),
                    "release_ts": parse_steam_date(rd.get("date")),
                }
    except Exception:
        pass
    return {"release_date": None, "coming_soon": False, "release_ts": 0}


# ─────────────────────────────────────────────────────────────
# STEAMPEEK BFS
# ─────────────────────────────────────────────────────────────
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
    for a in cont.find_all(
        "a", href=re.compile(r"store\.steampowered\.com/app/\d+")
    ):
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
            parent = el.find_parent(
                class_=re.compile(r"lister_item_cont|lister_item")
            )
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


async def fetch_similar(
    session: CurlAsync, appid: int, sem: asyncio.Semaphore
) -> list[dict]:
    async with sem:
        for i in range(3):
            try:
                r = await session.get(
                    f"{BASE_URL}/?appid={appid}",
                    headers={"referer": BASE_URL + "/"},
                    timeout=TIMEOUT,
                )
                await asyncio.sleep(DELAY_PEEK)
                return parse_peek(r.text, appid)
            except Exception:
                await asyncio.sleep(0.4 * (2**i))
        return []


async def bfs(
    peek: CurlAsync,
    seed_id: int,
    seed_name: str,
    depth: int,
    max_total: int | None = None,
) -> list[dict]:
    all_g: dict[int, dict] = {
        seed_id: {
            "appid": seed_id,
            "name": seed_name,
            "depth": 0,
            "parent": None,
        }
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

        log.info(
            "BFS Layer %d/%d — %d nodes to expand", lvl + 1, depth, len(current)
        )

        async def job(parent: int):
            return parent, await fetch_similar(peek, parent, sem)

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

        log.info(
            "BFS Layer %d done — total=%d, next=%d",
            lvl + 1, len(all_g), len(nxt),
        )
        current = nxt

    return [g for g in all_g.values() if g["appid"] != seed_id]


# ─────────────────────────────────────────────────────────────
# BUILD RECORD
# ─────────────────────────────────────────────────────────────
def _owners_min(owners_str) -> int:
    if not owners_str:
        return 0
    m = re.search(r"([\d,]+)", str(owners_str))
    if m:
        return safe_int(m.group(1))
    return 0


def build_record(
    appid: int,
    spy: dict | None,
    extra: dict,
    date_info: dict | None = None,
    dl_info: dict | None = None,
) -> dict:
    spy = spy or {}
    date_info = date_info or {
        "release_date": None,
        "coming_soon": False,
        "release_ts": 0,
    }
    name = spy.get("name") or extra.get("name") or f"#{appid}"
    pos = safe_int(spy.get("positive"))
    neg = safe_int(spy.get("negative"))
    total = pos + neg
    score = round((pos / total) * 100, 2) if total else None
    final = money(spy.get("price"))
    initial = money(spy.get("initialprice"))
    tags_raw = spy.get("tags") or {}
    tags = (
        [
            {"name": k, "votes": v}
            for k, v in sorted(tags_raw.items(), key=lambda x: -x[1])[:30]
        ]
        if isinstance(tags_raw, dict)
        else []
    )
    genres = [
        x.strip()
        for x in str(spy.get("genre") or "").split(",")
        if x.strip()
    ]
    appid = int(appid)
    return {
        "appid": appid,
        "name": name,
        "developer": spy.get("developer"),
        "publisher": spy.get("publisher"),
        "developers": [spy.get("developer")] if spy.get("developer") else [],
        "publishers": [spy.get("publisher")] if spy.get("publisher") else [],
        "genres": genres,
        "tags": tags,
        "languages": spy.get("languages"),
        "release_date": date_info.get("release_date"),
        "release_ts": date_info.get("release_ts", 0),
        "coming_soon": date_info.get("coming_soon", False),
        "price": {
            "final_usd": final,
            "initial_usd": initial,
            "discount_percent": safe_int(spy.get("discount")),
            "formatted": (
                ("FREE" if final == 0 else f"${final:.2f}")
                if final is not None
                else None
            ),
            "is_free": final == 0,
        },
        "reviews": {
            "positive": pos,
            "negative": neg,
            "total": total,
            "score_percent": score,
            "userscore": safe_int(spy.get("userscore")),
        },
        "owners_estimate": spy.get("owners"),
        "owners_min": _owners_min(spy.get("owners")),
        "ccu": safe_int(spy.get("ccu")),
        "playtime": {
            "average_forever_min": safe_int(spy.get("average_forever")),
            "average_2weeks_min": safe_int(spy.get("average_2weeks")),
            "median_forever_min": safe_int(spy.get("median_forever")),
        },
        "images": {
            "header": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg",
            "capsule_616x353": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_616x353.jpg",
            "capsule_231x87": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_231x87.jpg",
            "library_600x900": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/library_600x900.jpg",
            "library_hero": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/library_hero.jpg",
            "logo": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/logo.png",
        },
        "links": {
            "steam_store": f"https://store.steampowered.com/app/{appid}/",
            "steampeek": f"{BASE_URL}/?appid={appid}",
            "steamspy": f"https://steamspy.com/app/{appid}",
            "steamdb": f"https://steamdb.info/app/{appid}/",
            "protondb": f"https://www.protondb.com/app/{appid}",
            "steamcharts": f"https://steamcharts.com/app/{appid}",
            "itad": f"https://isthereanydeal.com/steam/app/{appid}/",
        },
        "downloads": dl_info,
        "has_downloads": bool(dl_info),
        "_bfs_depth": extra.get("depth", 0),
        "_bfs_parent": extra.get("parent"),
    }


# ─────────────────────────────────────────────────────────────
# ENRICHMENT PIPELINE
# ─────────────────────────────────────────────────────────────
async def enrich_all(
    client: httpx.AsyncClient,
    games: list[dict],
    include_dates: bool = True,
    include_downloads: bool = True,
    dl_max: int = DEFAULT_DL_MAX,
    lang: str = "it",
    cc: str = "IT",
) -> list[dict]:
    sem_spy = asyncio.Semaphore(CONCURRENCY_SPY)
    sem_date = asyncio.Semaphore(CONCURRENCY_DATE)
    sem_dl = asyncio.Semaphore(CONCURRENCY_DL)
    results: list[dict] = []
    done_count = 0
    total = len(games)

    async def worker(g: dict) -> dict:
        nonlocal done_count
        async with sem_spy:
            spy = await steamspy_details(client, g["appid"])
            await asyncio.sleep(DELAY_SPY)

        clean_name = (spy or {}).get("name") or g.get("name", "")
        skip_dl = (
            not include_downloads
            or not clean_name
            or clean_name.startswith("#")
        )

        date_task = (
            steam_release_date(client, g["appid"], lang, cc)
            if include_dates
            else None
        )
        dl_task = (
            gamevault_search_smart(client, clean_name, sem_dl, max_variants=dl_max)
            if not skip_dl
            else None
        )

        tasks_list = [t for t in [date_task, dl_task] if t]
        outs = (
            await asyncio.gather(*tasks_list, return_exceptions=True)
            if tasks_list
            else []
        )

        date_info = None
        dl_info = None
        idx = 0
        if include_dates:
            date_info = (
                outs[idx]
                if idx < len(outs) and not isinstance(outs[idx], Exception)
                else None
            )
            idx += 1
        if not skip_dl:
            dl_info = (
                outs[idx]
                if idx < len(outs) and not isinstance(outs[idx], Exception)
                else None
            )

        done_count += 1
        if done_count % 25 == 0 or done_count == total:
            log.info("Enriched %d/%d", done_count, total)

        return build_record(g["appid"], spy, g, date_info, dl_info)

    tasks = [worker(g) for g in games]
    for coro in asyncio.as_completed(tasks):
        try:
            results.append(await coro)
        except Exception as e:
            log.warning("Enrich worker error: %s", e)

    return results


# ─────────────────────────────────────────────────────────────
# SORT & FILTER
# ─────────────────────────────────────────────────────────────
SORT_KEYS = {
    "name": lambda x: (x.get("name") or "").lower(),
    "date": lambda x: x.get("release_ts") or 0,
    "score": lambda x: (x.get("reviews") or {}).get("score_percent") or 0,
    "reviews": lambda x: (x.get("reviews") or {}).get("total") or 0,
    "positive": lambda x: (x.get("reviews") or {}).get("positive") or 0,
    "price": lambda x: (x.get("price") or {}).get("final_usd") or 0,
    "owners": lambda x: x.get("owners_min") or 0,
    "ccu": lambda x: x.get("ccu") or 0,
    "playtime": lambda x: (x.get("playtime") or {}).get("average_forever_min") or 0,
    "depth": lambda x: (x.get("_bfs_depth", 99), (x.get("name") or "").lower()),
    "appid": lambda x: x.get("appid") or 0,
    "downloads": lambda x: (x.get("downloads") or {}).get("total_found") or 0,
}


def sort_records(records: list[dict], field: str = "depth", desc: bool = False):
    if field not in SORT_KEYS:
        field = "depth"
    return sorted(records, key=SORT_KEYS[field], reverse=desc)


def filter_records(
    records: list[dict],
    min_review: int = 0,
    min_score: float = 0,
    free_only: bool = False,
    paid_only: bool = False,
    dl_only: bool = False,
) -> list[dict]:
    out = []
    for g in records:
        rev = g.get("reviews") or {}
        if min_review and (rev.get("total") or 0) < min_review:
            continue
        if min_score and (rev.get("score_percent") or 0) < min_score:
            continue
        pr = g.get("price") or {}
        if free_only and not pr.get("is_free"):
            continue
        if paid_only and pr.get("is_free"):
            continue
        if dl_only and not g.get("has_downloads"):
            continue
        out.append(g)
    return out


# ─────────────────────────────────────────────────────────────
# CORE SCRAPE
# ─────────────────────────────────────────────────────────────
async def scrape(
    query: str,
    depth: int = DEFAULT_DEPTH,
    appid: int | None = None,
    max_total: int | None = None,
    lang: str = DEFAULT_LANG,
    cc: str = DEFAULT_CC,
    include_dates: bool = True,
    include_downloads: bool = True,
    dl_max: int = DEFAULT_DL_MAX,
) -> list[dict]:
    t0 = time.time()

    async with (
        httpx.AsyncClient(
            timeout=TIMEOUT, follow_redirects=True, headers=HDR
        ) as spy_client,
        CurlAsync(impersonate=IMPERSONATE) as peek_client,
    ):
        # Warmup
        try:
            await peek_client.get(BASE_URL + "/", timeout=TIMEOUT)
        except Exception:
            pass

        # Resolve seed
        if appid:
            seed_id, seed_name = appid, query
        else:
            seed_id, seed_name = await resolve_appid(spy_client, query)
            if not seed_id:
                raise HTTPException(
                    status_code=404,
                    detail=f"Cannot resolve game: '{query}'",
                )
        log.info("Seed: %s (appid=%d)", seed_name, seed_id)

        # BFS Discovery
        t1 = time.time()
        discovered = await bfs(peek_client, seed_id, seed_name, depth, max_total)
        t2 = time.time()
        log.info(
            "BFS done: %d games in %.2fs", len(discovered), t2 - t1
        )

        # Enrichment
        enriched = await enrich_all(
            spy_client,
            discovered,
            include_dates=include_dates,
            include_downloads=include_downloads,
            dl_max=dl_max,
            lang=lang,
            cc=cc,
        )
        t3 = time.time()
        log.info(
            "Enrichment done: %d records in %.2fs", len(enriched), t3 - t2
        )

    with_dl = sum(1 for x in enriched if x.get("has_downloads"))
    log.info(
        "Total: %d records, %d with downloads, %.2fs elapsed",
        len(enriched), with_dl, time.time() - t0,
    )
    return enriched


# ─────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 SteamPeek API v4 starting on Railway")
    yield
    log.info("👋 SteamPeek API shutting down")


app = FastAPI(
    title="SteamPeek API",
    version="4.0.0",
    description=(
        "Steam game discovery API with BFS similarity crawling, "
        "SteamSpy enrichment, and GameVault auto-download matching."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health ───────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "steampeek-api",
        "version": "4.0.0",
        "timestamp": datetime.utcnow().isoformat(),
    }


# ── Search (sincrono — attende il risultato) ────────────────
@app.get("/api/search")
async def api_search(
    q: str = Query(..., description="Game name to search"),
    depth: int = Query(DEFAULT_DEPTH, ge=1, le=4, description="BFS depth 1-4"),
    max: Optional[int] = Query(None, ge=1, description="Max games to discover"),
    lang: str = Query(DEFAULT_LANG, description="Language code"),
    cc: str = Query(DEFAULT_CC, description="Country code"),
    dates: bool = Query(True, description="Include release dates"),
    downloads: bool = Query(True, description="Include download links"),
    dl_max: int = Query(DEFAULT_DL_MAX, ge=1, le=20, description="Max download variants per game"),
    sort: str = Query("depth", description="Sort field"),
    desc: bool = Query(False, description="Descending order"),
    min_reviews: int = Query(0, ge=0, description="Min total reviews"),
    min_score: float = Query(0, ge=0, le=100, description="Min score %"),
    free_only: bool = Query(False, description="Only free games"),
    paid_only: bool = Query(False, description="Only paid games"),
    dl_only: bool = Query(False, description="Only games with downloads"),
    limit: Optional[int] = Query(None, ge=1, description="Limit results returned"),
):
    """
    Synchronous search — BFS discovery + enrichment + download matching.
    Returns full results when done. Can take 30s-5min depending on depth.
    """
    t0 = time.time()
    records = await scrape(
        query=q,
        depth=depth,
        max_total=max,
        lang=lang,
        cc=cc,
        include_dates=dates,
        include_downloads=downloads,
        dl_max=dl_max,
    )
    if not records:
        return {
            "query": q,
            "total": 0,
            "results": [],
            "elapsed_seconds": round(time.time() - t0, 2),
        }

    records = filter_records(
        records,
        min_review=min_reviews,
        min_score=min_score,
        free_only=free_only,
        paid_only=paid_only,
        dl_only=dl_only,
    )
    records = sort_records(records, field=sort, desc=desc)

    if limit:
        records = records[:limit]

    return {
        "query": q,
        "total": len(records),
        "with_downloads": sum(1 for r in records if r.get("has_downloads")),
        "elapsed_seconds": round(time.time() - t0, 2),
        "params": {
            "depth": depth,
            "max": max,
            "sort": sort,
            "desc": desc,
            "downloads": downloads,
            "dates": dates,
        },
        "results": records,
    }


# ── Search by AppID ─────────────────────────────────────────
@app.get("/api/appid/{appid}")
async def api_appid(
    appid: int,
    depth: int = Query(DEFAULT_DEPTH, ge=1, le=4),
    max: Optional[int] = Query(None, ge=1),
    lang: str = Query(DEFAULT_LANG),
    cc: str = Query(DEFAULT_CC),
    dates: bool = Query(True),
    downloads: bool = Query(True),
    dl_max: int = Query(DEFAULT_DL_MAX, ge=1, le=20),
    sort: str = Query("depth"),
    desc: bool = Query(False),
    min_reviews: int = Query(0, ge=0),
    min_score: float = Query(0, ge=0, le=100),
    free_only: bool = Query(False),
    paid_only: bool = Query(False),
    dl_only: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1),
):
    """Search by Steam AppID directly."""
    t0 = time.time()
    records = await scrape(
        query=f"AppID {appid}",
        depth=depth,
        appid=appid,
        max_total=max,
        lang=lang,
        cc=cc,
        include_dates=dates,
        include_downloads=downloads,
        dl_max=dl_max,
    )

    records = filter_records(
        records,
        min_review=min_reviews,
        min_score=min_score,
        free_only=free_only,
        paid_only=paid_only,
        dl_only=dl_only,
    )
    records = sort_records(records, field=sort, desc=desc)
    if limit:
        records = records[:limit]

    return {
        "appid": appid,
        "total": len(records),
        "with_downloads": sum(1 for r in records if r.get("has_downloads")),
        "elapsed_seconds": round(time.time() - t0, 2),
        "results": records,
    }


# ── Async Job (per ricerche lunghe) ─────────────────────────
@app.post("/api/search/async")
async def api_search_async(
    background_tasks: BackgroundTasks,
    q: str = Query(...),
    depth: int = Query(DEFAULT_DEPTH, ge=1, le=4),
    max: Optional[int] = Query(None, ge=1),
    lang: str = Query(DEFAULT_LANG),
    cc: str = Query(DEFAULT_CC),
    dates: bool = Query(True),
    downloads: bool = Query(True),
    dl_max: int = Query(DEFAULT_DL_MAX, ge=1, le=20),
    sort: str = Query("depth"),
    desc: bool = Query(False),
):
    """
    Start async search job. Returns job_id immediately.
    Poll /api/jobs/{job_id} for status and results.
    """
    import uuid

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "query": q,
        "started_at": datetime.utcnow().isoformat(),
        "progress": "Starting BFS...",
        "results": None,
        "error": None,
    }

    async def run_job():
        try:
            records = await scrape(
                query=q,
                depth=depth,
                max_total=max,
                lang=lang,
                cc=cc,
                include_dates=dates,
                include_downloads=downloads,
                dl_max=dl_max,
            )
            records = sort_records(records, field=sort, desc=desc)
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["results"] = {
                "total": len(records),
                "with_downloads": sum(
                    1 for r in records if r.get("has_downloads")
                ),
                "records": records,
            }
            jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)

    background_tasks.add_task(run_job)

    return {
        "job_id": job_id,
        "status": "running",
        "poll_url": f"/api/jobs/{job_id}",
    }


@app.get("/api/jobs/{job_id}")
async def api_job_status(job_id: str):
    """Check status of async search job."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]


@app.get("/api/jobs")
async def api_jobs_list():
    """List all jobs."""
    return {
        "total": len(jobs),
        "jobs": [
            {
                "id": j["id"],
                "status": j["status"],
                "query": j["query"],
                "started_at": j["started_at"],
            }
            for j in jobs.values()
        ],
    }


# ── Quick Resolve (solo lookup nome → appid) ────────────────
@app.get("/api/resolve")
async def api_resolve(q: str = Query(..., description="Game name")):
    """Quickly resolve a game name to Steam AppID via SteamSpy catalog."""
    async with httpx.AsyncClient(
        timeout=60, follow_redirects=True, headers=HDR
    ) as client:
        appid, name = await resolve_appid(client, q)
    if not appid:
        raise HTTPException(
            status_code=404, detail=f"Cannot resolve: '{q}'"
        )
    return {
        "query": q,
        "appid": appid,
        "name": name,
        "steam_url": f"https://store.steampowered.com/app/{appid}/",
    }


# ── Quick Download Search (solo GameVault) ───────────────────
@app.get("/api/downloads")
async def api_downloads(
    q: str = Query(..., description="Game name"),
    max_variants: int = Query(5, ge=1, le=20),
):
    """Search download links for a game via GameVault only."""
    sem = asyncio.Semaphore(3)
    async with httpx.AsyncClient(
        timeout=30, follow_redirects=True, headers=HDR
    ) as client:
        result = await gamevault_search_smart(
            client, q, sem, max_variants=max_variants
        )
    if not result:
        return {"query": q, "found": False, "downloads": None}
    return {"query": q, "found": True, "downloads": result}


# ── SteamSpy Proxy ──────────────────────────────────────────
@app.get("/api/steamspy/{appid}")
async def api_steamspy(appid: int):
    """Proxy SteamSpy details for a single AppID."""
    async with httpx.AsyncClient(
        timeout=TIMEOUT, follow_redirects=True, headers=HDR
    ) as client:
        data = await steamspy_details(client, appid)
    if not data:
        raise HTTPException(
            status_code=404, detail=f"SteamSpy data not found for {appid}"
        )
    return data


# ── Docs redirect ───────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "SteamPeek API v4",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health",
        "endpoints": {
            "search": "GET /api/search?q=game+name",
            "appid": "GET /api/appid/{appid}",
            "async_search": "POST /api/search/async?q=game+name",
            "job_status": "GET /api/jobs/{job_id}",
            "resolve": "GET /api/resolve?q=game+name",
            "downloads": "GET /api/downloads?q=game+name",
            "steamspy": "GET /api/steamspy/{appid}",
        },
    }


# ─────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")