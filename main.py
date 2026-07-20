"""
SteamPeek API v6 PRO — Railway Edition
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Risolto: SteamPeek carica i dati via AJAX (/gsearch). 
Ora scarica correttamente TUTTE le pagine e tutti i risultati.
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
CONCURRENCY_PAGES = 5

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
CACHE_MAX_ENTRIES = 500

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
# CACHE HELPERS
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
        if x is None or x == "": return d
        return int(str(x).replace(",", ""))
    except Exception: return d

def money(x):
    try:
        v = int(str(x))
        if v <= 0: return None
        return round(v / 100, 2)
    except Exception: return None

def parse_steam_date(s: str | None) -> int:
    if not s: return 0
    s = str(s).strip()
    for fmt in ["%b %d, %Y", "%d %b, %Y", "%B %d, %Y", "%d %B, %Y", "%B %Y", "%b %Y"]:
        try: return int(datetime.strptime(s, fmt).timestamp())
        except Exception: pass
    m = re.search(r"Q([1-4])\s*(\d{4})", s)
    if m:
        q, y = int(m.group(1)), int(m.group(2))
        try: return int(datetime(y, (q - 1) * 3 + 1, 1).timestamp())
        except Exception: pass
    m = re.search(r"\b(19|20)\d{2}\b", s)
    if m:
        try: return int(datetime(int(m.group(0)), 6, 15).timestamp())
        except Exception: pass
    if any(k in s.lower() for k in ["coming", "tba", "announce", "soon"]):
        return 9999999999
    return 0


# ═══════════════════════════════════════════════════════════════
# GAMEVAULT
# ═══════════════════════════════════════════════════════════════
def normalize_for_match(s: str) -> str:
    if not s: return ""
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
    if not na or not nb: return 0
    if na == nb: return 1.0
    if na in nb or nb in na: return 0.85
    return difflib.SequenceMatcher(None, na, nb).ratio()

def make_search_queries(name: str) -> list[str]:
    queries = [name.strip()]
    clean = re.sub(r"[™®©]", "", name).strip()
    if clean != queries[0]: queries.append(clean)
    m = re.split(r"[:\-–—]", clean, 1)
    if m[0].strip() and m[0].strip() != clean: queries.append(m[0].strip())
    stripped = re.sub(r"\s*(deluxe|ultimate|goty|complete|definitive|remastered|game of the year|anniversary|collectors|enhanced|directors cut|edition)\s*$", "", clean, flags=re.I).strip()
    if stripped and stripped != clean: queries.append(stripped)
    
    seen, out = set(), []
    for q in queries:
        if len(q) < 3: continue
        k = q.lower()
        if k not in seen:
            seen.add(k)
            out.append(q)
    return out[:4]

async def gamevault_search(client: httpx.AsyncClient, game_name: str, sem: asyncio.Semaphore, max_variants: int = 8, min_similarity: float = 0.55):
    cache_key = make_cache_key("gv", game_name, max_variants)
    cached = cache_get(cache_key)
    if cached is not None: return cached

    async with sem:
        queries = make_search_queries(game_name)
        best_result = None
        best_sim = 0.0

        for q in queries:
            try:
                r = await client.get(f"{GAMEVAULT_API}/cercaTutto", params={"q": q}, headers=HDR, timeout=25)
                data = r.json() if r.status_code == 200 else None
            except Exception:
                data = None
            
            await asyncio.sleep(DELAY_DL)
            if not data or not data.get("risultati"): continue

            scored = []
            for r in data["risultati"]:
                title = r.get("titolo", "")
                clean_title = re.sub(r"\b(build|v|version|update)\s*[\d.]+.*$", "", title, flags=re.I).strip()
                clean_title = re.sub(r"\s+", " ", clean_title)
                scored.append((similarity(game_name, clean_title), r))

            scored.sort(key=lambda x: -x[0])

            if scored and scored[0][0] > best_sim:
                best_sim = scored[0][0]
                if scored[0][0] >= min_similarity:
                    threshold = max(0.5, scored[0][0] - 0.15)
                    valid = [r for sim, r in scored if sim >= threshold]
                    best_result = {
                        "search_query": q,
                        "matched_similarity": round(scored[0][0], 3),
                        "match_confidence": "perfect" if scored[0][0] >= 0.95 else "high" if scored[0][0] >= 0.8 else "medium" if scored[0][0] >= 0.65 else "low",
                        "total_found": len(valid),
                        "variants": [{
                            "title": r.get("titolo"), "url": r.get("url"), "cover": r.get("copertina"),
                            "download_links": r.get("links", [])[:20], "num_links": len(r.get("links", []))
                        } for r in valid[:max_variants]],
                        "variants_shown": min(len(valid), max_variants),
                        "total_download_links": sum(len(r.get("links", [])) for r in valid[:max_variants]),
                    }
                    if scored[0][0] >= 0.9: break

        cache_set(cache_key, best_result)
        return best_result


# ═══════════════════════════════════════════════════════════════
# STEAMSPY & STEAM STORE
# ═══════════════════════════════════════════════════════════════
async def steamspy_all(client: httpx.AsyncClient) -> dict:
    if CACHE_ALL.exists():
        if time.time() - CACHE_ALL.stat().st_mtime < CACHE_ALL_TTL:
            try: return json.loads(CACHE_ALL.read_text(encoding="utf-8"))
            except Exception: pass
    r = await client.get(STEAMSPY_API, params={"request": "all"}, headers=HDR, timeout=90)
    data = r.json()
    CACHE_ALL.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data

async def resolve_appid(client: httpx.AsyncClient, query: str):
    cache_key = make_cache_key("resolve", query)
    cached = cache_get(cache_key)
    if cached is not None: return cached
    q = query.strip()

    try:
        r = await client.get(STEAM_SEARCH, params={"term": q, "l": "english", "cc": "US"}, headers=HDR, timeout=15)
        if r.status_code == 200:
            items = r.json().get("items", [])
            if items:
                scored = [(similarity(q, it.get("name", "")), it) for it in items]
                scored.sort(key=lambda x: -x[0])
                if scored[0][0] >= 0.5:
                    res = (int(scored[0][1]["id"]), scored[0][1]["name"])
                    cache_set(cache_key, res); return res
    except Exception: pass

    try:
        data = await steamspy_all(client)
        ql = q.lower()
        exact, contains, names = [], [], {}
        for appid, item in data.items():
            name = (item.get("name") or "").strip()
            if not name: continue
            nl = name.lower()
            names[nl] = (int(appid), name)
            if nl == ql: exact.append((int(appid), name))
            elif ql in nl: contains.append((int(appid), name))
        if exact: res = exact[0]; cache_set(cache_key, res); return res
        if contains:
            contains.sort(key=lambda x: len(x[1]))
            res = contains[0]; cache_set(cache_key, res); return res
        close = difflib.get_close_matches(ql, names.keys(), n=1, cutoff=0.65)
        if close: res = names[close[0]]; cache_set(cache_key, res); return res
    except Exception: pass
    return (None, None)

async def steamspy_details(client: httpx.AsyncClient, appid: int):
    cache_key = make_cache_key("spy", appid)
    cached = cache_get(cache_key)
    if cached is not None: return cached
    for _ in range(RETRY_ATTEMPTS):
        try:
            r = await client.get(STEAMSPY_API, params={"request": "appdetails", "appid": appid}, headers=HDR, timeout=TIMEOUT)
            if r.status_code == 200:
                d = r.json()
                if d and d.get("appid"): cache_set(cache_key, d); return d
        except Exception: await asyncio.sleep(0.5)
    return None

async def steam_store_details(client: httpx.AsyncClient, appid: int, lang: str = "italian", cc: str = "IT"):
    cache_key = make_cache_key("store", appid, lang, cc)
    cached = cache_get(cache_key)
    if cached is not None: return cached
    for _ in range(RETRY_ATTEMPTS):
        try:
            r = await client.get(STEAM_APPDETAILS, params={"appids": appid, "l": lang, "cc": cc}, headers=HDR, timeout=20)
            if r.status_code == 200:
                entry = r.json().get(str(appid), {})
                if entry.get("success"):
                    data = entry.get("data", {})
                    cache_set(cache_key, data); return data
                cache_set(cache_key, None); return None
        except Exception: await asyncio.sleep(0.5)
    return None

async def steam_reviews_summary(client: httpx.AsyncClient, appid: int):
    cache_key = make_cache_key("rev", appid)
    cached = cache_get(cache_key)
    if cached is not None: return cached
    try:
        r = await client.get(f"{STEAM_REVIEWS}/{appid}", params={"json": 1, "language": "all", "purchase_type": "all", "num_per_page": 0}, headers=HDR, timeout=15)
        if r.status_code == 200:
            summary = r.json().get("query_summary", {})
            cache_set(cache_key, summary); return summary
    except Exception: pass
    return None


# ═══════════════════════════════════════════════════════════════
# STEAMPEEK — AJAX DISCOVERY (IL FIX VERO)
# ═══════════════════════════════════════════════════════════════
def parse_peek_html(html: str, src: int) -> list[dict]:
    """Parsa HTML grezzo in cerca di data-appid"""
    soup = BS(html, "lxml")
    games = {}
    for el in soup.select("[data-appid]"):
        try:
            aid = int(el.get("data-appid"))
            if aid == src or aid in games: continue
            
            # extract name
            name = ""
            img = el.find("img", alt=True)
            if img:
                alt = re.sub(r"\s+(and\s+similar\s+games|thumbnail|logo|capsule).*$", "", img["alt"], flags=re.I).strip()
                if alt and len(alt)>1 and not alt.startswith("#"): name = alt
            if not name:
                for a in ["data-appname", "data-name", "title", "aria-label"]:
                    if el.get(a): name = el.get(a).strip(); break
            
            games[aid] = {"appid": aid, "name": name or f"#{aid}"}
        except Exception: continue
    return list(games.values())

async def fetch_ajax_page(session: CurlAsync, appid: int, page: int, sem: asyncio.Semaphore) -> tuple[list[dict], int]:
    """Chiama l'endpoint POST AJAX reale di SteamPeek"""
    async with sem:
        for i in range(RETRY_ATTEMPTS):
            try:
                r = await session.post(
                    f"{BASE_URL}/gsearch",
                    data={"appid": str(appid), "similiraty": "8", "page": str(page), "order": "similarity"},
                    headers={"x-requested-with": "XMLHttpRequest"},
                    timeout=TIMEOUT
                )
                if r.status_code == 200:
                    data = r.json()
                    # Il json contiene "html" con i div renderizzati
                    html = data.get("html", "")
                    games = parse_peek_html(html, appid)
                    total_pages = safe_int(data.get("total_pages"), 1)
                    return games, total_pages
            except Exception as e:
                log.debug(f"AJAX {appid} p{page} error: {e}")
                await asyncio.sleep(0.4 * (2 ** i))
        return [], 1

async def fetch_all_similar_ajax(session: CurlAsync, appid: int, sem: asyncio.Semaphore, max_pages: int = 5) -> list[dict]:
    # 1. Prima pagina
    p1_games, total_pages = await fetch_ajax_page(session, appid, 1, sem)
    all_games = {g["appid"]: g for g in p1_games}
    total_pages = min(total_pages, max_pages)

    if total_pages <= 1:
        return list(all_games.values())

    # 2. Pagine successive in parallelo
    page_sem = asyncio.Semaphore(CONCURRENCY_PAGES)
    async def fetch_p(p: int):
        games, _ = await fetch_ajax_page(session, appid, p, page_sem)
        return games

    results = await asyncio.gather(*[fetch_p(p) for p in range(2, total_pages + 1)], return_exceptions=True)
    for res in results:
        if isinstance(res, Exception): continue
        for g in res:
            if g["appid"] not in all_games:
                all_games[g["appid"]] = g

    return list(all_games.values())

async def bfs_discover(peek: CurlAsync, seed_id: int, seed_name: str, depth: int, max_total: int | None = None, max_pages_per_game: int = 5) -> list[dict]:
    all_g = {seed_id: {"appid": seed_id, "name": seed_name, "depth": 0, "parent": None}}
    current = [seed_id]
    visited = {seed_id}
    sem = asyncio.Semaphore(CONCURRENCY_PEEK)
    unlimited = max_total is None

    for lvl in range(depth):
        if not current or (not unlimited and len(all_g) >= max_total): break
        log.info("BFS L%d/%d — expanding %d nodes", lvl + 1, depth, len(current))

        async def job(parent: int):
            games = await fetch_all_similar_ajax(peek, parent, sem, max_pages=max_pages_per_game)
            return parent, games

        tasks = [job(a) for a in current]
        nxt = []
        for coro in asyncio.as_completed(tasks):
            parent, games = await coro
            for g in games:
                gid = g["appid"]
                if gid in all_g: continue
                if not unlimited and len(all_g) >= max_total: break
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
# BUILD RECORD & ENRICHMENT
# ═══════════════════════════════════════════════════════════════
def build_full_record(appid: int, spy: dict | None, store: dict | None, rev_sum: dict | None, dl_info: dict | None, extra: dict | None = None) -> dict:
    spy, store, rev_sum, extra = spy or {}, store or {}, rev_sum or {}, extra or {}
    appid = int(appid)
    name = store.get("name") or spy.get("name") or extra.get("name") or f"#{appid}"

    pos, neg = safe_int(spy.get("positive")), safe_int(spy.get("negative"))
    total = pos + neg
    score = round((pos / total) * 100, 2) if total else None

    steam_total = safe_int(rev_sum.get("total_reviews"))
    steam_pos = safe_int(rev_sum.get("total_positive"))
    steam_score = round((steam_pos / steam_total) * 100, 2) if steam_total > 0 else None

    if store.get("is_free"):
        price_info = {"final": 0, "currency": "USD", "formatted": "FREE", "is_free": True}
    elif store.get("price_overview"):
        po = store["price_overview"]
        price_info = {"final": po.get("final", 0) / 100, "currency": po.get("currency"), "formatted": po.get("final_formatted"), "is_free": False}
    else:
        final = money(spy.get("price"))
        price_info = {"final": final, "currency": "USD", "formatted": ("FREE" if final == 0 else f"${final:.2f}" if final is not None else None), "is_free": final == 0}

    tags_raw = spy.get("tags") or {}
    tags = [{"name": k, "votes": v} for k, v in sorted(tags_raw.items(), key=lambda x: -x[1])[:20]] if isinstance(tags_raw, dict) else []

    genres = list(dict.fromkeys([g.get("description") for g in store.get("genres", []) if g.get("description")] + [x.strip() for x in str(spy.get("genre") or "").split(",") if x.strip()]))

    rd = store.get("release_date", {})
    release_date = rd.get("date") or spy.get("release_date")

    def clean(s): return BS(s, "lxml").get_text(" ", strip=True) if s else ""

    return {
        "appid": appid, "name": name,
        "type": store.get("type", "game"),
        "short_description": store.get("short_description"),
        "developers": store.get("developers") or ([spy.get("developer")] if spy.get("developer") else []),
        "genres": genres, "tags": tags,
        "release_date": release_date, "release_ts": parse_steam_date(release_date),
        "price": price_info,
        "reviews": {
            "steamspy": {"positive": pos, "negative": neg, "total": total, "score_percent": score},
            "steam": {"total_reviews": steam_total, "score_percent": steam_score, "review_score_desc": rev_sum.get("review_score_desc")}
        },
        "owners_estimate": spy.get("owners"),
        "owners_min": safe_int((re.search(r"([\d,]+)", str(spy.get("owners"))) or type('obj', (object,), {'group': lambda self, x: 0}))().group(1)),
        "images": {
            "header": store.get("header_image") or f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg",
            "capsule_616x353": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_616x353.jpg",
        },
        "links": {
            "steam_store": f"https://store.steampowered.com/app/{appid}/",
            "steampeek": f"{BASE_URL}/?appid={appid}",
        },
        "downloads": dl_info,
        "has_downloads": bool(dl_info),
        "_bfs_depth": extra.get("depth"),
    }

async def enrich_single_game(http_client: httpx.AsyncClient, appid: int, name_hint: str = None, include_downloads: bool = True, dl_max: int = 8, lang: str = DEFAULT_LANG, cc: str = DEFAULT_CC, extra: dict = None) -> dict:
    sem_dl = asyncio.Semaphore(CONCURRENCY_DL)
    spy = await steamspy_details(http_client, appid)
    clean_name = (spy or {}).get("name") or name_hint or ""
    
    tasks = [steam_store_details(http_client, appid, lang, cc), steam_reviews_summary(http_client, appid)]
    if include_downloads and clean_name and not clean_name.startswith("#"):
        tasks.append(gamevault_search(http_client, clean_name, sem_dl, dl_max))
    else: tasks.append(asyncio.sleep(0, result=None))
    
    res = await asyncio.gather(*tasks, return_exceptions=True)
    return build_full_record(appid, spy, res[0] if not isinstance(res[0], Exception) else None, res[1] if not isinstance(res[1], Exception) else None, res[2] if not isinstance(res[2], Exception) else None, extra or {"name": name_hint})

async def enrich_batch(http_client: httpx.AsyncClient, games: list[dict], include_downloads: bool = True, dl_max: int = 5, lang: str = DEFAULT_LANG, cc: str = DEFAULT_CC, full_details: bool = False) -> list[dict]:
    sem_spy, sem_store, sem_rev, sem_dl = asyncio.Semaphore(CONCURRENCY_SPY), asyncio.Semaphore(CONCURRENCY_STORE), asyncio.Semaphore(CONCURRENCY_REVIEWS), asyncio.Semaphore(CONCURRENCY_DL)
    results, done, total = [], 0, len(games)

    async def worker(g: dict):
        nonlocal done
        async with sem_spy: spy = await steamspy_details(http_client, g["appid"])
        clean_name = (spy or {}).get("name") or g.get("name", "")
        
        tasks = []
        if full_details:
            async def _s(): async with sem_store: return await steam_store_details(http_client, g["appid"], lang, cc)
            async def _r(): async with sem_rev: return await steam_reviews_summary(http_client, g["appid"])
            tasks.extend([_s(), _r()])
        else: tasks.extend([asyncio.sleep(0, result=None), asyncio.sleep(0, result=None)])
        
        if include_downloads and clean_name and not clean_name.startswith("#"): tasks.append(gamevault_search(http_client, clean_name, sem_dl, dl_max))
        else: tasks.append(asyncio.sleep(0, result=None))

        res = await asyncio.gather(*tasks, return_exceptions=True)
        done += 1
        if done % 25 == 0 or done == total: log.info("Enriched %d/%d", done, total)
        return build_full_record(g["appid"], spy, res[0] if not isinstance(res[0], Exception) else None, res[1] if not isinstance(res[1], Exception) else None, res[2] if not isinstance(res[2], Exception) else None, g)

    for coro in asyncio.as_completed([worker(g) for g in games]):
        try: results.append(await coro)
        except Exception: pass
    return results


# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════
jobs: dict[str, dict] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 SteamPeek API v6 PRO starting")
    asyncio.create_task(prewarm())
    yield

async def prewarm():
    try:
        async with httpx.AsyncClient(timeout=90, headers=HDR) as c: await steamspy_all(c)
    except: pass

app = FastAPI(title="SteamPeek API v6", default_response_class=ORJSONResponse, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health(): return {"status": "ok", "version": "6.0.0", "cache": len(_memory_cache)}

@app.get("/resolve")
async def endpoint_resolve(q: str = Query(...)):
    async with httpx.AsyncClient(timeout=60, headers=HDR) as c: appid, name = await resolve_appid(c, q)
    if not appid: raise HTTPException(404, detail=f"Non trovato: '{q}'")
    return {"query": q, "appid": appid, "name": name, "steam_url": f"https://store.steampowered.com/app/{appid}/"}

@app.get("/cerca")
async def endpoint_cerca(q: str = Query(...), lang: str = Query(DEFAULT_LANG), cc: str = Query(DEFAULT_CC), downloads: bool = Query(True), dl_max: int = Query(10, ge=1, le=30)):
    t0 = time.time()
    try: appid, name_hint = int(q.strip()), None
    except ValueError:
        async with httpx.AsyncClient(timeout=60, headers=HDR) as c: appid, name_hint = await resolve_appid(c, q)
        if not appid: raise HTTPException(404, detail=f"Non trovato: '{q}'")

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HDR, follow_redirects=True) as c:
        record = await enrich_single_game(c, appid, name_hint=name_hint, include_downloads=downloads, dl_max=dl_max, lang=lang, cc=cc)
    return {"ok": True, "query": q, "resolved_appid": appid, "elapsed_seconds": round(time.time() - t0, 2), "game": record}

@app.get("/simili")
async def endpoint_simili(
    q: str = Query(...),
    depth: int = Query(2, ge=1, le=4),
    max_total: Optional[int] = Query(None, ge=1, alias="max"),
    max_pages_per_game: int = Query(5, ge=1, le=20),
    lang: str = Query(DEFAULT_LANG), cc: str = Query(DEFAULT_CC),
    downloads: bool = Query(True), dl_max: int = Query(5, ge=1, le=20),
    full: bool = Query(False), sort: str = Query("depth"), desc: bool = Query(False),
    min_score: float = Query(0, ge=0, le=100), min_reviews: int = Query(0, ge=0),
    free_only: bool = Query(False), paid_only: bool = Query(False), dl_only: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1)
):
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HDR, follow_redirects=True) as http_c, CurlAsync(impersonate=IMPERSONATE) as peek_c:
            try: appid, seed_name = int(q.strip()), q
            except ValueError:
                appid, seed_name = await resolve_appid(http_c, q)
                if not appid: raise HTTPException(404, detail=f"Non trovato: '{q}'")

            discovered = await bfs_discover(peek_c, appid, seed_name, depth, max_total, max_pages_per_game=max_pages_per_game)
            seed_task = enrich_single_game(http_c, appid, name_hint=seed_name, include_downloads=downloads, dl_max=(dl_max if dl_max >= 10 else 10), lang=lang, cc=cc, extra={"depth": 0, "name": seed_name})
            similar_task = enrich_batch(http_c, discovered, include_downloads=downloads, dl_max=dl_max, lang=lang, cc=cc, full_details=full)
            seed_rec, sim_recs = await asyncio.gather(seed_task, similar_task)

        def filter_ok(g: dict) -> bool:
            r = ((g.get("reviews") or {}).get("steamspy") or {})
            if min_reviews and (r.get("total") or 0) < min_reviews: return False
            if min_score and (r.get("score_percent") or 0) < min_score: return False
            pr = g.get("price") or {}
            if free_only and not pr.get("is_free"): return False
            if paid_only and pr.get("is_free"): return False
            if dl_only and not g.get("has_downloads"): return False
            return True

        filtered = [g for g in sim_recs if filter_ok(g)]
        
        SK = {
            "depth": lambda x: (x.get("_bfs_depth") if x.get("_bfs_depth") is not None else 99, (x.get("name") or "").lower()),
            "name": lambda x: (x.get("name") or "").lower(),
            "date": lambda x: x.get("release_ts") or 0,
            "score": lambda x: (((x.get("reviews") or {}).get("steamspy") or {}).get("score_percent") or 0),
            "reviews": lambda x: (((x.get("reviews") or {}).get("steamspy") or {}).get("total") or 0),
            "price": lambda x: (x.get("price") or {}).get("final") or 0,
            "downloads": lambda x: (x.get("downloads") or {}).get("total_found") or 0,
            "owners": lambda x: x.get("owners_min") or 0,
        }
        filtered.sort(key=SK.get(sort, SK["depth"]), reverse=desc)
        if limit: filtered = filtered[:limit]

        with_dl = sum(1 for r in filtered if r.get("has_downloads"))
        return {
            "ok": True, "query": q, "resolved_appid": appid,
            "seed": seed_rec, "similar": filtered,
            "stats": {
                "total_discovered": len(discovered), "total_after_filter": len(filtered),
                "with_downloads": with_dl, "download_rate_pct": round(with_dl * 100 / max(len(filtered), 1), 1),
                "elapsed_seconds": round(time.time() - t0, 2),
                "params": {"depth": depth, "max": max_total, "max_pages": max_pages_per_game}
            }
        }
    except HTTPException: raise
    except Exception as e: raise HTTPException(500, detail={"error": "simili_failed", "message": str(e)})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), log_level="info")
