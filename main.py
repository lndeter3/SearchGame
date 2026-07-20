"""
SteamPeek API v7 — Railway Edition
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FIX: SteamPeek richiede JS. Usiamo SteamSpy "genre" e "tag" 
per trovare i simili in modo AFFIDABILE.
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
from collections import Counter

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from bs4 import BeautifulSoup as BS
import httpx

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
STEAMSPY_API = "https://steamspy.com/api.php"
GAMEVAULT_API = "https://halsbroken.s74zczkfgu.workers.dev"
STEAM_APPDETAILS = "https://store.steampowered.com/api/appdetails"
STEAM_REVIEWS = "https://store.steampowered.com/appreviews"
STEAM_SEARCH = "https://store.steampowered.com/api/storesearch"

DEFAULT_LANG = "italian"
DEFAULT_CC = "IT"

CONCURRENCY_SPY = 40
CONCURRENCY_STORE = 20
CONCURRENCY_DL = 8
CONCURRENCY_REVIEWS = 15
CONCURRENCY_TAG = 10

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
CACHE_TAG_TTL = 3600 * 6

_memory_cache: dict[str, tuple[float, Any]] = {}
CACHE_MAX_ENTRIES = 2000

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
def cache_get(key, ttl=CACHE_GAME_TTL):
    if key in _memory_cache:
        ts, val = _memory_cache[key]
        if time.time() - ts < ttl:
            return val
        del _memory_cache[key]
    return None


def cache_set(key, value):
    if len(_memory_cache) >= CACHE_MAX_ENTRIES:
        sorted_keys = sorted(_memory_cache.items(), key=lambda x: x[1][0])
        for k, _ in sorted_keys[: CACHE_MAX_ENTRIES // 5]:
            del _memory_cache[k]
    _memory_cache[key] = (time.time(), value)


def make_cache_key(*parts):
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


def parse_steam_date(s):
    if not s:
        return 0
    s = str(s).strip()
    for fmt in ["%b %d, %Y", "%d %b, %Y", "%B %d, %Y", "%d %B, %Y", "%B %Y", "%b %Y"]:
        try:
            return int(datetime.strptime(s, fmt).timestamp())
        except Exception:
            pass
    m = re.search(r"Q([1-4])\s*(\d{4})", s)
    if m:
        q, y = int(m.group(1)), int(m.group(2))
        try:
            return int(datetime(y, (q - 1) * 3 + 1, 1).timestamp())
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


def owners_min(owners_str):
    if not owners_str:
        return 0
    m = re.search(r"([\d,]+)", str(owners_str))
    if m:
        return safe_int(m.group(1))
    return 0


def clean_html_text(s):
    if not s:
        return ""
    try:
        return BS(s, "lxml").get_text(" ", strip=True)
    except Exception:
        return str(s)


# ═══════════════════════════════════════════════════════════════
# GAMEVAULT
# ═══════════════════════════════════════════════════════════════
def normalize_for_match(s):
    if not s:
        return ""
    s = re.sub(r"[™®©]", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    stopwords = [
        "edition", "deluxe", "ultimate", "goty", "complete", "definitive",
        "remastered", "remake", "game of the year", "anniversary", "collectors",
        "directors cut", "enhanced", "vr", "the", "a", "an", "and",
    ]
    for w in stopwords:
        s = re.sub(rf"\b{re.escape(w)}\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def similarity(a, b):
    na = normalize_for_match(a)
    nb = normalize_for_match(b)
    if not na or not nb:
        return 0
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.85
    return difflib.SequenceMatcher(None, na, nb).ratio()


def make_search_queries(name):
    queries = [name.strip()]
    clean = re.sub(r"[™®©]", "", name).strip()
    if clean != queries[0]:
        queries.append(clean)
    parts = re.split(r"[:\-–—]", clean, 1)
    if parts[0].strip() and parts[0].strip() != clean:
        queries.append(parts[0].strip())
    stripped = re.sub(
        r"\s*(deluxe|ultimate|goty|complete|definitive|remastered|"
        r"game of the year|anniversary|collectors|enhanced|"
        r"directors cut|edition)\s*$",
        "", clean, flags=re.I,
    ).strip()
    if stripped and stripped != clean:
        queries.append(stripped)
    seen = set()
    out = []
    for q in queries:
        if len(q) < 3:
            continue
        k = q.lower()
        if k not in seen:
            seen.add(k)
            out.append(q)
    return out[:4]


async def gamevault_search(client, game_name, sem, max_variants=8):
    cache_key = make_cache_key("gv", game_name, max_variants)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    async with sem:
        queries = make_search_queries(game_name)
        best_result = None
        best_sim = 0.0

        for q in queries:
            try:
                r = await client.get(
                    f"{GAMEVAULT_API}/cercaTutto",
                    params={"q": q}, headers=HDR, timeout=25,
                )
                data = r.json() if r.status_code == 200 else None
            except Exception:
                data = None

            await asyncio.sleep(DELAY_DL)
            if not data or not data.get("risultati"):
                continue

            scored = []
            for r in data["risultati"]:
                title = r.get("titolo", "")
                ct = re.sub(r"\b(build|v|version|update)\s*[\d.]+.*$", "", title, flags=re.I).strip()
                ct = re.sub(r"\s+", " ", ct)
                scored.append((similarity(game_name, ct), r))

            scored.sort(key=lambda x: -x[0])

            if scored and scored[0][0] > best_sim:
                best_sim = scored[0][0]
                if scored[0][0] >= 0.55:
                    threshold = max(0.5, scored[0][0] - 0.15)
                    valid = [r for s, r in scored if s >= threshold]
                    confidence = (
                        "perfect" if scored[0][0] >= 0.95
                        else "high" if scored[0][0] >= 0.8
                        else "medium" if scored[0][0] >= 0.65
                        else "low"
                    )
                    variants = [
                        {
                            "title": r.get("titolo"),
                            "url": r.get("url"),
                            "cover": r.get("copertina"),
                            "download_links": r.get("links", [])[:20],
                            "num_links": len(r.get("links", [])),
                        }
                        for r in valid[:max_variants]
                    ]
                    best_result = {
                        "search_query": q,
                        "matched_similarity": round(scored[0][0], 3),
                        "match_confidence": confidence,
                        "total_found": len(valid),
                        "variants": variants,
                        "variants_shown": len(variants),
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
async def steamspy_all(client):
    if CACHE_ALL.exists():
        if time.time() - CACHE_ALL.stat().st_mtime < CACHE_ALL_TTL:
            try:
                return json.loads(CACHE_ALL.read_text(encoding="utf-8"))
            except Exception:
                pass
    log.info("Fetching SteamSpy catalog...")
    r = await client.get(STEAMSPY_API, params={"request": "all"}, headers=HDR, timeout=90)
    data = r.json()
    CACHE_ALL.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    log.info("SteamSpy cached: %d games", len(data))
    return data


async def resolve_appid(client, query):
    cache_key = make_cache_key("resolve", query)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    q = query.strip()

    # 1. Steam Store search API
    try:
        r = await client.get(
            STEAM_SEARCH,
            params={"term": q, "l": "english", "cc": "US"},
            headers=HDR, timeout=15,
        )
        if r.status_code == 200:
            items = r.json().get("items", [])
            if items:
                scored = [(similarity(q, it.get("name", "")), it) for it in items]
                scored.sort(key=lambda x: -x[0])
                if scored[0][0] >= 0.5:
                    result = (int(scored[0][1]["id"]), scored[0][1]["name"])
                    cache_set(cache_key, result)
                    return result
    except Exception as e:
        log.warning("Steam search failed: %s", e)

    # 2. SteamSpy catalog fallback
    try:
        data = await steamspy_all(client)
        ql = q.lower()
        exact, contains = [], []
        names = {}
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


async def steamspy_details(client, appid):
    cache_key = make_cache_key("spy", appid)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    for _ in range(RETRY_ATTEMPTS):
        try:
            r = await client.get(
                STEAMSPY_API,
                params={"request": "appdetails", "appid": appid},
                headers=HDR, timeout=TIMEOUT,
            )
            if r.status_code == 200:
                d = r.json()
                if d and d.get("appid"):
                    cache_set(cache_key, d)
                    return d
        except Exception:
            await asyncio.sleep(0.5)
    return None


async def steamspy_by_tag(client, tag_name):
    """Ottieni tutti i giochi con un tag specifico (SteamSpy)"""
    cache_key = make_cache_key("tag", tag_name)
    cached = cache_get(cache_key, ttl=CACHE_TAG_TTL)
    if cached is not None:
        return cached
    try:
        r = await client.get(
            STEAMSPY_API,
            params={"request": "tag", "tag": tag_name},
            headers=HDR, timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            cache_set(cache_key, data)
            return data
    except Exception as e:
        log.warning("steamspy_by_tag %s failed: %s", tag_name, e)
    return {}


async def steamspy_by_genre(client, genre_name):
    """Ottieni giochi per genere"""
    cache_key = make_cache_key("genre", genre_name)
    cached = cache_get(cache_key, ttl=CACHE_TAG_TTL)
    if cached is not None:
        return cached
    try:
        r = await client.get(
            STEAMSPY_API,
            params={"request": "genre", "genre": genre_name},
            headers=HDR, timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            cache_set(cache_key, data)
            return data
    except Exception as e:
        log.warning("steamspy_by_genre %s failed: %s", genre_name, e)
    return {}


async def steam_store_details(client, appid, lang="italian", cc="IT"):
    cache_key = make_cache_key("store", appid, lang, cc)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    for _ in range(RETRY_ATTEMPTS):
        try:
            r = await client.get(
                STEAM_APPDETAILS,
                params={"appids": appid, "l": lang, "cc": cc},
                headers=HDR, timeout=20,
            )
            if r.status_code == 200:
                entry = r.json().get(str(appid), {})
                if entry.get("success"):
                    data = entry.get("data", {})
                    cache_set(cache_key, data)
                    return data
                cache_set(cache_key, None)
                return None
        except Exception:
            await asyncio.sleep(0.5)
    return None


async def steam_reviews_summary(client, appid):
    cache_key = make_cache_key("rev", appid)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        r = await client.get(
            f"{STEAM_REVIEWS}/{appid}",
            params={"json": 1, "language": "all", "purchase_type": "all", "num_per_page": 0},
            headers=HDR, timeout=15,
        )
        if r.status_code == 200:
            summary = r.json().get("query_summary", {})
            cache_set(cache_key, summary)
            return summary
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════
# TROVA SIMILI VIA TAG/GENRE (SteamSpy) — QUESTA È LA MAGIA
# ═══════════════════════════════════════════════════════════════
async def find_similar_via_tags(
    http_client, seed_appid, seed_spy_data,
    max_results=100, min_tag_matches=2,
):
    """
    Trova giochi simili basandosi sui tag/genere del gioco seed.
    - Prende i TOP N tag del seed
    - Per ciascuno chiede a SteamSpy la lista completa
    - Aggrega, scorreggia per intersezione di tag comuni
    - Filtra per popolarità e restituisce i migliori
    """
    if not seed_spy_data:
        return []

    # 1. Estrai tag e genere del seed
    tags_raw = seed_spy_data.get("tags") or {}
    if not isinstance(tags_raw, dict) or not tags_raw:
        log.warning("Seed %d has no tags", seed_appid)
        return []

    # Top 8 tag per rilevanza (evita di scaricare mille categorie)
    top_tags = sorted(tags_raw.items(), key=lambda x: -x[1])[:8]
    tag_names = [t[0] for t in top_tags]

    log.info("Fetching similar via tags: %s", tag_names[:5])

    # 2. Scarica in parallelo i giochi per ogni tag
    sem = asyncio.Semaphore(CONCURRENCY_TAG)

    async def fetch_tag(tname):
        async with sem:
            return tname, await steamspy_by_tag(http_client, tname)

    tag_results = await asyncio.gather(
        *[fetch_tag(t) for t in tag_names],
        return_exceptions=True,
    )

    # 3. Conta quante volte ogni appid compare (= quanti tag condivide col seed)
    appid_matches = Counter()
    appid_data = {}  # appid → dict info base
    appid_seen_tags = {}  # appid → lista tag matchati

    for res in tag_results:
        if isinstance(res, Exception):
            continue
        tname, games_dict = res
        if not isinstance(games_dict, dict):
            continue
        for aid_str, ginfo in games_dict.items():
            try:
                aid = int(aid_str)
            except Exception:
                continue
            if aid == seed_appid:
                continue
            appid_matches[aid] += 1
            if aid not in appid_data:
                appid_data[aid] = ginfo
                appid_seen_tags[aid] = []
            appid_seen_tags[aid].append(tname)

    # 4. Filtra per almeno min_tag_matches tag comuni
    candidates = [
        (aid, count) for aid, count in appid_matches.items()
        if count >= min_tag_matches
    ]

    # 5. Score: mix di tag matches + popolarità (owners)
    def score(item):
        aid, count = item
        info = appid_data.get(aid, {})
        owners = owners_min(info.get("owners"))
        # normalizza owners (log scale)
        pop_score = 1
        if owners > 0:
            import math
            pop_score = math.log10(owners + 1)
        return (count * 2) + pop_score

    candidates.sort(key=score, reverse=True)
    candidates = candidates[:max_results]

    # 6. Costruisci lista giochi con metadata base
    similar = []
    for aid, count in candidates:
        info = appid_data.get(aid, {})
        similar.append({
            "appid": aid,
            "name": info.get("name") or f"#{aid}",
            "shared_tags_count": count,
            "shared_tags": appid_seen_tags.get(aid, [])[:8],
            "_spy_preview": info,  # riusiamo dopo se disponibile
        })

    log.info(
        "Similar via tags: %d candidates (min %d shared tags)",
        len(similar), min_tag_matches,
    )
    return similar


# ═══════════════════════════════════════════════════════════════
# BUILD RECORD
# ═══════════════════════════════════════════════════════════════
def build_full_record(appid, spy, store, rev_sum, dl_info, extra=None):
    spy = spy or {}
    store = store or {}
    rev_sum = rev_sum or {}
    extra = extra or {}
    appid = int(appid)

    name = store.get("name") or spy.get("name") or extra.get("name") or f"#{appid}"

    pos = safe_int(spy.get("positive"))
    neg = safe_int(spy.get("negative"))
    total_rev = pos + neg
    score = round((pos / total_rev) * 100, 2) if total_rev else None

    steam_total = safe_int(rev_sum.get("total_reviews"))
    steam_pos = safe_int(rev_sum.get("total_positive"))
    steam_score = round((steam_pos / steam_total) * 100, 2) if steam_total > 0 else None

    if store.get("is_free"):
        price_info = {"final": 0, "currency": "USD", "formatted": "FREE", "is_free": True}
    elif store.get("price_overview"):
        po = store["price_overview"]
        price_info = {
            "final": po.get("final", 0) / 100,
            "initial": po.get("initial", 0) / 100,
            "discount_percent": po.get("discount_percent", 0),
            "currency": po.get("currency"),
            "formatted": po.get("final_formatted"),
            "is_free": False,
        }
    else:
        final = money(spy.get("price"))
        price_info = {
            "final": final,
            "currency": "USD",
            "formatted": ("FREE" if final == 0 else f"${final:.2f}" if final is not None else None),
            "is_free": final == 0,
        }

    tags_raw = spy.get("tags") or {}
    tags = []
    if isinstance(tags_raw, dict):
        for k, v in sorted(tags_raw.items(), key=lambda x: -x[1])[:20]:
            tags.append({"name": k, "votes": v})

    store_genres = [g.get("description") for g in store.get("genres", []) if g.get("description")]
    spy_genres = [x.strip() for x in str(spy.get("genre") or "").split(",") if x.strip()]
    genres = list(dict.fromkeys(store_genres + spy_genres))

    store_cats = [c.get("description") for c in store.get("categories", []) if c.get("description")]

    rd = store.get("release_date", {}) or {}
    release_date = rd.get("date") or spy.get("release_date")

    screenshots = [
        {"id": s.get("id"), "thumbnail": s.get("path_thumbnail"), "full": s.get("path_full")}
        for s in (store.get("screenshots") or [])
    ]

    movies = []
    for m in (store.get("movies") or []):
        webm = m.get("webm") or {}
        mp4 = m.get("mp4") or {}
        movies.append({
            "id": m.get("id"),
            "name": m.get("name"),
            "thumbnail": m.get("thumbnail"),
            "webm_max": webm.get("max"),
            "mp4_max": mp4.get("max"),
        })

    platforms = store.get("platforms") or {}

    def parse_req(r):
        if not r or not isinstance(r, dict):
            return None
        return {
            "minimum": clean_html_text(r.get("minimum")),
            "recommended": clean_html_text(r.get("recommended")),
        }

    achievements = store.get("achievements") or {}

    return {
        "appid": appid,
        "name": name,
        "type": store.get("type", "game"),
        "required_age": store.get("required_age", 0),
        "short_description": store.get("short_description"),
        "detailed_description": clean_html_text(store.get("detailed_description")),
        "about_the_game": clean_html_text(store.get("about_the_game")),
        "supported_languages": clean_html_text(store.get("supported_languages")),
        "website": store.get("website"),
        "developers": store.get("developers") or ([spy.get("developer")] if spy.get("developer") else []),
        "publishers": store.get("publishers") or ([spy.get("publisher")] if spy.get("publisher") else []),
        "genres": genres,
        "categories": store_cats,
        "tags": tags,
        "release_date": release_date,
        "release_ts": parse_steam_date(release_date),
        "coming_soon": rd.get("coming_soon", False),
        "price": price_info,
        "reviews": {
            "steamspy": {
                "positive": pos, "negative": neg, "total": total_rev,
                "score_percent": score,
            },
            "steam": {
                "total_reviews": steam_total,
                "total_positive": steam_pos,
                "total_negative": safe_int(rev_sum.get("total_negative")),
                "score_percent": steam_score,
                "review_score_desc": rev_sum.get("review_score_desc"),
            },
        },
        "owners_estimate": spy.get("owners"),
        "owners_min": owners_min(spy.get("owners")),
        "ccu": safe_int(spy.get("ccu")),
        "playtime": {
            "average_forever_min": safe_int(spy.get("average_forever")),
            "average_2weeks_min": safe_int(spy.get("average_2weeks")),
            "median_forever_min": safe_int(spy.get("median_forever")),
        },
        "platforms": {
            "windows": platforms.get("windows", False),
            "mac": platforms.get("mac", False),
            "linux": platforms.get("linux", False),
        },
        "requirements": {
            "pc": parse_req(store.get("pc_requirements")),
            "mac": parse_req(store.get("mac_requirements")),
            "linux": parse_req(store.get("linux_requirements")),
        },
        "images": {
            "header": store.get("header_image") or f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg",
            "capsule_616x353": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_616x353.jpg",
            "library_600x900": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/library_600x900.jpg",
            "library_hero": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/library_hero.jpg",
            "logo": f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/logo.png",
            "background": store.get("background"),
        },
        "screenshots": screenshots,
        "movies": movies,
        "metacritic": store.get("metacritic"),
        "achievements_total": achievements.get("total", 0),
        "dlc_ids": store.get("dlc") or [],
        "dlc_count": len(store.get("dlc") or []),
        "links": {
            "steam_store": f"https://store.steampowered.com/app/{appid}/",
            "steampeek": f"https://steampeek.hu/?appid={appid}",
            "steamspy": f"https://steamspy.com/app/{appid}",
            "steamdb": f"https://steamdb.info/app/{appid}/",
            "protondb": f"https://www.protondb.com/app/{appid}",
        },
        "downloads": dl_info,
        "has_downloads": bool(dl_info),
        "download_variants_count": (dl_info or {}).get("total_found", 0) if dl_info else 0,
        "download_links_count": (dl_info or {}).get("total_download_links", 0) if dl_info else 0,
        "shared_tags_count": extra.get("shared_tags_count"),
        "shared_tags": extra.get("shared_tags"),
    }


# ═══════════════════════════════════════════════════════════════
# ENRICH
# ═══════════════════════════════════════════════════════════════
async def enrich_single_game(
    http_client, appid, name_hint=None,
    include_downloads=True, dl_max=8,
    lang=DEFAULT_LANG, cc=DEFAULT_CC, extra=None,
):
    sem_dl = asyncio.Semaphore(CONCURRENCY_DL)
    spy = await steamspy_details(http_client, appid)
    clean_name = (spy or {}).get("name") or name_hint or ""

    tasks = [
        steam_store_details(http_client, appid, lang, cc),
        steam_reviews_summary(http_client, appid),
    ]
    if include_downloads and clean_name and not clean_name.startswith("#"):
        tasks.append(gamevault_search(http_client, clean_name, sem_dl, dl_max))
    else:
        tasks.append(asyncio.sleep(0, result=None))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    store = results[0] if not isinstance(results[0], Exception) else None
    rev = results[1] if not isinstance(results[1], Exception) else None
    dl = results[2] if not isinstance(results[2], Exception) else None

    return build_full_record(appid, spy, store, rev, dl, extra or {"name": name_hint})


async def _w_store(client, appid, lang, cc, sem):
    async with sem:
        await asyncio.sleep(DELAY_STORE)
        return await steam_store_details(client, appid, lang, cc)


async def _w_rev(client, appid, sem):
    async with sem:
        return await steam_reviews_summary(client, appid)


async def enrich_batch(
    http_client, games,
    include_downloads=True, dl_max=5,
    lang=DEFAULT_LANG, cc=DEFAULT_CC,
    full_details=False,
):
    sem_spy = asyncio.Semaphore(CONCURRENCY_SPY)
    sem_store = asyncio.Semaphore(CONCURRENCY_STORE)
    sem_rev = asyncio.Semaphore(CONCURRENCY_REVIEWS)
    sem_dl = asyncio.Semaphore(CONCURRENCY_DL)

    results = []
    done = [0]
    total = len(games)

    async def worker(g):
        async with sem_spy:
            spy = await steamspy_details(http_client, g["appid"])
            await asyncio.sleep(DELAY_SPY)

        clean_name = (spy or {}).get("name") or g.get("name", "")

        tasks = []
        if full_details:
            tasks.append(_w_store(http_client, g["appid"], lang, cc, sem_store))
            tasks.append(_w_rev(http_client, g["appid"], sem_rev))
        else:
            tasks.append(asyncio.sleep(0, result=None))
            tasks.append(asyncio.sleep(0, result=None))

        if include_downloads and clean_name and not clean_name.startswith("#"):
            tasks.append(gamevault_search(http_client, clean_name, sem_dl, dl_max))
        else:
            tasks.append(asyncio.sleep(0, result=None))

        outs = await asyncio.gather(*tasks, return_exceptions=True)
        store = outs[0] if not isinstance(outs[0], Exception) else None
        rev = outs[1] if not isinstance(outs[1], Exception) else None
        dl = outs[2] if not isinstance(outs[2], Exception) else None

        done[0] += 1
        if done[0] % 25 == 0 or done[0] == total:
            log.info("Enriched %d/%d", done[0], total)

        # extra: mantieni shared_tags info
        extra = {
            "name": g.get("name"),
            "shared_tags_count": g.get("shared_tags_count"),
            "shared_tags": g.get("shared_tags"),
        }
        return build_full_record(g["appid"], spy, store, rev, dl, extra)

    tasks = [worker(g) for g in games]
    for coro in asyncio.as_completed(tasks):
        try:
            results.append(await coro)
        except Exception as e:
            log.warning("Worker error: %s", e)

    return results


# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════
async def prewarm():
    try:
        async with httpx.AsyncClient(timeout=90, headers=HDR) as c:
            await steamspy_all(c)
    except Exception as e:
        log.warning("Prewarm failed: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 SteamPeek API v7 starting")
    asyncio.create_task(prewarm())
    yield
    log.info("👋 Shutting down")


app = FastAPI(
    title="SteamPeek API v7",
    version="7.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "service": "SteamPeek API v7",
        "version": "7.0.0",
        "engine": "SteamSpy tag-based similarity",
        "endpoints": {
            "cerca": "GET /cerca?q=NOME",
            "simili": "GET /simili?q=NOME&max=100",
            "resolve": "GET /resolve?q=NOME",
        },
        "health": "/health",
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "7.0.0",
        "cache_entries": len(_memory_cache),
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/resolve")
async def endpoint_resolve(q: str = Query(...)):
    async with httpx.AsyncClient(timeout=60, headers=HDR) as c:
        appid, name = await resolve_appid(c, q)
    if not appid:
        raise HTTPException(404, detail=f"Non trovato: '{q}'")
    return {
        "query": q, "appid": appid, "name": name,
        "steam_url": f"https://store.steampowered.com/app/{appid}/",
    }


@app.get("/cerca")
async def endpoint_cerca(
    q: str = Query(...),
    lang: str = Query(DEFAULT_LANG),
    cc: str = Query(DEFAULT_CC),
    downloads: bool = Query(True),
    dl_max: int = Query(10, ge=1, le=30),
):
    """Info complete di UN singolo gioco."""
    t0 = time.time()
    try:
        appid = int(q.strip())
        name_hint = None
    except ValueError:
        async with httpx.AsyncClient(timeout=60, headers=HDR) as c:
            appid, name_hint = await resolve_appid(c, q)
        if not appid:
            raise HTTPException(404, detail=f"Non trovato: '{q}'")

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HDR, follow_redirects=True) as c:
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


@app.get("/simili")
async def endpoint_simili(
    q: str = Query(..., description="Nome gioco o AppID"),
    max_total: Optional[int] = Query(100, ge=1, le=500, alias="max"),
    min_shared_tags: int = Query(2, ge=1, le=10, description="Min tag in comune col seed"),
    lang: str = Query(DEFAULT_LANG),
    cc: str = Query(DEFAULT_CC),
    downloads: bool = Query(True),
    dl_max: int = Query(5, ge=1, le=20),
    full: bool = Query(False, description="Fetch Steam Store per ogni simile (più lento)"),
    sort: str = Query("shared_tags", description="Sort: shared_tags|name|date|score|reviews|price|downloads|owners"),
    desc: bool = Query(True),
    min_score: float = Query(0, ge=0, le=100),
    min_reviews: int = Query(0, ge=0),
    free_only: bool = Query(False),
    paid_only: bool = Query(False),
    dl_only: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1),
):
    """
    Gioco + simili trovati via TAG matching su SteamSpy.
    Molto più affidabile del scraping SteamPeek.
    """
    t0 = time.time()

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HDR, follow_redirects=True) as c:
            # Resolve
            try:
                appid = int(q.strip())
                seed_name = q
            except ValueError:
                appid, seed_name = await resolve_appid(c, q)
                if not appid:
                    raise HTTPException(404, detail=f"Non trovato: '{q}'")

            log.info("Seed: %s appid=%d", seed_name, appid)

            # Fetch seed data (per avere i tag)
            seed_spy = await steamspy_details(c, appid)
            if not seed_spy:
                raise HTTPException(404, detail=f"SteamSpy data non disponibile per {appid}")

            # Trova simili via tag matching
            similar_raw = await find_similar_via_tags(
                c, appid, seed_spy,
                max_results=max_total or 100,
                min_tag_matches=min_shared_tags,
            )
            log.info("Found %d similar candidates", len(similar_raw))

            # Enrichment: seed completo + simili in parallelo
            seed_dl_max = dl_max if dl_max >= 10 else 10
            seed_task = enrich_single_game(
                c, appid,
                name_hint=seed_name,
                include_downloads=downloads,
                dl_max=seed_dl_max,
                lang=lang, cc=cc,
                extra={"name": seed_name},
            )
            sim_task = enrich_batch(
                c, similar_raw,
                include_downloads=downloads,
                dl_max=dl_max,
                lang=lang, cc=cc,
                full_details=full,
            )
            seed_rec, sim_recs = await asyncio.gather(seed_task, sim_task)

        # Filtri
        def filter_ok(g):
            r = ((g.get("reviews") or {}).get("steamspy") or {})
            if min_reviews and (r.get("total") or 0) < min_reviews:
                return False
            if min_score and (r.get("score_percent") or 0) < min_score:
                return False
            pr = g.get("price") or {}
            if free_only and not pr.get("is_free"):
                return False
            if paid_only and pr.get("is_free"):
                return False
            if dl_only and not g.get("has_downloads"):
                return False
            return True

        filtered = [g for g in sim_recs if filter_ok(g)]

        SK = {
            "shared_tags": lambda x: x.get("shared_tags_count") or 0,
            "name": lambda x: (x.get("name") or "").lower(),
            "date": lambda x: x.get("release_ts") or 0,
            "score": lambda x: (((x.get("reviews") or {}).get("steamspy") or {}).get("score_percent") or 0),
            "reviews": lambda x: (((x.get("reviews") or {}).get("steamspy") or {}).get("total") or 0),
            "price": lambda x: (x.get("price") or {}).get("final") or 0,
            "downloads": lambda x: (x.get("downloads") or {}).get("total_found") or 0,
            "owners": lambda x: x.get("owners_min") or 0,
        }
        filtered.sort(key=SK.get(sort, SK["shared_tags"]), reverse=desc)

        if limit:
            filtered = filtered[:limit]

        with_dl = sum(1 for r in filtered if r.get("has_downloads"))

        return {
            "ok": True,
            "query": q,
            "resolved_appid": appid,
            "seed": seed_rec,
            "similar": filtered,
            "stats": {
                "engine": "steamspy_tag_matching",
                "total_discovered": len(similar_raw),
                "total_after_filter": len(filtered),
                "with_downloads": with_dl,
                "download_rate_pct": round(with_dl * 100 / max(len(filtered), 1), 1),
                "elapsed_seconds": round(time.time() - t0, 2),
                "params": {
                    "max": max_total,
                    "min_shared_tags": min_shared_tags,
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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
