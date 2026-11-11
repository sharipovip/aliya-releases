#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Этап 12 — Цепочка свежих новостей каждые 15 минут (`pipeline/12_collect_15min_news_chain.py`).

Предназначен для запуска в публичном репозитории `sharipovip/aliya-releases` (`cron: '*/15 * * * *'` — 0 платных минут!):
  1) Опрашивает достоверные сайты Таджикистана (Президент РТ, АМИТ «Ховар», «Ҷумҳурият», «Садои мардум»,
     Asia-Plus, Avesta.tj, Pressa.tj, Faraj.tj, Sputnik Тоҷикистон, Your.tj, Радиои Озодӣ, Varzish-Sport,
     Maorif.tj, ММТ / НТЦ, БМТ / НБТ), России (ТАСС, РИА Новости) и мира (BBC, ООН).
  2) Отсекает дубли по SHA-256 ссылки/заголовка: если за эти 15 минут новых материалов не вышло —
     НЕ создаёт пустых файлов и лишних коммитов!
  3) Каждую порцию новых материалов сохраняет СТРОГО ПО ЦЕПОЧКАМ:
       `chains/news/YYYY-MM-DD/link_000001.json.gz`
     с полями `seq`, `prev_seq`, `created_at` и чистым литературным текстом + 128-мерным `int8` вектором.
  4) Автоматически сворачивает звенья прошлых суток в компактный суточный пакет `chains/news/daily/day_YYYY-MM-DD.json.gz`,
     чтобы цепочка всегда оставалась быстрой и аккуратной.
"""

import argparse
import datetime as dt
import gzip
import hashlib
import html
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup

from aliya_vector_core import clean_literary_text, embed128_b64, extract_stems

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AliyaNewsChain/3.2; +https://github.com/sharipovip)"
}

NEWS_SOURCES: List[Dict[str, Any]] = [
    # Официальные, новостные и образовательные порталы Таджикистана
    {"region": "tajikistan", "lang": "tg", "source": "Президенти ҶТ (president.tj)", "type": "html", "url": "https://president.tj/", "limit": 8},
    {"region": "tajikistan", "lang": "ru", "source": "Президент РТ (president.tj/ru)", "type": "html", "url": "https://president.tj/ru", "limit": 6},
    {"region": "tajikistan", "lang": "tg", "source": "АМИТ «Ховар»", "type": "rss", "url": "https://khovar.tj/feed/", "limit": 10},
    {"region": "tajikistan", "lang": "ru", "source": "НИАТ «Ховар»", "type": "rss", "url": "https://khovar.tj/rus/feed/", "limit": 8},
    {"region": "tajikistan", "lang": "tg", "source": "Нашрияи расмии «Ҷумҳурият»", "type": "rss", "url": "https://jumhuriyat.tj/rss.xml", "fallback_html": "https://jumhuriyat.tj/", "limit": 8},
    {"region": "tajikistan", "lang": "tg", "source": "«Садои мардум»", "type": "rss", "url": "https://sadoimardum.tj/feed/", "limit": 8},
    {"region": "tajikistan", "lang": "tg", "source": "Pressa.tj", "type": "rss", "url": "https://pressa.tj/feed/", "limit": 8},
    {"region": "tajikistan", "lang": "tg", "source": "Faraj.tj", "type": "rss", "url": "https://faraj.tj/feed/", "limit": 6},
    {"region": "tajikistan", "lang": "ru", "source": "Asia-Plus", "type": "rss", "url": "https://asiaplus.tj/ru/rss", "limit": 8},
    {"region": "tajikistan", "lang": "ru", "source": "Avesta.tj", "type": "rss", "url": "https://avesta.tj/feed/", "limit": 8},
    {"region": "tajikistan", "lang": "tg", "source": "Sputnik Тоҷикистон", "type": "rss", "url": "https://sputnik.tj/export/rss2/archive/index.xml", "limit": 8},
    {"region": "tajikistan", "lang": "ru", "source": "Your.tj", "type": "rss", "url": "https://your.tj/feed/", "limit": 6},
    {"region": "tajikistan", "lang": "tg", "source": "Радиои Озодӣ", "type": "rss", "url": "https://www.ozodi.org/api/epiqq", "limit": 6},
    {"region": "tajikistan", "lang": "tg", "source": "Varzish-Sport.tj", "type": "html", "url": "https://varzish-sport.tj/", "limit": 5},
    {"region": "tajikistan", "lang": "tg", "source": "Вазорати маориф ва илм (maorif.tj)", "type": "html", "url": "https://maorif.tj/", "limit": 5},
    {"region": "tajikistan", "lang": "tg", "source": "Маркази миллии тестӣ (mmt.tj)", "type": "html", "url": "https://mmt.tj/", "limit": 5},
    # Россия и мир
    {"region": "russia", "lang": "ru", "source": "ТАСС", "type": "rss", "url": "https://tass.ru/rss/v2.xml", "limit": 8},
    {"region": "russia", "lang": "ru", "source": "РИА Новости", "type": "rss", "url": "https://ria.ru/export/rss2/archive/index.xml", "limit": 8},
    {"region": "world", "lang": "ru", "source": "BBC Русская служба", "type": "rss", "url": "https://feeds.bbci.co.uk/russian/rss.xml", "limit": 6},
    {"region": "world", "lang": "ru", "source": "Новости ООН", "type": "rss", "url": "https://news.un.org/feed/subscribe/ru/news/all/rss.xml", "limit": 6},
]


def _item_uid(title: str, link: str) -> str:
    norm = re.sub(r"\s+", " ", (title or "").strip().lower())
    base = link.strip() if link and len(link) > 12 else norm
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def _parse_rss_feed(xml_text: str, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    root = ET.fromstring(xml_text)
    for node in root.findall(".//item")[: int(meta.get("limit", 8))]:
        raw_t = (node.findtext("title") or "").strip()
        raw_d = (node.findtext("description") or "").strip()
        link = (node.findtext("link") or "").strip()
        pub = (node.findtext("pubDate") or "").strip()
        clean_t = clean_literary_text(raw_t, max_len=240)
        clean_d = clean_literary_text(raw_d, max_len=520) if raw_d else ""
        if len(clean_t) < 16:
            continue
        body = f"{clean_t}. {clean_d}".strip(". ") if clean_d and clean_d != clean_t else clean_t
        items.append({
            "uid": _item_uid(clean_t, link),
            "t": clean_t,
            "s": clean_d[:360],
            "src": meta["source"],
            "r": meta["region"],
            "l": meta["lang"],
            "pub": pub[:32],
            "stems": " ".join(extract_stems(body, max_stems=14)),
            "v": embed128_b64(body),
        })
    return items


def _parse_html_headlines(html_text: str, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    seen_titles: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for a_tag in soup.find_all("a", href=True):
        txt = re.sub(r"\s+", " ", a_tag.get_text(" ", strip=True)).strip()
        if len(txt) < 28 or len(txt) > 220:
            continue
        clean_t = clean_literary_text(txt, max_len=220)
        if len(clean_t) < 26 or clean_t.lower() in seen_titles:
            continue
        seen_titles.add(clean_t.lower())
        href = str(a_tag["href"]).strip()
        out.append({
            "uid": _item_uid(clean_t, href),
            "t": clean_t,
            "s": "",
            "src": meta["source"],
            "r": meta["region"],
            "l": meta["lang"],
            "pub": "",
            "stems": " ".join(extract_stems(clean_t, max_stems=12)),
            "v": embed128_b64(clean_t),
        })
        if len(out) >= int(meta.get("limit", 6)):
            break
    return out


def fetch_source(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        r = requests.get(meta["url"], headers=UA_HEADERS, timeout=11)
        if r.status_code == 200 and r.text:
            if meta["type"] == "rss":
                try:
                    return _parse_rss_feed(r.text, meta)
                except Exception:
                    pass
            return _parse_html_headlines(r.text, meta)
    except Exception:
        pass
    fb = meta.get("fallback_html")
    if fb:
        try:
            r2 = requests.get(fb, headers=UA_HEADERS, timeout=11)
            if r2.status_code == 200 and r2.text:
                return _parse_html_headlines(r2.text, meta)
        except Exception:
            pass
    return []


def compact_old_days(chain_root: Path, today_str: str, manifest: Dict[str, Any]) -> None:
    """Склеивает вчерашние 15-минутные звенья в один дневной архив `daily/day_YYYY-MM-DD.json.gz`."""
    daily_dir = chain_root / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    daily_list: List[Dict[str, Any]] = manifest.get("daily_rollups", [])
    known_days = {d["day"] for d in daily_list}

    for sub in sorted(chain_root.iterdir()):
        if not sub.is_dir() or sub.name == "daily" or sub.name >= today_str:
            continue
        day_name = sub.name
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_name):
            continue
        merged_articles: List[Dict[str, Any]] = []
        seen_uids: Set[str] = set()
        for gz_f in sorted(sub.glob("link_*.json.gz")):
            try:
                obj = json.loads(gzip.decompress(gz_f.read_bytes()).decode("utf-8"))
                for art in obj.get("items", []):
                    u = art.get("uid", "")
                    if u and u not in seen_uids:
                        seen_uids.add(u)
                        merged_articles.append(art)
            except Exception:
                continue
        if merged_articles and day_name not in known_days:
            out_day = daily_dir / f"day_{day_name}.json.gz"
            payload = gzip.compress(
                json.dumps({"day": day_name, "count": len(merged_articles), "items": merged_articles}, ensure_ascii=False).encode("utf-8"),
                compresslevel=9,
            )
            out_day.write_bytes(payload)
            daily_list.append({"day": day_name, "file": f"daily/day_{day_name}.json.gz", "count": len(merged_articles), "size_bytes": len(payload)})
        # Удаляем мелкие вчерашние звенья после склейки
        for gz_f in sub.glob("link_*.json.gz"):
            gz_f.unlink(missing_ok=True)
        try:
            sub.rmdir()
        except OSError:
            pass

    manifest["daily_rollups"] = daily_list[-30:]
    manifest["recent_links"] = [lnk for lnk in manifest.get("recent_links", []) if lnk.get("day") == today_str]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain-dir", default="releases-repo/chains/news", help="Корневая папка цепочки новостей")
    args = ap.parse_args()

    chain_root = Path(args.chain_dir)
    chain_root.mkdir(parents=True, exist_ok=True)
    manifest_path = chain_root / "manifest.json"

    manifest: Dict[str, Any] = {
        "format": "aliya-news-chain-v1",
        "latest_seq": 0,
        "updated_at": "",
        "recent_links": [],
        "daily_rollups": [],
        "seen_uids": [],
    }
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    dushanbe_now = dt.datetime.now(dt.timezone(dt.timedelta(hours=5)))
    today_str = dushanbe_now.strftime("%Y-%m-%d")
    stamp_str = dushanbe_now.strftime("%Y-%m-%d %H:%M")

    compact_old_days(chain_root, today_str, manifest)

    seen_uids: List[str] = list(manifest.get("seen_uids", []))
    seen_set: Set[str] = set(seen_uids)

    fresh_items: List[Dict[str, Any]] = []
    for src_meta in NEWS_SOURCES:
        for item in fetch_source(src_meta):
            uid = item["uid"]
            if uid not in seen_set:
                seen_set.add(uid)
                seen_uids.append(uid)
                fresh_items.append(item)

    if not fresh_items:
        print("ℹ️ Новых новостей за последние 15 минут нет — новое звено не требуется.")
        return

    prev_seq = int(manifest.get("latest_seq", 0))
    next_seq = prev_seq + 1
    day_dir = chain_root / today_str
    day_dir.mkdir(parents=True, exist_ok=True)
    rel_file = f"{today_str}/link_{next_seq:06d}.json.gz"
    link_path = chain_root / rel_file

    link_obj = {
        "seq": next_seq,
        "prev_seq": prev_seq,
        "dushanbe_time": stamp_str,
        "count": len(fresh_items),
        "items": fresh_items,
    }
    compressed = gzip.compress(json.dumps(link_obj, ensure_ascii=False).encode("utf-8"), compresslevel=9)
    link_path.write_bytes(compressed)

    recent_links: List[Dict[str, Any]] = list(manifest.get("recent_links", []))
    recent_links.append({
        "seq": next_seq,
        "prev_seq": prev_seq,
        "day": today_str,
        "dushanbe_time": stamp_str,
        "file": rel_file,
        "count": len(fresh_items),
        "size_bytes": len(compressed),
    })

    manifest["latest_seq"] = next_seq
    manifest["updated_at"] = stamp_str
    manifest["recent_links"] = recent_links[-96:]
    manifest["seen_uids"] = seen_uids[-4500:]
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Звено #{next_seq:06d} записано ({len(fresh_items)} свежих новостей, {len(compressed):,} байт) -> {rel_file}")


if __name__ == "__main__":
    main()
