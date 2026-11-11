#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Этап 11 — Потоковый конвейер микро-векторов Алии (`pipeline/11_build_micro_vectors.py`).

Читает библиотеку `sharipovip/books` (15 700+ книг, 60 рубрик, `Kitobhoi darsi 1..11`, `Шеърҳо`, `president/`)
и оцифрованные учебники, автоматически включая OCR `Tesseract (tgk+rus)` для сканированных PDF,
вычищает текст до идеальной литературной нормы и раскладывает знания по СОТНЯМ лёгких микро-векторов `.avdb`:
  • Персональные микро-векторы крупных поэтов и классиков (`poet_rudaki`, `poet_firdavsi`, `poet_sino`,
    `poet_khayyam`, `poet_saadi`, `poet_hofiz`, `poet_kamol`, `poet_jomi`, `poet_bedil`, `poet_ayni`,
    `poet_tursunzoda`, `poet_loiq`, `poet_bozor_sobir`, `poet_gulrukhsor`, `poet_farzona` и др.);
  • Тематические поэтические векторы (`shero_modar` — стихи про Маму, `shero_vatan_tojikiston` — про Родину
    и Таджикистан, `shero_navruz_bahor`, `shero_ishq`, `shero_pandu_akhloq`, `chiston_zarbulmasal`);
  • Отдельный микро-вектор на каждый школьный предмет и класс 1–11 (`darsi_zaboni_modari_1` .. `darsi_biologiya_11`);
  • Архив Лидера нации (`president_speeches`, `president_books`, `president_documents`, `president_trips` и др.);
  • Все 60 рубрик и подрубрик библиотеки `sharipovip/books` + растущие пакеты вопросов-ответов `qa_pack_001..N`.

Поддерживает закладку прогресса `books_cursor.json` и ограничение времени `--max-runtime-minutes`,
чтобы GitHub Actions спокойно работал порциями по расписанию до 100% охвата всех 36.4 ГБ книг!
"""

import argparse
import gzip
import io
import json
import os
import re
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from aliya_vector_core import (
    clean_literary_text,
    extract_stems,
    fold_for_match,
    write_avdb_pack,
)

BOOKS_RAW_BASE = "https://raw.githubusercontent.com/sharipovip/books/main"
SEARCH_INDEX_URL = f"{BOOKS_RAW_BASE}/search-index.json"
PRESIDENT_INDEX_URL = f"{BOOKS_RAW_BASE}/president/index.json"
KITOBHO_ZIP_URL = "https://raw.githubusercontent.com/sharipovip/savolu-api/main/public/kitobho.zip"

POET_PROFILES: List[Tuple[str, str, str, Tuple[str, ...]]] = [
    ("poet_rudaki", "Абуабдуллоҳи Рӯдакӣ — Ашъор ва мерос", "Абуабдулло Рудаки — Стихи и наследие", ("рудаки", "рӯдакӣ", "rudaki", "одамушшуаро", "мулиен")),
    ("poet_firdavsi", "Абулқосим Фирдавсӣ ва «Шоҳнома»", "Абулькасим Фирдоуси и «Шахнаме»", ("фирдавс", "шохнома", "шоҳнома", "firdavs", "shohnoma")),
    ("poet_sino", "Абуалӣ ибни Сино — Осори илмӣ ва адабӣ", "Абуали ибн Сино — Труды и рубои", ("сино", "авицен", "sino", "avicenna", "конуни тиб")),
    ("poet_khayyam", "Умари Хайём — Рубоиёт", "Омар Хайям — Рубаи", ("хайём", "хайем", "хайям", "khayyam", "ruboiyot", "рубоиет")),
    ("poet_nosiri_khusrav", "Носири Хусрав — Сафарнома ва ашъор", "Насир Хусрав — Сафарнаме и стихи", ("носири хусрав", "хусрав", "сафарнома")),
    ("poet_saadi", "Саъдии Шерозӣ — «Гулистон» ва «Бӯстон»", "Саади Ширази — «Гулистан» и «Бустан»", ("саъд", "саади", "гулистон", "бустон", "бӯстон")),
    ("poet_hofiz", "Ҳофизи Шерозӣ — Ғазалиёт", "Хафиз Ширази — Газели", ("хофиз", "ҳофиз", "хафиз", "hofiz", "gazaliyot")),
    ("poet_mavlavii_rumi", "Ҷалолуддини Балхӣ (Румӣ) — Маснавӣ", "Джалаладдин Балхи (Руми) — Маснави", ("балхи", "балхӣ", "руми", "румӣ", "маснави", "маснавӣ")),
    ("poet_kamol", "Камоли Хуҷандӣ — Ғазалиёт", "Камол Худжанди — Газели", ("камоли хучанд", "камоли хуҷанд", "kamol")),
    ("poet_jomi", "Абдураҳмони Ҷомӣ — Баҳористон ва ашъор", "Абдуррахман Джами — Бахаристан и поэзия", ("чоми", "ҷомӣ", "джами", "бахористон", "баҳористон")),
    ("poet_bedil", "Мирзо Абдулқодири Бедил", "Мирзо Абдулкадир Бедиль", ("бедил", "bedil")),
    ("poet_sayido", "Сайидои Насафӣ", "Сайидо Насафи", ("сайидо", "насафи", "насафӣ")),
    ("poet_zebunniso", "Зебуннисо — Ғазалҳо", "Зебуннисо — Стихи и газели", ("зебуннисо", "zebunniso")),
    ("poet_donish", "Аҳмади Дониш", "Ахмад Дониш", ("ахмади дониш", "аҳмади дониш", "наводир")),
    ("poet_ayni", "Садриддин Айнӣ — Осор ва «Ёддоштҳо»", "Садриддин Айни — Произведения и стихи", ("айни", "айнӣ", "ayni", "еддоштхо", "ёддоштҳо", "дохунда", "марги судхур")),
    ("poet_lohuti", "Абулқосим Лоҳутӣ", "Абулькасим Лахути", ("лохути", "лоҳутӣ", "lohuti")),
    ("poet_tursunzoda", "Мирзо Турсунзода — Шеърҳо ва достонҳо", "Мирзо Турсунзаде — Стихи и поэмы", ("турсунзода", "tursunzoda", "хасани аробакаш", "чони ширин")),
    ("poet_loiq", "Лоиқ Шералӣ — «Модарнома» ва ғазалиёт", "Лоик Шерали — «Модарнома» и лирика", ("лоик", "лоиқ", "loiq", "модарнома")),
    ("poet_bozor_sobir", "Бозор Собир — Ашъори мунтахаб", "Бозор Собир — Избранные стихи", ("бозор собир", "bozor sobir", "гулихор", "симхор")),
    ("poet_gulrukhsor", "Гулрухсор Сафиева — Ашъор", "Гулрухсор Сафиева — Поэзия", ("гулрухсор", "gulrukhsor")),
    ("poet_farzona", "Фарзона — «Ояти ишқ» ва шеърҳо", "Фарзона — Стихи и лирика", ("фарзона", "farzona", "ояти ишк", "oyati ishq")),
    ("poet_gafurov", "Бобоҷон Ғафуров — «Тоҷикон»", "Бободжан Гафуров — «Таджики»", ("гафуров", "ғафуров", "точикон", "тоҷикон")),
]

THEMATIC_POETRY_PACKS = {
    "shero_modar": ("Шеърҳо дар бораи Модар", "Стихи про Маму (Модар)", ["модар", "мама", "мать", "меҳри модар", "модарнома", "дасти дуои модар"]),
    "shero_vatan_tojikiston": ("Шеърҳо дар бораи Ватан ва Тоҷикистон", "Стихи о Родине и Таджикистане", ["ватан", "тоҷикистон", "таджикистан", "родина", "меҳан", "забони модарӣ", "истиқлол"]),
    "shero_navruz_bahor": ("Шеърҳо дар бораи Баҳор ва Наврӯз", "Стихи о Весне и Наврузе", ["наврӯз", "навруз", "баҳор", "бахор", "весна", "гул", "суманак"]),
    "shero_ishq": ("Шеърҳои ишқӣ ва ғазалиёт", "Стихи о любви и лирика", ["ишқ", "ишк", "муҳаббат", "любовь", "дил", "ёр", "ошиқ"]),
    "shero_pandu_akhloq": ("Рубоиёт, панд ва ҳикмат", "Рубаи, мудрость и наставления", ["панд", "ахлоқ", "хирад", "дониш", "рубоӣ", "рубаи", "ҳикмат", "некӣ"]),
    "chiston_zarbulmasal": ("Чистонҳо ва зарбулмасалҳои тоҷикӣ", "Таджикские загадки и пословицы", ["чистон", "загадка", "зарбулмасал", "мақол", "пословица"]),
}


def slugify_id(raw: str) -> str:
    tr = {
        "а": "a", "б": "b", "в": "v", "г": "g", "ғ": "gh", "д": "d", "е": "e", "ё": "yo",
        "ж": "zh", "з": "z", "и": "i", "ӣ": "i", "й": "y", "к": "k", "қ": "q", "л": "l",
        "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
        "ӯ": "u", "ф": "f", "х": "kh", "ҳ": "h", "ч": "ch", "ҷ": "j", "ш": "sh", "щ": "sh",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    }
    folded = (raw or "").lower()
    out = "".join(tr.get(c, c if ("a" <= c <= "z" or c.isdigit()) else "_") for c in folded)
    out = re.sub(r"_+", "_", out).strip("_")
    return out[:44] or "misc"


def classify_school_pack_id(filename: str, grade: int) -> Tuple[str, str, str, List[str]]:
    low = fold_for_match(filename)
    g = max(1, min(11, grade))
    if "алифбо" in low or "alifbo" in low or "буквар" in low:
        return f"darsi_alifbo_{g}", f"Алифбо (синфи {g})", f"Алифбо / Букварь ({g} класс)", ["алифбо", "букварь", f"синфи {g}", f"{g} класс"]
    if "забони модар" in low or "zaboni modar" in low:
        return f"darsi_zaboni_modari_{g}", f"Забони модарӣ (синфи {g})", f"Родной язык ({g} класс)", ["забони модари", "родной язык", f"синфи {g}", f"{g} класс"]
    if "забони точик" in low or "zaboni tojik" in low:
        return f"darsi_zaboni_tojiki_{g}", f"Забони тоҷикӣ (синфи {g})", f"Таджикский язык ({g} класс)", ["забони точики", "таджикский язык", "грамматика", f"синфи {g}", f"{g} класс"]
    if "адабиет" in low or "adabiyot" in low:
        return f"darsi_adabiyot_{g}", f"Адабиёти тоҷик (синфи {g})", f"Таджикская литература ({g} класс)", ["адабиет", "литература", f"синфи {g}", f"{g} класс"]
    if "алгебр" in low or "algebr" in low:
        return f"darsi_algebra_{g}", f"Алгебра (синфи {g})", f"Алгебра ({g} класс)", ["алгебра", "математика", "муодила", f"синфи {g}", f"{g} класс"]
    if "геометр" in low or "geometr" in low:
        return f"darsi_geometriya_{g}", f"Геометрия (синфи {g})", f"Геометрия ({g} класс)", ["геометрия", "шакл", "теорема", f"синфи {g}", f"{g} класс"]
    if "математ" in low or "matemat" in low:
        return f"darsi_matematika_{g}", f"Математика (синфи {g})", f"Математика ({g} класс)", ["математика", "риёзи", "хисоб", f"синфи {g}", f"{g} класс"]
    if "биолог" in low or "biolog" in low or "ботаник" in low or "зоолог" in low:
        return f"darsi_biologiya_{g}", f"Биология (синфи {g})", f"Биология ({g} класс)", ["биология", "хучайра", "растани", "хайвонот", f"синфи {g}", f"{g} класс"]
    if "физик" in low or "fizik" in low or "астроном" in low:
        return f"darsi_fizika_{g}", f"Физика (синфи {g})", f"Физика ({g} класс)", ["физика", "кувва", "барк", f"синфи {g}", f"{g} класс"]
    if "хими" in low or "ximi" in low:
        return f"darsi_ximiya_{g}", f"Химия (синфи {g})", f"Химия ({g} класс)", ["химия", "модда", "реаксия", f"синфи {g}", f"{g} класс"]
    if "иттилоот" in low or "информатик" in low or "компютер" in low or "informatik" in low:
        return f"darsi_it_{g}", f"Технологияи иттилоотӣ (синфи {g})", f"Информатика ({g} класс)", ["технологияи иттилооти", "информатика", "компютер", "компьютер", f"синфи {g}", f"{g} класс"]
    if "таърихи умум" in low or "тарихи умум" in low:
        return f"darsi_tarixi_umumi_{g}", f"Таърихи умумӣ (синфи {g})", f"Всемирная история ({g} класс)", ["таърихи умуми", "всемирная история", f"синфи {g}", f"{g} класс"]
    if "таърих" in low or "тарих" in low or "tarix" in low:
        return f"darsi_tarixi_tojik_{g}", f"Таърихи халқи тоҷик (синфи {g})", f"История таджикского народа ({g} класс)", ["таърихи халки точик", "таърих", "история", f"синфи {g}", f"{g} класс"]
    if "чугрофи" in low or "географ" in low or "jugrofi" in low:
        return f"darsi_jugrofiya_{g}", f"Ҷуғрофия (синфи {g})", f"География ({g} класс)", ["чугрофия", "география", f"синфи {g}", f"{g} класс"]
    if "хукук" in low or "конститут" in low or "право" in low:
        return f"darsi_huquq_{g}", f"Ҳуқуқ ва инсон (синфи {g})", f"Право человека ({g} класс)", ["хукук", "право", "конститутсия", f"синфи {g}", f"{g} класс"]
    if "табиатшинос" in low or "эколог" in low:
        return f"darsi_tabiat_{g}", f"Табиатшиносӣ (синфи {g})", f"Природоведение ({g} класс)", ["табиатшиноси", "природа", "экология", f"синфи {g}", f"{g} класс"]
    if "рус" in low:
        return f"darsi_zabon_rusi_{g}", f"Забони русӣ (синфи {g})", f"Русский язык ({g} класс)", ["забони руси", "русский язык", f"синфи {g}", f"{g} класс"]
    if "english" in low or "англис" in low:
        return f"darsi_english_{g}", f"Забони англисӣ (синфи {g})", f"Английский язык ({g} класс)", ["забони англиси", "english", f"синфи {g}", f"{g} класс"]
    return f"darsi_umumi_{g}", f"Китобҳои дарсии синфи {g}", f"Учебники {g} класса", [f"синфи {g}", f"{g} класс", "мактаб"]


def split_clean_chunks(raw_text: str, book_title: str, author: str = "", target_chars: int = 620) -> List[Dict[str, Any]]:
    paras = [p.strip() for p in re.split(r"\n\s*\n", raw_text) if len(p.strip()) >= 35]
    chunks: List[Dict[str, Any]] = []
    buf = ""
    approx_page = 1
    chars_seen = 0
    for p in paras:
        chars_seen += len(p)
        approx_page = max(1, chars_seen // 1800 + 1)
        if len(buf) + len(p) + 2 <= target_chars:
            buf = f"{buf}\n\n{p}".strip() if buf else p
        else:
            cleaned = clean_literary_text(buf, max_len=950)
            if cleaned:
                chunks.append({"t": book_title, "a": author, "p": approx_page, "x": cleaned})
            buf = p
    if buf:
        cleaned = clean_literary_text(buf, max_len=950)
        if cleaned:
            chunks.append({"t": book_title, "a": author, "p": approx_page, "x": cleaned})
    return chunks


def extract_pdf_with_ocr_fallback(pdf_bytes: bytes, book_title: str, author: str = "", max_pages: int = 320) -> List[Dict[str, Any]]:
    """Читает цифровой PDF через PyMuPDF, а для бумажных сканов автоматически включает OCR `Tesseract (tgk+rus)`."""
    try:
        import pymupdf as fitz
    except ImportError:
        import fitz

    try:
        fitz.TOOLS.mupdf_display_errors(False)
    except Exception:
        pass

    out_chunks: List[Dict[str, Any]] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        n_pages = min(len(doc), max_pages)
        for p_idx in range(n_pages):
            page = doc[p_idx]
            raw_txt = (page.get_text("text") or "").strip()
            if len(raw_txt) < 45:
                # Бумажный скан -> рендер 200 DPI и OCR Tesseract (tgk+rus) при наличии tesseract в системе
                try:
                    pix = page.get_pixmap(dpi=190, colorspace=fitz.csGRAY)
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=True) as tmp_img:
                        tmp_img.write(pix.tobytes("png"))
                        tmp_img.flush()
                        res = subprocess.run(
                            ["tesseract", tmp_img.name, "stdout", "-l", "tgk+rus", "--psm", "3"],
                            capture_output=True,
                            text=True,
                            timeout=22,
                        )
                        if res.returncode == 0 and len(res.stdout.strip()) >= 45:
                            raw_txt = res.stdout.strip()
                except Exception:
                    pass
            cleaned = clean_literary_text(raw_txt, max_len=980)
            if cleaned:
                out_chunks.append({"t": book_title, "a": author, "p": p_idx + 1, "x": cleaned})
    return out_chunks


def route_poetry_chunk_to_packs(chunk: Dict[str, Any]) -> List[str]:
    low = fold_for_match(f"{chunk.get('t', '')} {chunk.get('a', '')} {chunk.get('x', '')}")
    matched_packs: List[str] = []
    for pid, _, _, triggers in POET_PROFILES:
        if any(tr in low for tr in triggers):
            matched_packs.append(pid)
    for t_id, (_, _, kw_list) in THEMATIC_POETRY_PACKS.items():
        if any(fold_for_match(k) in low for k in kw_list):
            matched_packs.append(t_id)
    if not matched_packs:
        matched_packs.append("shero_pandu_akhloq")
    return matched_packs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="releases-repo/vectors", help="Папка сохранения .avdb и catalog.json")
    ap.add_argument("--search-index", default="", help="Локальный путь к search-index.json (иначе скачивается)")
    ap.add_argument("--kitobho-zip", default="", help="Локальный путь к kitobho.zip (иначе скачивается)")
    ap.add_argument("--president-index", default="", help="Локальный путь к president/index.json")
    ap.add_argument("--max-pdf-books", type=int, default=28, help="Макс. новых PDF книг из sharipovip/books за этот прогон")
    ap.add_argument("--max-runtime-minutes", type=float, default=210.0, help="Мягкий лимит времени одного прогона GitHub Actions")
    args = ap.parse_args()

    start_ts = time.time()
    out_dir = Path(args.out_dir)
    packs_dir = out_dir / "packs"
    packs_dir.mkdir(parents=True, exist_ok=True)
    cursor_path = out_dir / "books_cursor.json"
    cursor: Dict[str, Any] = {"processed_ids": [], "updated_at": ""}
    if cursor_path.exists():
        try:
            cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    processed_set = set(cursor.get("processed_ids", []))

    pack_chunks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    pack_meta: Dict[str, Tuple[str, str, str, List[str]]] = {}

    def register_pack(pid: str, title_tg: str, title_ru: str, cat: str, kw: List[str]) -> None:
        if pid not in pack_meta:
            pack_meta[pid] = (title_tg, title_ru, cat, list(kw))
        else:
            old_tg, old_ru, old_cat, old_kw = pack_meta[pid]
            pack_meta[pid] = (old_tg, old_ru, old_cat, old_kw + kw)

    # Регистрируем персональные векторы поэтов и тематические сборники стихов
    for pid, t_tg, t_ru, trigs in POET_PROFILES:
        register_pack(pid, t_tg, t_ru, "poetry_author", list(trigs) + ["шеър", "адабиёт", "шоир"])
    for tid, (t_tg, t_ru, kw) in THEMATIC_POETRY_PACKS.items():
        register_pack(tid, t_tg, t_ru, "poetry_theme", kw + ["шеър", "стих"])

    # 1. Загружаем каталог всех 15 700 книг sharipovip/books и раскладываем по сотням предметных и авторских микро-векторов
    if args.search_index and Path(args.search_index).exists():
        s_idx = json.loads(Path(args.search_index).read_text(encoding="utf-8"))
    else:
        raw = urllib.request.urlopen(SEARCH_INDEX_URL, timeout=30).read().decode("utf-8")
        s_idx = json.loads(raw)

    books_items = s_idx.get("items", [])
    author_books_map: Dict[str, List[str]] = defaultdict(list)

    for item in books_items:
        b_name = (item.get("name") or "").strip()
        b_author = (item.get("author") or "").strip()
        b_cat = (item.get("category") or "Дигар").strip()
        b_sub = (item.get("subcategory") or "").strip()
        b_folder = (item.get("folder") or "").strip()
        if not b_name:
            continue

        cat_slug = slugify_id(b_cat)
        sub_slug = slugify_id(b_sub) if b_sub else ""
        pack_id = f"lib_{cat_slug}_{sub_slug}"[:48] if sub_slug and sub_slug != cat_slug else f"lib_{cat_slug}"[:48]
        title_tg = f"{b_cat}" + (f" — {b_sub}" if b_sub else "")
        register_pack(pack_id, title_tg, title_tg, "library", [b_cat, b_sub, b_name[:30], b_author])

        desc_tg = f"Китоб: «{b_name}»." + (f" Муаллиф: {b_author}." if b_author else "") + f" Бахш: {title_tg} (манбаъ: Китобхонаи Алия)."
        chunk_obj = {"t": b_name, "a": b_author, "p": 0, "x": desc_tg}
        pack_chunks[pack_id].append(chunk_obj)

        low_combo = fold_for_match(f"{b_name} {b_author} {b_cat} {b_sub} {b_folder}")
        for pid, _, _, trigs in POET_PROFILES:
            if any(tr in low_combo for tr in trigs):
                pack_chunks[pid].append(chunk_obj)
                author_books_map[pid].append(b_name)

    # Добавляем сводные карточки «в каких книгах есть этот поэт/учёный» прямо внутрь каждого авторского вектора!
    for pid, t_tg, _, _ in POET_PROFILES:
        b_list = sorted(set(author_books_map.get(pid, [])))
        if b_list:
            sample_books = "», «".join(b_list[:25])
            summary_x = f"Дар китобхонаи Алия оид ба {t_tg.split('—')[0].strip()} {len(b_list)} китоб мавҷуд аст, аз ҷумла: «{sample_books}»."
            pack_chunks[pid].insert(0, {"t": t_tg, "a": "", "p": 1, "x": summary_x})

    # 2. Обрабатываем все 208 оцифрованных школьных учебников 1–11 классов (`kitobho.zip`)
    kz_bytes: Optional[bytes] = None
    if args.kitobho_zip and Path(args.kitobho_zip).exists():
        kz_bytes = Path(args.kitobho_zip).read_bytes()
    else:
        try:
            kz_bytes = urllib.request.urlopen(KITOBHO_ZIP_URL, timeout=45).read()
        except Exception:
            kz_bytes = None

    if kz_bytes:
        with zipfile.ZipFile(io.BytesIO(kz_bytes)) as zf:
            for zname in zf.namelist():
                if zname.endswith("/") or not zname.lower().endswith(".txt"):
                    continue
                m_grade = re.search(r"(\d{1,2})", zname.split("/")[0])
                grade = int(m_grade.group(1)) if m_grade else 5
                fname = Path(zname).stem.strip()
                raw_txt = zf.read(zname).decode("utf-8", errors="ignore")
                pid, t_tg, t_ru, kw = classify_school_pack_id(fname, grade)
                register_pack(pid, t_tg, t_ru, "school_textbook", kw + [fname])
                extracted = split_clean_chunks(raw_txt, book_title=fname, target_chars=640)
                pack_chunks[pid].extend(extracted[:220])
                # Поэтические фрагменты из учебников Адабиёт/Забони модарӣ также пополняют векторы поэтов и стихов!
                if "adabiyot" in pid or "zaboni_modari" in pid:
                    for ch in extracted[:140]:
                        for p_target in route_poetry_chunk_to_packs(ch):
                            if len(pack_chunks[p_target]) < 320:
                                pack_chunks[p_target].append(ch)

    # 3. Архив `president/` (`index.json`)
    p_idx: Any = None
    if args.president_index and Path(args.president_index).exists():
        p_idx = json.loads(Path(args.president_index).read_text(encoding="utf-8"))
    else:
        try:
            p_idx = json.loads(urllib.request.urlopen(PRESIDENT_INDEX_URL, timeout=20).read().decode("utf-8"))
        except Exception:
            p_idx = None

    if isinstance(p_idx, dict) and isinstance(p_idx.get("cats"), dict):
        for cat_key, items_list in p_idx["cats"].items():
            pid = f"president_{slugify_id(cat_key)}"
            register_pack(pid, f"Пешвои миллат — {cat_key}", f"Архив Президента — {cat_key}", "president_archive", ["пешвои миллат", "президент", "эмомали рахмон", cat_key])
            if isinstance(items_list, list):
                for entry in items_list[:350]:
                    if isinstance(entry, dict):
                        title = str(entry.get("title") or entry.get("name") or "").strip()
                        date_s = str(entry.get("date") or "").strip()
                        summary = clean_literary_text(str(entry.get("text") or entry.get("summary") or title), max_len=850)
                        if summary:
                            pack_chunks[pid].append({"t": f"{title} ({date_s})".strip(" ()"), "a": "president.tj", "p": 0, "x": summary})

    # 4. Порционная глубокая вычитка PDF книг из `sharipovip/books` (Шеърҳо, Адабиёти классикӣ, Чистон, Пешвои Миллат и далее по курсору!)
    priority_cats = ("Шеърҳо", "Чистон", "Адабиёти классикӣ", "Пешвои Миллат", "КИТОБҲО БАРОИ ПЕШВОИ МИЛЛАТ", "Таърих", "Ҳуқуқшиносӣ, Қонунгузорӣ")
    pdf_candidates = sorted(
        [it for it in books_items if str(it.get("id", "")).lower().endswith(".pdf") and int(it.get("size") or 0) < 18_000_000],
        key=lambda x: (0 if x.get("category") in priority_cats else 1, int(x.get("size") or 0)),
    )

    fetched_pdfs = 0
    for item in pdf_candidates:
        if fetched_pdfs >= args.max_pdf_books:
            break
        if (time.time() - start_ts) / 60.0 >= args.max_runtime_minutes:
            break
        book_rel = str(item.get("id") or "")
        if not book_rel or book_rel in processed_set:
            continue
        url = f"{BOOKS_RAW_BASE}/" + "/".join(urllib.parse.quote(seg) for seg in book_rel.split("/"))
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AliyaVectorBuilder/1.0"})
            pdf_bytes = urllib.request.urlopen(req, timeout=25).read()
            b_title = str(item.get("name") or Path(book_rel).stem).strip()
            b_author = str(item.get("author") or "").strip()
            b_cat = str(item.get("category") or "Дигар").strip()
            b_sub = str(item.get("subcategory") or "").strip()
            extracted_pdf = extract_pdf_with_ocr_fallback(pdf_bytes, book_title=b_title, author=b_author, max_pages=90)
            if extracted_pdf:
                cat_slug = slugify_id(b_cat)
                sub_slug = slugify_id(b_sub) if b_sub else ""
                target_pid = f"lib_{cat_slug}_{sub_slug}"[:48] if sub_slug and sub_slug != cat_slug else f"lib_{cat_slug}"[:48]
                register_pack(target_pid, b_cat, b_cat, "library", [b_cat, b_sub, b_title])
                pack_chunks[target_pid].extend(extracted_pdf[:180])
                if b_cat in ("Шеърҳо", "Адабиёти классикӣ", "Чистон"):
                    for ch in extracted_pdf[:180]:
                        for rp in route_poetry_chunk_to_packs(ch):
                            if len(pack_chunks[rp]) < 360:
                                pack_chunks[rp].append(ch)
            processed_set.add(book_rel)
            fetched_pdfs += 1
        except Exception:
            processed_set.add(book_rel)
            continue

    # 5. Записываем все готовые микро-векторы `.avdb` и единый реестр `catalog.json`
    catalog_entries: List[Dict[str, Any]] = []
    for pid in sorted(pack_chunks.keys()):
        chunks = pack_chunks[pid]
        if not chunks:
            continue
        # Дедупликация чанков внутри одного микро-вектора
        seen_x = set()
        unique_chunks = []
        for c in chunks:
            key = c["x"][:120]
            if key not in seen_x:
                seen_x.add(key)
                unique_chunks.append(c)
        if not unique_chunks:
            continue
        t_tg, t_ru, cat, kw = pack_meta.get(pid, (pid, pid, "library", [pid]))
        avdb_file = packs_dir / f"{pid}.avdb"
        entry = write_avdb_pack(avdb_file, pid, t_tg, t_ru, cat, kw, unique_chunks[:400])
        catalog_entries.append(entry)

    cursor["processed_ids"] = sorted(processed_set)
    cursor["total_books_in_repo"] = len(books_items)
    cursor["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    cursor_path.write_text(json.dumps(cursor, ensure_ascii=False, indent=2), encoding="utf-8")

    catalog = {
        "format": "aliya-micro-vectors-v1",
        "updated_at": cursor["updated_at"],
        "total_packs": len(catalog_entries),
        "total_books_indexed": len(books_items),
        "deep_pdfs_processed": len(processed_set),
        "packs": catalog_entries,
    }
    (out_dir / "catalog.json").write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    total_mb = sum(e["size_bytes"] for e in catalog_entries) / (1024.0 * 1024.0)
    print(f"✅ Собрано микро-векторов: {len(catalog_entries)} ({total_mb:.2f} МБ), обработано PDF по курсору: {len(processed_set)}")


if __name__ == "__main__":
    main()
