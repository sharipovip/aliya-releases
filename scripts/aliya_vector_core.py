#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ядро векторной и текстовой базы Алии (`pipeline/aliya_vector_core.py`).

Обеспечивает 100% побитовую совместимость между серверным сборщиком Python (GitHub Actions)
и мобильным движком Android Kotlin (`OfflineKnowledgeVault.kt` / `AliyaVectorHub.kt`):
  1) Жёсткая литературная очистка текстов и результатов OCR (замена старых букв `ѓ→ғ, ќ→қ, њ→ҳ, љ→ҷ, ў→ӯ, ї→ӣ`,
     склейка переносов, удаление мусора сканера и разметки).
  2) Единая нормализация и стемминг таджикских и русских слов + кросс-языковые смысловые кластеры.
  3) Детерминированный 128-мерный квантованный `int8` семантический вектор на базе `CRC32`
     (работает за < 0.5 мс на телефоне без 2 ГБ ОЗУ!).
  4) Компактный контейнер микро-векторов `.avdb` (Gzip JSONL с метаданными, чанками, страницами и `int8` векторами).
"""

import base64
import gzip
import html
import json
import math
import re
import unicodedata
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

LEGACY_TAJIK_MAP = str.maketrans({
    "ѓ": "ғ", "Ѓ": "Ғ",
    "ќ": "қ", "Ќ": "Қ",
    "њ": "ҳ", "Њ": "Ҳ",
    "љ": "ҷ", "Љ": "Ҷ",
    "ў": "ӯ", "Ў": "Ӯ",
    "ї": "ӣ", "Ї": "Ӣ",
})

FOLD_DIACRITICS_MAP = str.maketrans({
    "ё": "е",
    "ӣ": "и",
    "қ": "к",
    "ӯ": "у",
    "ҳ": "х",
    "ҷ": "ч",
    "ғ": "г",
})

STOPWORDS: Set[str] = {
    # Тоҷикӣ
    "ва", "дар", "аз", "ба", "бо", "ки", "ин", "он", "то", "хам", "аст", "хаст", "нест", "буд",
    "шуд", "шуда", "мешавад", "мебошад", "барои", "чун", "агар", "лекин", "аммо", "еки", "чи",
    "кист", "чист", "кадом", "кучо", "чаро", "чанд", "тавр", "хел", "гуна", "бораи", "оид",
    "ман", "ту", "шумо", "мо", "онхо", "инхо", "худ", "худи", "хама", "хамаи", "хар", "як", "яке",
    "китоб", "китоби", "сахифа", "сахифаи", "синф", "синфи", "боб", "боби", "мавзуъ", "маълумот",
    # Русский
    "и", "в", "во", "на", "с", "со", "по", "из", "к", "ко", "о", "об", "от", "до", "за", "при",
    "для", "без", "про", "или", "но", "а", "же", "ли", "бы", "не", "ни", "это", "то", "все",
    "кто", "что", "как", "где", "когда", "почему", "зачем", "какой", "какая", "какие", "сколько",
    "такой", "такая", "такое", "быть", "был", "была", "были", "есть", "будет", "мне", "мой", "моя",
    "расскажи", "скажи", "объясни", "покажи", "найди", "книга", "книги", "учебник", "страница", "класс",
}

# Двуязычные смысловые кластеры (дают совпадение смысла даже если вопрос на русском, а книга на таджикском, и наоборот!)
SEMANTIC_CLUSTERS: List[Tuple[str, Tuple[str, ...]]] = [
    ("c_rudaki", ("рудаки", "панчруд", "одамушшуаро", "буи чуи мулиен")),
    ("c_firdavsi", ("фирдавси", "шохнома", "шахнаме", "рустам", "сухроб")),
    ("c_sino", ("сино", "авиценна", "ибни сино", "алконун", "конуни тиб", "шифо")),
    ("c_khayyam", ("хайем", "хайям", "рубои", "рубаи")),
    ("c_hofiz", ("хофиз", "хафиз", "шерози", "газал")),
    ("c_saadi", ("саъди", "саади", "гулистон", "бустон")),
    ("c_jomi", ("чоми", "джами", "бахористон", "хафт авранг")),
    ("c_kamol", ("камол", "хучанди")),
    ("c_bedil", ("бедил", "мирзо абдулкодир")),
    ("c_donish", ("ахмади дониш", "наводир")),
    ("c_ayni", ("айни", "садриддин", "еддоштхо", "доxунда", "марги судхур", "гуломон")),
    ("c_tursunzoda", ("турсунзода", "мирзо", "чони ширин", "осие", "хасани аробакаш")),
    ("c_loiq", ("лоик", "шерали", "модарнома", "дасти дуои модар")),
    ("c_bozor", ("бозор собир", "забони модари")),
    ("c_farzona", ("фарзона", "хучанд", "шеъри муосир")),
    ("c_gulrukhsor", ("гулрухсор", "сафиева")),
    ("c_lohuti", ("лохути", "абулкосим")),
    ("c_somoni", ("сомони", "исмоили", "сомониен", "саманид")),
    ("c_gafurov", ("гафуров", "бобочон", "точикон")),
    ("c_peshvo", ("пешвои миллат", "эмомали рахмон", "президент", "истиклолият", "вахдат")),
    ("c_modar", ("модар", "мама", "мать", "матери", "модарнома", "мехри модар")),
    ("c_vatan", ("ватан", "родина", "точикистон", "таджикистан", "механ")),
    ("c_navruz", ("навруз", "бахор", "весна", "суманак")),
    ("c_ishq", ("ишк", "мухаббат", "любовь", "ошик", "дил")),
    ("c_poem", ("шеър", "шеърхо", "стих", "стихи", "стихотворение", "газал", "рубои", "байт", "назм")),
    ("c_riddle", ("чистон", "загадка", "загадки", "зарбулмасал", "макол", "пословица", "пословицы")),
    ("c_bio", ("биология", "хучайра", "клетка", "фотосинтез", "растани", "хайвонот", "анатомия", "ген", "днк")),
    ("c_math", ("математика", "риёзи", "алгебра", "геометрия", "муодила", "уравнение", "формула", "каср", "дробь")),
    ("c_phys", ("физика", "кувва", "сила", "суръат", "энергия", "барк", "электричество", "нур", "атом")),
    ("c_chem", ("химия", "элемент", "модда", "реаксия", "кислота", "менделеев", "молекула")),
    ("c_it", ("информатика", "компютер", "компьютер", "алгоритм", "барномасози", "программирование", "интернет", "иттилооти")),
    ("c_hist", ("таърих", "история", "аср", "давлат", "тамаддун", "бостон")),
    ("c_geo", ("чугрофия", "география", "кух", "дарё", "иклим", "ахоли", "помир", "кӯҳ")),
    ("c_law", ("хукук", "конун", "конститутсия", "право", "закон", "кодекс", "моддаи")),
    ("c_med", ("тиб", "саломати", "бемори", "табобат", "дору", "медицина", "здоровье", "лечение", "витамин")),
    ("c_cook", ("хурок", "палав", "плов", "курутоб", "самбуса", "шурбо", "ошхона", "рецепт", "приготовить")),
]

_WORD_RE = re.compile(r"[а-яёӣқӯҳҷғa-z0-9]{2,}", flags=re.IGNORECASE)
_CORRUPT_OCR_RE = re.compile(r"[|~^§¤¦¨°±²³µ¶·¸º¼½¾¿×÷ƒ„…†‡ˆ‰Š‹ŒŽ‘’•™š›œžŸ]{2,}")
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def crc32_u32(text: str) -> int:
    """Беззнаковый CRC32 (0..2^32-1), на 100% идентичный `java.util.zip.CRC32` в Android Kotlin."""
    return zlib.crc32(text.encode("utf-8")) & 0xFFFFFFFF


def clean_literary_text(raw: str, max_len: int = 1600) -> str:
    """
    Вычищает сырой текст из PDF / OCR / Wiki до состояния гладкого литературного текста
    без странных букв, переносов и мусорных символов. Возвращает `""`, если текст битый.
    """
    if not raw:
        return ""
    s = html.unescape(raw)
    s = unicodedata.normalize("NFC", s)
    s = s.translate(LEGACY_TAJIK_MAP)
    s = _HTML_TAG_RE.sub(" ", s)
    # Убираем спецсимволы управления и мягкие переносы
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\xad\ufeff\ufffd]", "", s)
    # Склеиваем переносы по слогам в PDF/сканах (`маъ-\nлумот` -> `маълумот`)
    s = re.sub(r"([а-яёӣқӯҳҷғА-ЯЁӢҚӮҲҶҒ])-\s*\n\s*([а-яёӣқӯҳҷғ])", r"\1\2", s)
    # Убираем одиночные номера страниц на отдельных строках
    s = re.sub(r"(?m)^\s*[-–—•·]?\s*\d{1,4}\s*[-–—•·]?\s*$", "", s)
    # Убираем мусор сканера и повторяющиеся спецсимволы
    s = _CORRUPT_OCR_RE.sub(" ", s)
    s = re.sub(r"[|_~^`\\]{2,}", " ", s)
    s = re.sub(r"([.,!?;:—\-])\1{2,}", r"\1", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    if len(s) < 18:
        return ""

    # Проверка доли нормальных букв (отсекает битую кодировку старых сканов вроде `Ìàúëóìîò`)
    non_space = [ch for ch in s if not ch.isspace()]
    if not non_space:
        return ""
    valid_chars = sum(
        1 for ch in non_space
        if ("а" <= ch.lower() <= "я")
        or ch in "ёЁӣқӯҳҷғӢҚӮҲҶҒ"
        or ("a" <= ch.lower() <= "z")
        or ch.isdigit()
        or ch in ".,!?;:—–-«»\"'()%/+=\n•"
    )
    cyr_letters = sum(1 for ch in non_space if ("а" <= ch.lower() <= "я") or ch in "ёЁӣқӯҳҷғӢҚӮҲҶҒ")
    if valid_chars / float(len(non_space)) < 0.82 or cyr_letters < 10:
        return ""

    if len(s) > max_len:
        cut = s[:max_len]
        last_stop = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "), cut.rfind(".\n"))
        s = cut[: last_stop + 1].strip() if last_stop >= max_len // 2 else (cut.rsplit(" ", 1)[0].strip() + "…")
    return s


def fold_for_match(text: str) -> str:
    return (text or "").lower().translate(LEGACY_TAJIK_MAP).translate(FOLD_DIACRITICS_MAP)


def stem_word(word: str) -> str:
    w = word
    if len(w) >= 6:
        for suf in ("хои", "гон", "ро", "хо", "ам", "ат", "аш", "он", "ого", "ому", "ыми", "ими", "ами", "ями", "ах", "ях", "ов", "ев", "ии", "ие", "ый", "ий", "ая", "ое"):
            if w.endswith(suf) and len(w) - len(suf) >= 3:
                w = w[: -len(suf)]
                break
    return w


def extract_stems(text: str, max_stems: int = 24) -> List[str]:
    folded = fold_for_match(text)
    words = _WORD_RE.findall(folded)
    out: List[str] = []
    seen: Set[str] = set()
    for w in words:
        if w in STOPWORDS or w.isdigit():
            continue
        st = stem_word(w)
        if len(st) >= 3 and st not in STOPWORDS and st not in seen:
            seen.add(st)
            out.append(st)
            if len(out) >= max_stems:
                break
    return out


def shard_ids_for_text(question_or_title: str, extra_hint: str = "", num_shards: int = 64, max_buckets: int = 2) -> List[int]:
    """Возвращает 1–2 детерминированных номера шарда (0..63) по ключевым корням текста."""
    stems = extract_stems(f"{question_or_title} {extra_hint}", max_stems=8)
    if not stems:
        return [crc32_u32(fold_for_match(question_or_title)[:32]) % num_shards]
    # Предпочитаем наиболее информативные (длинные/специфичные) корни
    ranked = sorted(stems, key=lambda s: (min(len(s), 9), s), reverse=True)
    buckets: List[int] = []
    for st in ranked:
        b = crc32_u32(st) % num_shards
        if b not in buckets:
            buckets.append(b)
            if len(buckets) >= max_buckets:
                break
    return buckets


def embed128_int8(text: str) -> bytes:
    """
    Строит 128-мерный L2-нормированный квантованный `int8` вектор (128 байт):
    объединяет смысловые кластеры RU<->TJ, стеммы слов, биграммы и символьные 4-граммы.
    Полностью воспроизводим в Kotlin через `java.util.zip.CRC32`!
    """
    dim = 128
    vec = [0.0] * dim
    folded = fold_for_match(text)

    def _add_feat(feat: str, weight: float) -> None:
        h = crc32_u32(feat)
        idx = h & 127
        sign = 1.0 if ((h >> 7) & 1) == 0 else -1.0
        vec[idx] += sign * weight

    # 1. Межъязыковые смысловые кластеры (высший вес)
    for cid, triggers in SEMANTIC_CLUSTERS:
        if any(tr in folded for tr in triggers):
            _add_feat(f"cls:{cid}", 4.5)

    # 2. Стеммы значимых слов + биграммы
    stems = extract_stems(folded, max_stems=32)
    for i, st in enumerate(stems):
        _add_feat(f"w:{st}", 3.0)
        if i + 1 < len(stems):
            _add_feat(f"bg:{st}_{stems[i + 1]}", 1.8)
        # 3. Символьные 4-граммы для устойчивости к окончаниям и опечаткам
        padded = f"^{st}$"
        if len(padded) >= 4:
            for j in range(min(8, len(padded) - 3)):
                _add_feat(f"g:{padded[j:j + 4]}", 0.75)

    norm = math.sqrt(sum(v * v for v in vec))
    if norm < 1e-9:
        return bytes(dim)
    out = bytearray(dim)
    for i, v in enumerate(vec):
        q = int(round((v / norm) * 127.0))
        q = max(-127, min(127, q))
        out[i] = q & 0xFF
    return bytes(out)


def embed128_b64(text: str) -> str:
    return base64.b64encode(embed128_int8(text)).decode("ascii")


def write_avdb_pack(
    out_path: Path,
    pack_id: str,
    title_tg: str,
    title_ru: str,
    category: str,
    keywords: List[str],
    chunks: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Записывает микро-вектор `.avdb` (сжатый Gzip-контейнер):
      Строка 1: JSON-шапка пакета (`id`, `title_tg`, `title_ru`, `category`, `keywords`, `count`)
      Строки 2..N: JSON-чанки (`t` = книга/заголовок, `a` = автор, `p` = стр., `x` = чистый текст, `s` = стеммы, `v` = base64 int8 вектор)
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header = {
        "format": "aliya-avdb-v1",
        "id": pack_id,
        "title_tg": title_tg,
        "title_ru": title_ru,
        "category": category,
        "keywords": sorted(set(fold_for_match(k).strip() for k in keywords if k.strip()))[:60],
        "count": len(chunks),
    }
    raw_lines = [json.dumps(header, ensure_ascii=False)]
    for ch in chunks:
        txt = ch["x"]
        title = ch.get("t", "")
        author = ch.get("a", "")
        page = int(ch.get("p", 0) or 0)
        stems = extract_stems(f"{title} {author} {txt}", max_stems=18)
        v_b64 = embed128_b64(f"{title} {author} {txt}")
        rec = {
            "t": title,
            "a": author,
            "p": page,
            "x": txt,
            "s": " ".join(stems),
            "v": v_b64,
        }
        raw_lines.append(json.dumps(rec, ensure_ascii=False))

    payload = ("\n".join(raw_lines) + "\n").encode("utf-8")
    compressed = gzip.compress(payload, compresslevel=9)
    out_path.write_bytes(compressed)
    import hashlib
    sha = hashlib.sha256(compressed).hexdigest()
    return {
        "id": pack_id,
        "file": out_path.name,
        "title_tg": title_tg,
        "title_ru": title_ru,
        "category": category,
        "keywords": header["keywords"],
        "chunks": len(chunks),
        "size_bytes": len(compressed),
        "sha256": sha,
    }
