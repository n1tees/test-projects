from __future__ import annotations

import re
from collections import Counter


WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё]+(?:-[A-Za-zА-Яа-яЁё]+)*", re.UNICODE)


def extract_words(line_text: str) -> list[str]:
    if not line_text:
        return []
    return [token.lower() for token in WORD_RE.findall(line_text)]


def count_lemmas_in_line(line_text: str, normalizer: object) -> dict[str, int]:
    words = extract_words(line_text)
    if not words:
        return {}

    freq: Counter[str] = Counter()
    for word in words:
        lemma = normalizer.normalize(word)
        freq[lemma] += 1
    return dict(freq)
