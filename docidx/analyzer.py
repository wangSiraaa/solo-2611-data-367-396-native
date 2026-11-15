"""分析器：词法扫描与停词。

按题面实现，不使用任何分词库的默认语义：
- 按 Unicode 码点逐个扫描，连续的 isalpha() 或 isdigit() 码点构成一个词；
- term = 原词 casefold()；
- position 是该词在值内的序号（含停词，停词不压缩后续位置）；
- 同时记录原词在该字段值 UTF-8 字节序列上的半开区间 [start, end)。
"""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Token:
    term: str          # casefold 后的词项，用于倒排
    text: str          # 原词（码点序列，未改写）
    position: int      # 值内序号，停词也占位置
    byte_start: int    # 原字段值 UTF-8 编码上的起始字节（含）
    byte_end: int      # 结束字节（不含）
    is_stopword: bool


def tokenize(value: str) -> list[Token]:
    """扫描一个字段值，返回全部词（含停词），保持值内顺序。"""
    raw = value.encode("utf-8")
    tokens: list[Token] = []
    position = 0
    i = 0
    n = len(value)
    while i < n:
        ch = value[i]
        if ch.isalpha() or ch.isdigit():
            j = i + 1
            while j < n and (value[j].isalpha() or value[j].isdigit()):
                j += 1
            word = value[i:j]
            # 码点区间 [i, j) 映射到 UTF-8 字节区间
            byte_start = len(value[:i].encode("utf-8"))
            byte_end = byte_start + len(word.encode("utf-8"))
            # 自检：字节区间切回的字节必须正好是原词的字节
            assert raw[byte_start:byte_end] == word.encode("utf-8")
            tokens.append(
                Token(
                    term=word.casefold(),
                    text=word,
                    position=position,
                    byte_start=byte_start,
                    byte_end=byte_end,
                    is_stopword=False,
                )
            )
            position += 1
            i = j
        else:
            i += 1
    return tokens


class Analyzer:
    """停词表仅决定某词是否进入倒排，不影响任何 position/字节坐标。"""

    def __init__(self, stopwords: list[str]):
        self.stopwords = frozenset(w.casefold() for w in stopwords)

    @classmethod
    def load(cls, path: str) -> "Analyzer":
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return cls(cfg.get("stopwords", []))

    def is_stopword(self, term: str) -> bool:
        return term.casefold() in self.stopwords

    def indexable(self, value: str) -> list[Token]:
        """返回应进入倒排的词（过滤停词），position 保持原值内序号。"""
        return [t for t in tokenize(value) if t.term not in self.stopwords]

    def all_tokens(self, value: str) -> list[Token]:
        return tokenize(value)
