"""全部错误类型。造库前可拒绝的问题与运行期失败严格区分。"""


class SynthDBError(Exception):
    """所有本工具错误的基类。"""


class SubsetViolation(SynthDBError):
    """schema 含 schema-subset.md 之外的结构/约束，造库前拒绝。"""

    def __init__(self, message: str, location: str | None = None, raw: str | None = None):
        self.location = location
        self.raw = raw
        prefix = f"[{location}] " if location else ""
        super().__init__(f"{prefix}{message}" + (f"  原文: {raw!r}" if raw else ""))


class SchemaParseError(SynthDBError):
    """schema 不是合法 SQLite DDL，或声明文件结构非法。"""


class DeclarationError(SynthDBError):
    """generation.json 不符合固定行槽/有限域声明规则。"""


class UnsatError(SynthDBError):
    """约束问题无解。携带可独立枚举核对的冲突子问题。"""

    def __init__(self, message: str, conflict: dict | None = None):
        self.conflict = conflict or {}
        super().__init__(message)


class LoadRejected(SynthDBError):
    """真实 SQLite 拒绝了已生成的赋值；整份输出不发布。"""

    def __init__(self, message: str, mapping: dict | None = None):
        self.mapping = mapping or {}
        super().__init__(message)


class VerifyError(SynthDBError):
    """成品库独立复核失败。"""
