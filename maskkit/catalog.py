"""只读源库的结构自省。

约束信息一律来自 PRAGMA 元数据，不解析 CREATE TABLE 文本，
以保证输出库重建的 PK / UNIQUE / NOT NULL / FK 与声明一致。
"""

from dataclasses import dataclass, field
import sqlite3


@dataclass
class Column:
    name: str
    cid: int
    decl_type: str | None
    notnull: bool
    default: str | None
    pk_pos: int  # 0 表示非主键列，>=1 表示在复合主键中的位置


@dataclass
class ForeignKey:
    fid: int
    seq: int
    ref_table: str
    from_col: str
    to_col: str | None  # NULL 表示引用父表主键


@dataclass
class UniqueIndex:
    name: str
    columns: list[str]
    origin: str  # 'u' = UNIQUE 约束, 'c' = CREATE UNIQUE INDEX, 'pk' = 主键索引


@dataclass
class Table:
    name: str
    columns: list[Column]
    fks: list[ForeignKey] = field(default_factory=list)
    unique_indexes: list[UniqueIndex] = field(default_factory=list)
    create_sql: str = ""

    def column(self, name):
        for col in self.columns:
            if col.name == name:
                return col
        return None

    @property
    def pk_columns(self):
        return [c.name for c in sorted(self.columns, key=lambda c: c.pk_pos)
                if c.pk_pos > 0]

    @property
    def fk_from_columns(self):
        return {fk.from_col for fk in self.fks}

    @property
    def fk_target_columns(self):
        return {fk.to_col for fk in self.fks if fk.to_col}

    @property
    def unique_columns(self):
        cols = set()
        for idx in self.unique_indexes:
            cols.update(idx.columns)
        return cols

    def participates_in_constraint(self, col_name):
        """该列是否参与 PK / UNIQUE / 任一外键的任一端。"""
        if col_name in self.pk_columns:
            return True
        if col_name in self.fk_from_columns or col_name in self.fk_target_columns:
            return True
        if col_name in self.unique_columns:
            return True
        return False


@dataclass
class Catalog:
    tables: dict[str, Table]

    def table_names(self):
        return sorted(self.tables)

    def get_table(self, name):
        return self.tables.get(name)

    def has_column(self, fq):
        table, col = fq.split(".", 1)
        t = self.tables.get(table)
        return t is not None and t.column(col) is not None

    def all_fk_pairs(self):
        """生成 (子表, 子列, 父表, 父列(或 None 表示父主键))。"""
        for tname in sorted(self.tables):
            for fk in self.tables[tname].fks:
                yield tname, fk.from_col, fk.ref_table, fk.to_col


def open_source(path):
    """以只读不可变方式打开源库；调用方负责关闭。"""
    uri = f"file:{path}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_catalog(conn):
    names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    tables = {}
    for name in names:
        cols = []
        for r in conn.execute(f'PRAGMA table_info("{name}")'):
            cols.append(
                Column(
                    name=r["name"],
                    cid=r["cid"],
                    decl_type=r["type"],
                    notnull=bool(r["notnull"]),
                    default=r["dflt_value"],
                    pk_pos=r["pk"],
                )
            )

        fks = []
        for r in conn.execute(f'PRAGMA foreign_key_list("{name}")'):
            fks.append(
                ForeignKey(
                    fid=r["id"],
                    seq=r["seq"],
                    ref_table=r["table"],
                    from_col=r["from"],
                    to_col=r["to"],
                )
            )

        indexes = []
        for r in conn.execute(f'PRAGMA index_list("{name}")'):
            if not r["unique"]:
                continue
            idx_cols = [
                ir["name"]
                for ir in conn.execute(
                    f'PRAGMA index_info("{r["name"]}")'
                )
            ]
            indexes.append(
                UniqueIndex(
                    name=r["name"],
                    columns=idx_cols,
                    origin=r["origin"],
                )
            )

        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()[0]
        tables[name] = Table(
            name=name, columns=cols, fks=fks,
            unique_indexes=indexes, create_sql=sql or "",
        )
    return Catalog(tables=tables)
