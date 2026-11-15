"""合成测试库装配器：plan / generate / verify。

读取只读的真实 schema.sql 与 generation.json，在独立新 SQLite 库中
装配合成数据。全程启用真实外键与约束，任何无解/拒绝都不发布脏库。
"""

__version__ = "1.0.0"
