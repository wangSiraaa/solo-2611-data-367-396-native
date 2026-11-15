# 边界补充

- 全部控制与数据源用显式end帧结束；source.py可用--end输出尾帧。EOF不代表end。
- 依赖控制未到：waiting_dependency；全部源end后仍缺依赖：missing_dependency；缺片：incomplete。
- 新完成事务与历史已提交事务有无依赖写冲突时只拒绝新事务，不能追溯撤销已提交事实。双方已知完整且均未提交则都拒绝。合法源事务中同key写必须有依赖路径。
- 外键父对象由同事务先前操作或显式依赖提供；无隐藏依赖推断。
- dependencies的abort/failed前驱令后继blocked，不相关就绪事务可以继续。
