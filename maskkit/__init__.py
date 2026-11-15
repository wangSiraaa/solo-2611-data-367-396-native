"""maskkit — 本地脱敏数据包生产工具。

子命令：
  init-vault 初始化持久 token-vault
  plan       读取只读源库与 policy，做全部静态校验并输出计划
  release    原子发布一个脱敏 release.sqlite 与公开回执
  verify     重开成品库核查约束、令牌格式与跨批稳定性
  recover    核查/修复中断的发布记录
"""

__version__ = "1.0.0"
