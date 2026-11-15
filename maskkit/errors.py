"""统一错误类型。

所有面向用户的错误信息只允许引用表/列/规则 ID 与域代号，
严禁携带任何源库中的原身份值。
"""


class MaskKitError(Exception):
    """可预期的工具错误，exit_code 决定 CLI 退出码。"""

    exit_code = 1


class PlanRejected(MaskKitError):
    """plan 阶段静态校验拒绝。"""

    exit_code = 2

    def __init__(self, violations):
        self.violations = list(violations)
        super().__init__("; ".join(self.violations))


class CapacityExhausted(MaskKitError):
    """域代号空间耗尽，属于可预期拒绝而非数据损坏。"""

    exit_code = 3

    def __init__(self, domain, rule_id="R015"):
        super().__init__(f"{rule_id}: domain={domain!r} 可用代号已耗尽")
        self.domain = domain
        self.rule_id = rule_id


class ReleaseInterrupted(MaskKitError):
    """发布在中途中断，可由 recover 修复。"""

    exit_code = 4
