"""policy.json 解析与纯格式校验。

违反规则的信息只引用 表.列 / 规则 ID / 域名，绝不出现源数据原值。
动作三种：drop（删除）、keep（原样保留）、tokenize:<domain>（令牌化）。
"""

from dataclasses import dataclass, field
import json

ACTION_DROP = "drop"
ACTION_KEEP = "keep"
ACTION_TOKENIZE = "tokenize"
TOKENIZE_PREFIX = "tokenize:"


@dataclass
class DomainSpec:
    name: str
    prefix: str
    alphabet: str
    width: int

    @property
    def token_length(self):
        return len(self.prefix) + self.width

    @property
    def capacity(self):
        """后缀空间大小 = len(alphabet) ** width。"""
        return len(self.alphabet) ** self.width


@dataclass
class ColumnRule:
    fq: str
    action: str  # ACTION_DROP / ACTION_KEEP
    domain: str | None = None  # 仅 tokenize 使用

    @property
    def is_tokenize(self):
        return self.domain is not None


@dataclass
class Policy:
    policy_id: str
    domains: dict[str, DomainSpec]
    columns: dict[str, ColumnRule]
    sensitive_columns: frozenset[str]
    public_keep_columns: frozenset[str]
    raw: dict = field(default_factory=dict)

    def rule(self, fq):
        return self.columns.get(fq)


def load_policy(path):
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    violations = []

    policy_id = str(raw.get("policy_id", "")) or "anonymous"

    raw_domains = raw.get("domains") or {}
    domains = {}
    for dname, spec in raw_domains.items():
        prefix = str(spec.get("prefix", ""))
        alphabet = str(spec.get("alphabet", ""))
        width = spec.get("width")
        if not isinstance(width, int) or width <= 0:
            violations.append(f"R008: domain={dname!r} width 必须为正整数")
            continue
        if not alphabet:
            violations.append(f"R008: domain={dname!r} alphabet 不能为空")
            continue
        if len(set(alphabet)) != len(alphabet):
            violations.append(f"R008: domain={dname!r} alphabet 含重复字符")
            continue
        if not prefix:
            violations.append(f"R009: domain={dname!r} 缺少前缀")
            continue
        if len(set(prefix)) != len(prefix):
            violations.append(f"R009: domain={dname!r} 前缀含重复字符")
            continue
        if len(prefix) + width > 1024:
            violations.append(f"R008: domain={dname!r} 令牌长度超出 1024 上限")
            continue
        domains[dname] = DomainSpec(
            name=dname, prefix=prefix, alphabet=alphabet, width=width
        )

    # R010：不同域前缀必须互不相同，避免公开令牌跨域混淆身份。
    seen_prefix = {}
    for dname, spec in domains.items():
        if spec.prefix in seen_prefix:
            violations.append(
                f"R010: domain={dname!r} 与 domain={seen_prefix[spec.prefix]!r} 前缀冲突"
            )
        else:
            seen_prefix[spec.prefix] = dname

    columns = {}
    for fq, action in (raw.get("columns") or {}).items():
        if not isinstance(action, str) or "." not in fq:
            violations.append(f"R005: column={fq!r} 策略动作非法")
            continue
        if action == ACTION_DROP:
            columns[fq] = ColumnRule(fq=fq, action=ACTION_DROP)
        elif action == ACTION_KEEP:
            columns[fq] = ColumnRule(fq=fq, action=ACTION_KEEP)
        elif action.startswith(TOKENIZE_PREFIX):
            domain = action[len(TOKENIZE_PREFIX):]
            if not domain:
                violations.append(f"R005: column={fq!r} 缺少域声明")
                continue
            if domain not in domains:
                violations.append(
                    f"R006: column={fq!r} 引用未声明 domain={domain!r}"
                )
                continue
            columns[fq] = ColumnRule(fq=fq, action=ACTION_TOKENIZE, domain=domain)
        else:
            violations.append(f"R005: column={fq!r} 策略动作非法")

    sensitive = frozenset(raw.get("sensitive_columns") or [])
    public_keep = frozenset(raw.get("public_keep_columns") or [])

    # R014：敏感清单与列策略集合必须一致（清单列必须有策略，反之亦然）。
    for fq in sensitive:
        if fq not in columns:
            violations.append(f"R014: column={fq!r} 在敏感清单中但缺少列策略")
    for fq in columns:
        if fq not in sensitive and fq not in public_keep:
            violations.append(f"R014: column={fq!r} 有列策略但未登记在任何清单中")

    # R011：敏感列禁止原样保留。
    for fq in sensitive:
        rule = columns.get(fq)
        if rule is not None and rule.action == ACTION_KEEP:
            violations.append(f"R011: column={fq!r} 为敏感列却声明 keep")

    if violations:
        from .errors import PlanRejected

        raise PlanRejected(violations)

    return Policy(
        policy_id=policy_id,
        domains=domains,
        columns=columns,
        sensitive_columns=sensitive,
        public_keep_columns=public_keep,
        raw=raw,
    )


# 供 ColumnRule 统一使用的动作常量
ACTION_TOKENIZE = TOKENIZE_PREFIX.rstrip(":")
