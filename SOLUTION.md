# Arrow C 列批接管与多下游分流

在给定真实生产者 `source.c` / `arrow_c.h`（Arrow C Data + C Stream ABI）之上，
实现了一个 Python 3.11 + ctypes 的本机列批**接管**与**零拷贝切片分流**工具。
生产者未被修改，所有取值和释放都经过真实的 C 数据/回调。

```
abr/
  abi.py        ctypes 结构体布局 + 回调原型 + 位图/值读取（含 ABI sizeof 断言）
  loader.py     编译/加载 libsource（优先 gcc/clang；找不到时自动构建）
  batch.py      ImportedBatch：Arrow C Data 移动所有权接管、依赖计数、恰好一次 release
  stream.py     ProducerStream：真实 get_next / get_schema / release vtable
  view.py       View：零拷贝连续切片（offset/length/共享地址）、read/release/cancel
  driver.py     有界驱动器：场景操作、槽位背压、清理
  audit.py      事件审计与无地址身份摘要
  checks.py     独立校验（源语义 oracle + 生命周期 oracle）
  scenarios.py  覆盖全部要求行为的确定性场景集合
  cli.py        可复跑 CLI
tests/test_lifecycle.py   unittest 生命周期测试
out/            运行产物：<scenario>.json（结果+校验+摘要）与 <scenario>.audit.jsonl（逐事件审计）
```

## 运行

```bash
# 1) 全部内置场景 + 校验（失败时非零退出）
python3 -m abr.cli run --all --outdir out

# 2) 直接跑题面给的 scenario.json
python3 -m abr.cli run --scenario scenario.json --outdir out --verbose

# 3) 单元测试
python3 -m unittest discover -s tests -v

# 4) 列出/导出内置场景
python3 -m abr.cli list
python3 -m abr.cli write-scenarios --out scenarios/scenarios.json
```

首次运行若仓库根没有 `libsource.so`/`build/libsource.so`，`loader.py` 会用
`gcc/clang` 就地编译 `source.c`（不改动源文件）。也可 `--lib /path/to/libsource.so`。

当前结果：`15/15 scenarios passed`，`18` 个 unittest 全通过。

## 接管与所有权（对应题目要点）

`ImportedBatch.take()` 执行一次**校验后的移动**：

1. 校验 `n_buffers==2`、无 children、length/offset 合法、`release` 存在；
2. 把描述符按值复制到 Python 自身存储（仅复制小型描述符，不动 `buffers`）；
3. 紧接着把**导出结构**上的 `release` 与 `private_data` 置 NULL 并清空字段——
   移动后原导出结构不再拥有 release 职责；
4. 保留**原 release 回调地址**，在最后一个依赖句柄结束时通过真实 C 回调释放一次；
   `_releasing` 递归闸门保证重复释放只会产生状态错误，不会二次调用 C 回调。

因此“关闭 stream / 丢导入根 / 取消一个 view / 释放兄弟 view”都不会让仍有效的
view 失效，也不需要等整个作业退出才释放源批（见 `out/*.audit.jsonl` 中
`batch_release` 出现在依赖归零的那一刻）。

### 回收边界与备选组织方式

本实现采用**根句柄 + 依赖计数**：`ImportedBatch` 持有唯一 release 义务，
每个 `View` 在创建时 `add_dependency()`，在 read/release/cancel 终结时
`drop_dependency()`；当 `deps==0` 且**根句柄已丢弃**时立即调用原 release。
这个边界清楚地把“源分配何时可释放”与“谁还拿着视图”分开：

* **优点**：零拷贝；嵌套切片只累加 `offset`，不物化父视图；释放时机精确——
  最后一个依赖结束时立刻 free，不推迟到作业退出；根先丢/兄弟先关/stream 先关
  都自然成立；空视图也有同样的依赖语义（读 0 个元素，但维持批存活直到释放）。

* **另一种可行组织**：共享所有权（引用计数）句柄：每个 view 持有一个
  (`shared_ptr` 风格的) `BatchOwner`，或反过来由 view 持有“根 + 计数”，
  用 `weakref`/显式计数。功能等价，但通常会：
  * 让每个 view 多持有一个指向根的引用对象（每 view 增加一点 Python 对象与
    GC 压力，仍无数据拷贝）；
  * 若不提供 `drop_import` 这类显式“根放手”动作，批会在最后一个 view 结束时
    才释放——对比“根已释放但视图仍活”的场景，资源占用会多一份“根引用”的寿命；
  * 若改成按需复制/物化切片（例如为了便于任意过滤/重排）则失去零拷贝，内存
    与复制成本随切片大小线性增长，本实现明确不对任意过滤/重排做零拷贝。

## 零拷贝与 NULL

* `View` 只保存 `(values_addr, validity_addr, offset, length)`，均等于导入批的
  原始 buffer 地址（`checks.py::zero_copy_addresses` 逐个核对
  `view.values_addr == import.buffers[1]`、validity 同理）。
* 连续 slice 通过 `offset += start`、`length = len` 表达；嵌套切片在父视图
  上再累加，深度任意增长，不复制元素。
* 读取按**绝对位位置** `offset+i` 查 validity bitmap，再按字节位置
  `(offset+i)*8` 读 int64。因此 `start=2,len=1` 正确读出 NULL，
  `start=1,len=4` 读出 `[1, None, 3, 4]`。
* `View.read()` 返回验证用的小结果（`values/valid/null_count`），这是**检查用
  的读取**；视图本身不复制列，`tests` 与 `checks` 也证明地址共享。

## 有界驱动与背压

* `max_live_exported_batches`（缺省 2）定义同时“占用”的源批数量。
  一个批在**根导入句柄仍持有**或**尚有活 view**时占用槽位（两者都让源分配存活）。
* 槽位满：`next` **完全不调用** `get_next`，返回 `backpressure: true` 并写
  `backpressure` 审计事件（含 `get_next_called:false`、`occupied_slots`、
  `max_live_batches`）。不忙等、不预留全部输入。
* 释放依赖（view release/cancel 或 drop_import）让槽位在最后一个依赖结束时
  立即释放，后续 `next` 继续成功；`backpressure` 场景专门验证这一过程。
* 同时施加题面上限校验：最多 20 个批、每批 ≤1000 项（超限在**接管前**拒绝并
  通过真实 release 归还）、最多 8 个活动 view。
* 场景按操作序列表达下游交错（合同允许“给定屏障”）；`sleep` 作为显式屏障标记
  接受但驱动器单线程，不要求锁结构。`tests` 另用两个线程对独立 view 并发读取，
  验证句柄独立性（不作无锁承诺）。

## 安全 / 状态错误

* 已 `release`/`cancel` 的 view 再 `read`/`release`/`cancel` 抛
  `ViewStateError`，只写审计，不调用任何 C 回调（测试显式对比 batch_release
  计数不变）。
* 重复 `drop_import`、关闭已关闭 stream、在 EOS 后继续 `next`、非法切片
  （负 start/length、越界、嵌套下溢）都返回明确状态错误，且不破坏其它活句柄。
* 消费者异常（含场景注入的 `fail_at_operation`）后 `drain_all()` 仍会
  取消活 view、丢弃根、关闭 stream；产物中 `held_before_cleanup>0` 而
  `held_after_cleanup==0`，生产者 `live_batches` 最终归零。

## 审计内容

`out/<scenario>.audit.jsonl` 逐事件记录：原 buffer 地址（`import_take.buffers`）、
view 逻辑范围与共享地址（`view_create`）、实际 `get_next` 调用（`get_next`，含
调用序号）、`live_batches`/释放计数快照（`source_state`）、每次原 release
（`batch_release`，含原回调地址与原因）、背压（`backpressure`）、状态错误与
读取异常。摘要中的 release 统计按**场景批名**聚合，不以指针地址作跨进程身份。

## 覆盖矩阵（内置场景）

| 场景 | 覆盖点 |
| --- | --- |
| `given` | 题面 scenario.json 原样执行 |
| `sibling_first` | 兄弟 view 先释放、根先丢、stream 先关后仍可读 |
| `root_first` | 根导入先丢，最后一个依赖结束时释放 |
| `stream_first` | 关闭 stream 不影响已移动批；重复关闭/关闭后 next 报错 |
| `nested` | 嵌套切片累加 offset、共享地址、父先释放子仍有效 |
| `null_bits` | 非零 offset 下的 NULL 位读取、空切片 |
| `cancel` | 取消隔离 + 取消句柄后续操作报错 |
| `released_read` | 已释放句柄操作报错且不触发额外 C 回调 |
| `backpressure` | 满槽不调用 get_next，释放依赖后继续 |
| `bounds` | 负值/越界/嵌套越界拒绝 |
| `import_handle_lifecycle` | 丢弃导入根后不能新建 view，但既有 view 仍可读 |
| `barrier_multi` | 多下游交错读/取消/释放 + 屏障 + 槽位背压 |
| `exception_cleanup` | 注入消费者异常，清理仍释放全部接管资源 |
| `empty_views` | 空视图读 0 元素、任意顺序释放 |
| `all_batches` | 3 个真实批 + 多种视图顺序 + EOS 后 next 报错 |

## 范围声明

同 ABI、int64 + validity bitmap、连续切片零拷贝；不实现任意 Arrow 类型、
过滤/重排零拷贝、网络服务或 ABI 全标准。`source.c` 未做任何修改。
