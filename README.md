# 异构聚合来源材料

用 `python3 source_api.py --ready-file /tmp/agg-ready.json --log-file /tmp/agg-http.jsonl` 启动本机来源；实际端口写入ready文件。`GET /catalog` 返回能力，`POST /query` 接受JSON。A/B原始数据文件仅供服务器与独立小样本oracle读取，候选执行路径必须走HTTP。

请求包含source、version、kind、group_by，可选regions、page_size、cursor。kind为raw、aggregate或distinct_keys；B仅支持raw。所有页面的complete/cursor有明确终态。关闭来源聚合能力可用`--disable-aggregates`。

aggregate返回count、sum_cents、nonnull_count及local_distinct_count。sum_cents在无非NULL值时为0，须结合nonnull_count解释最终SQL式NULL。local_distinct_count只能表示这个来源自己的精确计数，不能跨来源相加；distinct_keys返回可用于全局精确去重的customer键。未分组空输入仍有一个零状态，分组空输入没有组。

来源不支持min_cents过滤。需要该过滤的查询必须在正确阶段本地执行，不能在原始行被聚合丢弃后再假装完成。金额单位均为整数分，region=NULL是独立分组，customer=NULL不计入DISTINCT，空串计入。

queries.json是查询示例，允许按题面有限语法变化；每侧occurrence_id只在各自来源内部唯一，跨源所有出现均属于输入bag。
