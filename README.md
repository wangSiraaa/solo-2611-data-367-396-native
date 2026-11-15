运行 python3 make_source.py 生成服务私有图。候选启动 GraphAPI(private_file) 后，只使用 capabilities() 与 query_edges(request,token) 取边，不能读取私有文件、导入make_source.generate或访问_rows。作者/验收oracle可单独读取私有输入。

request字段：snapshot_id、label、direction(out/in)、keys，out可选target_keys（仅能力声明支持时）。in按边dst筛keys，out按src筛keys。每请求keys 1–4个；target_keys至4个。返回edge_id/src/dst/label/cost，页25，按edge_id排序，total_matches是该请求全候选数量，不代表路径数量。next_token必须沿用原request逐页直到null；变更keys/target/方向/label后不能复用。

图版本只读，不要求在线更新。相同edge_id跨API页/不同访问路径表示同一边；同src/dst/label而不同edge_id仍是平行边，不能去重成一条。成本预算计实际API调用和返回边记录（重复传输照计）；另外报告request_bytes/edge_payload_bytes。元数据capabilities调用不计边查询数。API内部扫描不是客户端图访问，不向客户端开放全量scan。
