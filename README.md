# 多分辨率时间来源

source.Source是受限来源门面：catalog、read_summary(stream,width,start)、read_raw(stream,a,b)、inspect_calls。执行器只可通过这些公开方法取数，不直接读_rows/_summaries或调用fixture_rows/state替代执行。函数fixture_rows/state供环境生成与独立oracle使用。

每个summary只有固定大小充分状态，未保存原观测或来源ID列表。width=10、40，coverage=[0,80)，四个stream见catalog。raw只返回相交原件，若原区间跨越查询边界，执行器必须自行裁切。calls为真实返回字节/行计数。

请求基准见fixtures/profile.json。旧版少量观测仅保留在replaced/390-v1，本版以来源门面为正式输入。
