events.json为一个JSON数组，op按数组顺序接收，读取时用json.load。probe.py只提供小样本全量当前集oracle。time是整数业务时间，不是接收序号；完整接受/拒绝及session增量发布由候选实现。
