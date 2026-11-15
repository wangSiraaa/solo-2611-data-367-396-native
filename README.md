# 本地exchange输入与可控阻塞
`python3 generate.py out`生成两份JSONL以及扩展后的独立预期总数。每条记录带(source_id,seq,key,value,fanout)；fanout=0..3。
目标不是补一个给定队列算法，而是实现完整的两源、分区worker、sink执行闭环。可以使用queue、asyncio或进程与成熟库。
`schedules.json`中的控制点是待实现执行器的测试控制接口，不能用sleep时长代替达到控制点的证据。
