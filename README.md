# 跨分区事务材料
`source.py p0` / `source.py p1` / `source.py control`分别输出真实JSONL流。分区流允许跨事务交错；op_no是事务内部顺序，part_no是事务在该分区的顺序。control声明part_count、dependencies和terminal状态。
数据和控制可以由调用者任意交错，但每条源流内部顺序保持。相同片段可重送，内容不同的同身份片段属于损坏。源只读，不要求Kafka或PostgreSQL服务器。
