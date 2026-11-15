operator.py是持久、可复用、按run/round多路输出的真实本机设施，不是目标反馈执行器。固定启动两个进程：python operator.py --kind expand 和 python operator.py --kind reduce。两个作业配置的算法由设施提供。目标负责在真实交错下路由、迭代提交、取消与复用。
一个run的tail_gate可暂停reduce尾部，另一个run仍能在同PID的expand/reduce推进。使用cancel帧可解除被取消上下文的门控；不会终止另一run。不要为正常每轮重启进程规避共享状态。
