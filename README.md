这是两个独立SQLite副本的持续双向行复制题。seed-A.sqlite、seed-B.sqlite是无未提交事务的真实初始数据库，副本身份分别A/B；请复制到工作目录后运行，原件只读。source.py的低层设施可直接执行，不是完整复制器，尤其没有因果前沿/冲突物化/当前head CAS。
关键材料是event-contract.md和scenario.json。远端事件须保留原origin/seq；把receive套进本地write会产生新版本回声。场景有离线并发编辑、resolution先于其parents到达、重复传输、删除/修改冲突及显式墓碑resolution。全部是合成数据，不需真实网络或服务。
