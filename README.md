# 真实FTP两种能力与可续传故障

`python3 ftp_source.py --profile a --port 0 --audit-file /绝对临时路径`运行真实被动FTP，stdout返回端口。A支持MLSD/SIZE/REST，B不支持这些扩展，只可NLST和RETR。REST为该控制会话的下一次传输设置偏移，随后自动清零；应在读取/核对descriptor后再设置。

RETR descriptor.json拿当前批次，包含文件generation、大小和SHA。data.bin为16384字节，第一次只发送4096后控制426；稳定generation时A可REST 4096取得余下12288，B只能重新下载16384。control.bin首次全部字节后426，late.csv首次短文件却226，列表还省略late.csv。

`--change-after-first`使data.bin首次中断后内容与generation及batch_id一起更换，旧前缀不能拼新generation。源合同保证同generation字节不变；不声称仅mtime/size即可证明未变。audit仅独立验收使用，不可代替客户端正常传输与身份核验。

稳定A恢复基准只统计中断后data.bin的数据通道字节，含重试/重复读取，不含清单/MLSD/控制字符及其他文件；上限12288。B同基准上限16384。generation变化或本地prefix破坏的负例可放弃旧批次并显式重选新批次，不套此恢复成本上限。
