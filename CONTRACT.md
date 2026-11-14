# 采样导出的断线重试

## 现有链路
仅一个下载任务和一个工作者，不并行调用 run。Python 标准库和 SQLite。
collector.py 下载，runtime.py 原子入账，pipeline.py 处理最高修订和墓碑。
app.py 是上述函数的薄入口，不含安装或服务管理；不要求升级历史数据库。

调用 argv：
- ["python3","app.py","init",ROOT,CONTROL]：创建新仓库和独立暂停目录。
- ["python3","app.py","enqueue",ROOT,TASK_ID,URL]：接纳 HTTP 导出。
- ["python3","app.py","run",ROOT]：推进一次下载尝试。
- ["python3","app.py","downloads",ROOT]：返回下载状态数组。
- ["python3","app.py","read",ROOT,BATCH_ID]：读取已入账结果。

enqueue 沿用 collector.enqueue，run 沿用 collector.run_one，downloads 沿用
collector.status，read 沿用 runtime.read；既有参数、返回结构、任务幂等及批次冲突语义保持兼容。
TASK_ID 只含字母、数字、下划线和连字符。URL 为无凭据和片段的 HTTP URL。
一次 run 只推进一次 GET 尝试；短响应回到 queued，可通过下一次 run 重试。
queued/downloading 可推进，failed/completed 不再自动下载。
状态字段为 task_id、url、state、etag、length、sha256、received、error、receipt。
received 是本次任务已接收的前缀长度，安全确认偏移可以另存。
允许新增内部表或列，不能改动已有业务处理和原断言。
连接使用 http.client.HTTPConnection，保留 download 的 connection_factory 注入入口；
新增续取内部逻辑同样经该连接入口，录制源才能重放。

sample.json 是完整业务输入。导出正文为该 JSON 编码，允许末尾空白；
任务完成保留 downloads/TASK_ID.raw，其字节必须等于完整源正文。
样例汇总为 [{"station":"east","minute":0,"count":2,"sum":0},
{"station":"west","minute":1,"count":1,"sum":70}]。
最高 rev 生效，deleted 不汇总，零和非空组保留。
回执含 batch_id、input_sha256、result_sha256，后两者对规范编码计算，
不同于含填充空白的导出摘要；同批重投仍只有一行 ledger。

## 版本与异常边界
源提供强 ETag、完整实体的 X-Content-SHA256 和 Content-Length；无压缩。
完整 200 的 Content-Length 是完整长度；206 的长度是本响应体长度，
Content-Range 给出起止字节和完整长度。范围为从偏移到实体末尾。
同一 ETag 的内容不可变；不同请求之间源可以切换版本，长度也可改变。
条件范围不匹配时返回新版本完整 200，不能将其接在旧前缀之后。
206 只有在版本、完整长度、摘要、请求起点、范围终点及响应长度相互一致时才可信。
未请求的 206、错误范围和摘要不符的载荷不得提交给 runtime.submit。
对不可信范围可将任务置 failed，或清除旧前缀后留 queued 等待下次整包尝试。
416 本身不是下载完整的证据；可丢弃前缀，下一次重取整包。
短响应可保留已确认前缀；不足声明长度不能完成。
最终完整长度和 SHA256 验证通过后才发布 .raw 和进入原入账流程。
不改 runtime.py 的入账事务、幂等规则或 pipeline.py 的业务口径。

## 可重复验证
["python3","-B","checks.py","verify"] 保留原业务和源响应断言。
["python3","-B","bench.py","probe"] 为无文件写入的整包下载计量探针。
["python3","-B","bench.py","baseline"] 核对当前正确实现并报告实际传输量，
包含正常完成、三次截断、版本切换、两处终止重开和坏响应拒绝。
["python3","-B","bench.py","target"] 对相同负载增加优化门槛断言。
bench.py 用真实 HTTPResponse 解析录制响应；不是 HTTP 解析的替代实现。
每次 invoke 都启动全新 Python 进程，状态通过真实文件和 SQLite 续接，
Wire 计量实际交给客户端的响应体字节，不计头部和本地重读。
源、负载、计量器、checks.py 和既有断言不得修改以达标。

基准导出恰为 8388608 字节，三次 GET 各交付 1048576 字节后截断，
第四次不截断。同版本总响应体字节不得超过 8437760。
版本变化例先截断版本1，再提供同长度但内容、摘要、ETag均不同的版本2，
最终 west 汇总为110，必须保存版本2完整字节。
错误范围例强制返回起止与总长不一致的206；摘要错误例声明全零摘要；
两者不得产生 jobs 或 ledger 行。

written 和 checkpoint 两组各先交付1MiB后截断，再在下一进程的首个新块处 SIGKILL，
最后用新进程继续。同版本每组总响应体字节也不得超过 8437760。
written 钩子必须在实际写入新块后、该块进度提交前；
checkpoint 钩子必须在该块进度事务提交后。
保留 runtime.barrier(ROOT,PHASE) 调用及 CONTROL/hook.json 协议；
不能把钩子移到空操作、重置状态或另一条非生产路径来通过测试。
终止时文件可能比确认进度长；恢复只能复用可确认的前缀。
这些试验只模拟进程 SIGKILL 和网络截断，不模拟磁盘损坏或机器断电。

所有成功例独立核对完整源字节、源摘要、采样汇总、原回执和唯一入账。
失败载荷不得进入业务链路；重复 run 不得再次传输已完成任务。
完成优化后运行清单 verify 改为 ["python3","-B","bench.py","target"]；
demo 也使用 target，输出真实传输量、请求和核账结果。
附简短说明，解释文件前缀与持久进度的确认边界，并记录可重跑实测。
