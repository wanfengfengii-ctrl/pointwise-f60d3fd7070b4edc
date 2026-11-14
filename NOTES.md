# 断线续取实现说明

## 改动范围
只改 `collector.py`。`runtime.py` 的入账事务/幂等、`pipeline.py` 的业务口径、
`bench.py`/`checks.py`/`portal.py` 的源与断言均未改动。新增 `boundary.py`，
运行清单 `.pointwise-runtime.json` 的 verify/demo 改为 `bench.py target`。

## 可确认前缀的两侧边界
- **持久侧**：`downloads.received`（连同 `etag/length/sha256`）只在一整块
  （16 KiB 读取，最多累计 32 KiB）写入并 flush 后通过一个 SQLite 事务提交。
  提交前崩溃的块不在确认范围内。
- **文件侧**：`.part` 的字节可能多于 `received`（`written` 钩子恰在
  “已写新块、进度未提交”之后触发，进度事务前 SIGKILL 会留下超长文件）。
  每次 `run_one` 先读 SQLite，再把 `.part` **截断到 `received`**；若文件比
  确认进度短，则视为前缀不可确认，清零重来。因此“复用”的长度永远是
  数据库与文件共同证明的那一段。
- **checkpoint 与 written 的区别**：checkpoint 钩子在进度事务提交后，文件
  长度==确认长度；written 钩子在其前，文件更长，恢复时会丢掉多出的块。
  两种情况下恢复请求的 `Range` 起点都由 `received` 取，实测分别为
  `bytes=1081344-` 与 `bytes=1048576-`。

## 续取协议与信任判定
- 有确认前缀时发 `Range: bytes=<received>-` + `If-Range: <etag>`。
- 206 必须同时满足：`Content-Range` 为 `bytes start-end/total`、`start==received`、
  `end==total-1`、`Content-Length==total-received`、`ETag` 与已存版本相同、
  `total` 等于已存完整长度、`X-Content-SHA256` 等于已存摘要，才可信；
  否则 `RangeError`（CONTENT_RANGE/LENGTH/ETAG/DIGEST），任务置 failed，
  不入账、不发布 `.raw`。
- 未请求的 206 → `UNREQUESTED_RANGE`，拒绝。
- 条件范围不匹配时源返回完整 200：即使长度/摘要与旧版本相同也**不能拼接**；
  先清 SQLite 中的前缀再清空 `.part`（先库后文件，保证任何后续崩溃都不会
  复用旧前缀），然后用这一个 200 作为新实体继续流式写。
- 416：不是完成证据。清零前缀、截断 `.part`，任务留 queued 等下次整包重取。
- 短响应（不足 `Content-Length`）：保留已确认前缀（`OSError` → queued），
  下次从该偏移续取；长度与 SHA-256 全通过后才 `os.replace` 发布 `.raw`，
  再走原 `runtime.submit` / `runtime.run_one` / `read` 链路与唯一入账。

## 实测（可重跑）

```
python3 -B checks.py verify    # 原业务 + 源响应断言
python3 -B bench.py probe      # 整包基线探针
python3 -B bench.py target     # 优化门槛（verify/demo 使用）
python3 -B boundary.py         # 独立边界验证（进程级续取/终止/坏响应）
python3 -B smoke.py            # 真实 socket + app.py CLI 端到端
```

`bench.py target` 的真实结果（响应体字节，门槛 LIMIT = 8437760 = 8 MiB + 48 KiB）：

| 负载 | 响应体字节 | 说明 |
|---|---|---|
| normal | 8388608 | 一次整包 |
| stable | 8388608 | 1MiB×3 截断 + 续取，零重复 |
| written | 8404992 | SIGKILL 在进度提交前，只丢未确认的 16384 字节 |
| checkpoint | 8388608 | SIGKILL 在进度提交后，无重复 |
| changed | 9437184 | 版本1 截断 1MiB 后版本2 完整 200（旧前缀正确舍弃） |
| range | 1048576 | 坏 206 零字节被接收，job/ledger 为空 |
| digest | 8388608 | 全零摘要，不入账 |

`boundary.py` 另外覆盖：两次截断三段 Range 请求、416 清零后整包重取、
对坏 206 的 start/end/total/length/etag/digest 逐一拒绝、完成后再次 run
为 0 字节且 ledger 仍只有一行。
