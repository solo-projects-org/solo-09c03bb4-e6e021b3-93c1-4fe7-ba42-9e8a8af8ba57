# 交易修订 → 审核时点训练特征

把交易修订流水转换为「审核时点可见」的训练特征，并提供网页上传、查看、下载。

## 启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>。数据保存在 compose 卷 `ptf-data`（SQLite + 原始上传文件 + 报告文件），
`docker compose down` 不会删除；需要清空时执行 `docker compose down -v`。

本地不使用 Docker 时：

```bash
python3 -m pip install -r requirements-dev.txt
DATA_DIR=./data uvicorn app.main:create_app --factory --port 8000
python3 -m pytest tests -q
```

## 输入格式

时刻一律为 **UTC 秒整数**（如 `1736510400`），不接受 `2025-01-10T12:00:00Z` 这类字符串。
UTF-8 编码（可带 BOM），表头按列名匹配，允许多余列但不允许重复或缺失必需列。
中文表头（`事件号`、`账户`、`发生时刻`、`源系统入库时刻`、`金额`、`更新/撤销`、
`样本号`、`审核时刻`、`标签`）同样可用。

交易 CSV：

| 列 | 说明 |
| --- | --- |
| `event_id` | 事件号，业务上的“同一笔交易” |
| `account_id` | 账户 |
| `occurred_at` | 该修订声明的事件发生时刻（UTC 秒） |
| `ingested_at` | 源系统入库时刻（UTC 秒），即“何时可见” |
| `amount` | 十进制金额，固定两位小数，不得为科学计数法；更新须 > 0，撤销为 0（或任意非负值） |
| `action` | `revise` / `cancel`（也接受 `update`/`correct` 与 `void`/`撤销`） |

样本 CSV：`sample_id, account_id, audit_at, label`，`label` 只能为 `0`/`1`，样本号全局唯一。

`ingested_at` 相同的一对 (event_id, ingested_at) 只允许一个语义版本：同一批次内或跨批次
传入不同取值时**整批拒收**、零写入；内容完全一致的重传是幂等的（记为批次但不新增修订）。

## 特征口径

对每个样本，审核时刻为 `T`：

1. **可见性**：只看 `ingested_at <= T` 的修订（报告还额外只看创建报告时冻结的批次）。
2. **事件取最新修订**：同一 `event_id` 取 `ingested_at` 最大（同日按插入顺序）的那一条；
   该事件在其余时点的修订全部丢弃。
3. **窗口**：仅当被选中的那条修订自身的 `occurred_at ∈ [T − 7 天, T)`（左闭右开）时计入。
   撤销（`cancel`）无论时刻一律不计。
4. 输出 `txn_count_7d`、`txn_amount_7d`（两位小数字符串），以及窗口起止的 UTC 秒。

关键后果：事件若被改到窗口之外（或从窗外改进来），计入的是**最新修订自己的发生时刻**，
不会沿用旧修订的窗口内取值；审核后才入库的修订对本次审核不可见。

报告文件列：

```
sample_id,account_id,audit_at,window_start_at,window_end_at,txn_count_7d,txn_amount_7d,label
```

## 版本冻结与失败语义

- 点击“生成报告”的瞬间，以数据库事务冻结“已入库批次高水位”（交易与样本各一个）。
  报告此后只会读取 `batch_id <= 冻结水位` 的行；期间补传（含迟到补录）不会混入。
  需要包含新数据时另建一份新报告。
- 源文件按内容哈希 + UUID 落盘，永不覆盖；批次元数据（文件名、字节数、SHA-256、行数）
  一并冻结在报告清单里，页面可核对。
- 报告先写临时文件、`fsync`、原子 `rename`，随后才把状态改为 `ready`。
  写盘/校验失败时状态为 `failed`，下载接口返回 409，不会暴露半成品。
- “按原版本重取”会用冻结的高水位重跑，并在重跑前校验源数据摘要；
  源数据若被篡改则拒绝重取。状态为 `failed` 或文件被删除/篡改时，均可这样恢复。

## 样例

`samples/` 下：

| 文件 | 用途 |
| --- | --- |
| `transactions_base.csv` | 基础流水：E2 入库前改金额、E3 撤销、E4 改到窗外 |
| `transactions_late.csv` | 迟到补录：E7 改期、E9/E10 审核后入库、E10 后续撤销、E11 改到窗外 |
| `samples.csv` | 5 个样本（含无交易账户 C） |
| `transactions_conflict_rejected.csv` | 同事件同入库时刻异值 → 整批拒收演示 |

建议流程：上传 `transactions_base.csv` + `samples.csv` → 生成报告（得 S001=4 笔/280.00）→
再上传 `transactions_late.csv` → **旧报告不变**，新报告 S002=3 笔/110.00。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/uploads/transactions`、`/api/uploads/samples` | multipart `file`；422 返回逐行错误 |
| GET | `/api/batches` | 已入库批次清单 |
| POST | `/api/reports` | 冻结版本并排队生成；无数据返回 409 |
| GET | `/api/reports`、`/api/reports/{id}` | 状态与冻结清单 |
| GET | `/api/reports/{id}/download` | 未就绪/校验失败返回 409 |
| POST | `/api/reports/{id}/regenerate` | 按原冻结版本重取 |
| GET | `/api/docs` | OpenAPI 文档 |

## 设计取舍

- 单容器、SQLite（WAL）、进程内单线程报告 worker：够用且没有额外组件；
  并发上传靠 `BEGIN IMMEDIATE` 串行化，批次语义是全有或全无。
- 报告生成是“冻结快照 + 后台构建”。生成期间页面显示 `pending/running`，
  不阻塞上传；下载入口只在 `ready` 且 SHA-256 复核通过后开放。
- 金额用整数分存储，输出两位小数字符串，避免浮点误差。
