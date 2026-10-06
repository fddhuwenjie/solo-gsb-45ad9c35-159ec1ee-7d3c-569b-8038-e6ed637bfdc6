# 事务交错推演（Transaction Interleaving Playground）

用 Python + FastAPI + 原生 HTML/CSS/JavaScript 构建的事务调度可视化网页：
编辑初始键值与 2～3 条事务脚本，系统按**轮转调度**（依次从各事务队头取操作）
排出同一份全局调度，再分别在三种隔离级别下生成确定性轨迹并逐步展示；
另提供**严格两阶段锁（Strict 2PL）**推演：锁冲突会真正改变执行顺序，
阻塞、释放重试与等待图死锁回滚均可逐步查看。

## 隔离级别与锁协议

| 模式 | 规则 |
|---|---|
| **Read Uncommitted (RU)** | 读键上的**最新写入**，包括其他事务尚未提交的版本；写入方若随后 ABORT，该读列入「脏读清单」 |
| **Read Committed (RC)** | 每次读只取键上**最新已提交版本**——“最新”按**提交顺序**而非写入顺序（先写者可以晚提交，晚提交者即为当前版本）；不读未提交数据，亦不看自身未提交的写 |
| **Snapshot Isolation (SI)** | 读 **BEGIN 时的快照**，自身写入优先；BEGIN 之后同键若已被其他事务提交，提交时按**首提交者胜**整笔回滚 |
| **Strict 2PL（严格两阶段锁）** | `READ` 取**共享锁 S**、`WRITE` 取**排他锁 X**（已持 S 可升级为 X，S 与 S 兼容、其余冲突）；**所有锁持有到 COMMIT / ABORT 才释放**。冲突时记录持锁阻塞者、**保留队头**并转试其他事务，锁释放后下一轮自动重试；**等待关系成环**时回滚**环内编号最大**的事务：撤销其写入、跳过剩余操作并释放锁。读值**自身写入优先**，否则取按提交顺序最新的已提交版本 |

页面支持：

- 逐步 / 自动播放每一条调度操作，展示**读值、来源版本、写入者**与逐条解释；
- 每一步刷新**版本链**（初始 / 未提交 / 已提交 / 已回滚版本，读写位置高亮）与事务状态；
- 切换 **Strict 2PL** 标签可查看加锁推演：锁表（S/X 与持锁者）、等待图（成环红色高亮）、
  阻塞保留队头与释放后重试、死锁时回滚环内编号最大者（撤销写入 / 跳过操作 / 释放锁）、
  读值来源（自身写入优先否则最新已提交）与最终状态；
- 按**已提交事务的真实读写关系**生成三种依赖边：
  - `WR`：Tj 读到 Ti 已提交版本（Ti → Tj）；
  - `WW`：同键上 Ti 先写、Tj 后写且均提交（Ti → Tj，**按真实写入顺序**，与提交先后无关）；
  - `RW`（反依赖）：Ti 读过某版本**之后**，同键才有 Tj 提交的新版本（Ti → Tj；只读时已经提交的版本，即使版本编号更大，也不构成反依赖）；
  - 图中发现环时，环上的边与对应的读/写操作红色高亮；
- 单列「**脏读清单**」：读到后来被 ABORT 的未提交数据；
- **调度对照**：输入由 T1/T2/T3 组成的自定义执行顺序（每项取对应事务尚未执行的队头操作），
  与默认轮转各自独立运行 RU / RC / SI，按「事务名 + 事务内操作序号」对齐 READ / COMMIT / ABORT，
  依默认轮转的步序标出读值、来源类型、写入者与结束状态的**首次差异**（无差异也会明确标示），
  并并排展示两侧的脏读清单与依赖图成环情况；页面可切换回看两侧逐步轨迹；
- 内置四个一键加载示例：脏读、丢失更新（对比 RC 提交成环与 SI 回滚）、写偏序（SI 下双双提交但 RW 成环）、同键交错提交（见下）。

## 交错示例：同键先写者晚提交（RC 按提交顺序取版本）

初始 `x=0`（另设 `a=b=c=0` 仅用于拉开调度），三条事务经轮转调度得到如下交错——
**T1 先写 `x=1`，却晚于写 `x=2` 的 T2 提交；T3 在两次提交都完成之后读 x**：

| 步 | 操作 | 说明 |
|---|---|---|
| 1–3 | T1 / T2 / T3 `BEGIN` | 三个事务先后开始 |
| 4 | T1 `WRITE x 1` | 版本链上先生成 v(x=1)，未提交 |
| 5 | T2 `WRITE x 2` | 版本链上再生成 v(x=2)，未提交（**版本链保持写入顺序**） |
| 6–7 | T3 `READ b`；T1 `READ a` | 与 x 无关的填充操作 |
| 8 | T2 `COMMIT` | x=2 先成为已提交版本 |
| 9 | T3 `READ c` | 填充操作 |
| 10 | T1 `COMMIT` | x=1 **后提交**，成为按提交顺序最新的已提交版本 |
| 11 | T3 `READ x` | 三种隔离级别结果不同（见下） |
| 12 | T3 `COMMIT` | |

第 11 步 T3 读 x 的结果：

| 隔离级别 | 读值 | 来源 |
|---|---|---|
| **RU** | **2** | 继续取版本链上最新写入（此时为 T2 的 x=2） |
| **RC** | **1** | T1 的提交是最后一次提交——即使 v(x=1) 的版本编号更小、在链上更旧，它才是最新已提交版本 |
| **SI** | **0** | T3 读 BEGIN 快照；且 T1 写入的 x 在快照后已被 T2 提交，T1 于第 10 步提交时同键冲突，**整笔回滚**，x=1 作废 |

补充：第 8 步 T2 提交之前，任何事务在 RC 下读 x 都只能读到初始值 0
（不读未提交值，也不读自身未提交的写）；回滚产生的版本在三种级别下均不可读。

依赖图（只含已提交事务，回滚事务不参与）：

- RC / RU：`T1 → T3`（或 RU 下的 `T2 → T3`）为 **WR**；`T1 → T2` 为 **WW**（真实写入顺序）；
  T3 读 x 时两个版本都**已经提交**，所以**不会**仅因 T2 的版本编号更大而多出 `T3 → T2` 的 RW 边；
- SI：T1 回滚不参与图；T3 读快照且没有后于其读的提交，无 WR / RW 边。

页面点击「**示例4 · 同键交错提交**」即可一键加载该场景。

## 严格两阶段锁（Strict 2PL）推演

RU/RC/SI 共用一份预先排好的轮转调度；S2PL 则不同——**锁冲突会真正改变执行顺序**。
推演按**轮次**进行：每轮按 T1～T3 顺序让每个未结束事务尝试一次自己的**队头操作**：

- `READ` 申请 S 锁（多个事务可同时持 S），`WRITE` 申请 X 锁（与任何他人锁冲突）；
  事务已持 S 后写同一键时申请 **S→X 升级**（若还有他人持 S 则被阻塞，可能形成“转换死锁”）；
- 锁**直到 COMMIT / ABORT 才统一释放**（严格 2PL）；
- 申请被拒时：在等待图上登记 `等待者 → 持锁阻塞者`，**队头操作保留（pc 不推进）**，
  本轮继续转试其他事务；持锁者结束后等待边解除，等待者**下一轮重试**同一操作；
- 每次阻塞后立即检测等待图：**成环即死锁**，回滚**环内编号最大**的事务
  （`max(Ti 中的 i)`）——其写入版本全部标记为已回滚（**后续任何读取都不可见**）、
  剩余操作跳过、持有的锁全部释放，环上其他事务随后重试。

### 死锁示例（页面「2PL示例1 · 两事务死锁」一键加载）

初始 `x=0, y=0`：

```
T1: BEGIN  WRITE x 1  WRITE y 1  COMMIT
T2: BEGIN  WRITE y 2  WRITE x 2  COMMIT
```

| 步 | 轮次 | 操作 | 锁 / 等待图 |
|---|---|---|---|
| 1–2 | 1 | T1、T2 `BEGIN` | — |
| 3 | 2 | T1 `WRITE x 1` | T1 持 x 的 X 锁，生成未提交 v3 |
| 4 | 2 | T2 `WRITE y 2` | T2 持 y 的 X 锁，生成未提交 v4 |
| 5 | 3 | T1 `WRITE y 1` **等待** | 被 T2 阻塞，等待边 T1→T2，队头保留 |
| 6 | 3 | T2 `WRITE x 2` **死锁** | 被 T1 阻塞后等待图成环 `T2 → T1 → T2`；回滚环内编号最大的 **T2**：v4 作废、跳过 `WRITE x 2` 与 `COMMIT`、释放 y |
| 7 | 4 | T1 重试 `WRITE y 1` | 锁已释放，写入 v5 |
| 8 | 5 | T1 `COMMIT` | v3、v5 转为已提交并释放 x、y |

最终状态 `x=1, y=1`（均为 T1 提交）；T2 写入的 `y=2` 已回滚，任何后续读取都不会见到它。
页面右侧同步展示**锁表**（每键 S/X 与持锁事务）、**等待图**（环上红色高亮）、
**死锁处理记录**与**最终状态**；步骤条上黄色为阻塞、红色为死锁回滚。

另两个一键示例：

- **2PL示例2 · 三事务死锁环**：`T1→T2→T3→T1` 三环等待，回滚编号最大的 T3，
  随后 T2 再等 T1 释放 x 后提交（演示一次推演中锁释放后的重试）；
- **2PL示例3 · 阻塞与锁释放重试**：T2 的 `READ x` 被持 X 锁的 T1 阻塞，
  T1 提交后 T2 重试读到**按提交顺序最新**的已提交值 11，再写 22。

## 事务脚本语法

每条脚本一行，仅允许以下操作（大小写不敏感）：

```
BEGIN
READ key
WRITE key value
COMMIT
ABORT
```

- 必须以 `BEGIN` 开头，以 `COMMIT` 或 `ABORT` 结尾，结束语句之后不能再有操作；
- 值支持整数、小数、`true` / `false` / `null` 与字符串；
- 初始键值每行一个 `key=value`；
- 勾选「启用 T3」可使用 3 条事务。

## 一键启动（Docker Compose）

```bash
docker compose up --build
```

启动后访问：

- **http://localhost:8000** —— 推演网页
- http://localhost:8000/api/health —— 健康检查

后台停止：`docker compose down`。

## 本地直接运行（可选，无需 Docker）

需要 Python 3.11+：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

然后访问 http://localhost:8000 。

## 接口

`POST /api/simulate`

```json
{
  "initial": "x=0\ny=0",
  "txns": [
    "BEGIN\nREAD x\nWRITE x 10\nCOMMIT",
    "BEGIN\nREAD x\nWRITE x 20\nCOMMIT"
  ]
}
```

返回三种隔离级别各自的 `steps`（含每步版本链快照、读值来源、事务状态）、
`dirty_reads` 与 `graph`（节点 / WR·WW·RW 边 / 环路径）。

### 严格两阶段锁接口

`POST /api/twopl` —— 请求体与 `/api/simulate` 相同（初始键值 + 2～3 条脚本），
返回 S2PL 下的实际执行序列：锁申请/升级/等待/释放、等待图成环与回滚、读值来源、
逐步版本链 / 锁表 / 等待边 / 事务状态，以及死锁记录与最终状态。

```bash
curl -s -X POST http://localhost:8000/api/twopl \
  -H "Content-Type: application/json" \
  -d '{
    "initial": "x=0\ny=0",
    "txns": [
      "BEGIN\nWRITE x 1\nWRITE y 1\nCOMMIT",
      "BEGIN\nWRITE y 2\nWRITE x 2\nCOMMIT"
    ]
  }'
```

响应要点（`result` 内）：

- `steps[]`：每个元素是一次尝试，含
  - `round`（轮次）、`kind`（`begin/read/write/wait/deadlock/commit/abort`）、
    `op/key/value`；
  - 锁信息 `lock_mode`（`S` / `X` / `upgrade` / `…-reentrant`）与
    `lock_action`（`acquire` / `upgrade` / `wait` / `release` / `reentrant`）；
  - 阻塞时 `blockers`（持锁阻塞者）、`blocker_key`，等待图前后快照
    `wait_edges_before/after`；
  - 死锁步 `cycle`（环路径）、`victim`（环内编号最大者）、
    `undone_vids`（撤销写入）、`skipped_ops`（跳过的剩余操作）；
  - 读步 `read_value/read_vid/read_writer/read_source`
    （`own` 自身写入 / `committed` 最新已提交 / `initial` 初始值 / `missing` 无此键）；
  - 每步后的 `chains_after`（版本链）、`locks_after`（锁表）、
    `wait_edges_after`（等待图）、`txns_after`（事务状态与持锁键）。
- `deadlocks[]`：每次死锁的 `seq / cycle / cycle_label / victim / undone_vids /
  released_keys / skipped_ops / detail`；
- `txn_final`：每事务最终状态与全部读/写记录；
- `final_state`：每个键按提交顺序最新的已提交版本（回滚写入不在其中）。

用 `jq` 只看执行概览：

```bash
curl -s -X POST http://localhost:8000/api/twopl \
  -H "Content-Type: application/json" \
  -d '{"initial":"x=0\ny=0","txns":[
       "BEGIN\nWRITE x 1\nWRITE y 1\nCOMMIT",
       "BEGIN\nWRITE y 2\nWRITE x 2\nCOMMIT"]}' \
  | jq '.result.steps[] | {seq, round, txn, kind, op: .op, key, blockers, victim, read: .read_value}'
```

### 调度对照接口

`POST /api/compare` —— 同一组脚本分别按**默认轮转**与**自定义顺序**各跑 RU / RC / SI 并对齐比较：

```bash
curl -s -X POST http://localhost:8000/api/compare \
  -H "Content-Type: application/json" \
  -d '{
    "initial": "x=0",
    "txns": [
      "BEGIN\nREAD x\nWRITE x 10\nCOMMIT",
      "BEGIN\nREAD x\nWRITE x 20\nCOMMIT"
    ],
    "order": "T2 T2 T2 T2 T1 T1 T1 T1"
  }'
```

- `order` 为 T1/T2/T3 组成的完整执行顺序（空格/逗号分隔的字符串，或数组
  `["T2","T2",...]`），每项取对应事务尚未执行的队头操作；
  每个事务出现的次数必须恰好等于其脚本操作数——出现未知事务或次数不符时返回 400；
- 响应中 `sides.default` / `sides.custom` 为两侧完整推演结果（结构同 `/api/simulate` 的
  `results`，含 `steps` / `dirty_reads` / `graph`），可直接回看两侧逐步轨迹；
- `compare.{RU,RC,SI}` 给出对照结论：按「事务名 + 事务内操作序号」对齐 READ / COMMIT / ABORT，
  依默认轮转的步序得到 `first_diff`（读值 / 来源类型 / 写入者 / 结束状态的首次差异，
  含两侧步号 `default_seq` / `custom_seq`）、全部 `diffs` 与 `identical` 标记，
  以及两侧脏读数量与依赖图成环情况的 `summary`；
  SI 因顺序变化回滚时，后续读值、版本链与依赖图均以各侧实际运行结果为准。
