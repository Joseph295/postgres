# PostgreSQL 源码动手实验手册

> 配套《PostgreSQL源码学习指南》使用：指南负责"懂原理"，本手册负责"亲手验证"。
> 每个实验的结构：**目标 → 前置条件 → 步骤（完整命令）→ 预期观察 → 原理解读（对照源码）→ 延伸思考**。
> 环境搭建见《阶段0-环境搭建实录》（本机已于 2026-07-18 完成搭建：debug 版装在 `~/pgdev`，数据目录 `~/pgdata`）。
> 实验按学习路线图的阶段排序，编号对应指南的章节。

## 目录

- 实验 1（指南第 2–4 章）：观察一条 SQL 的三棵树
- 实验 2（指南第 2–5 章）：lldb 单步走完 exec_simple_query 五阶段
- 实验 3（指南第 6/7 章）：pageinspect 直击堆页与 MVCC 版本链 ★已验证
- 实验 4（指南第 6 章）：lldb 在 heap_insert 断住一条 INSERT ★已验证
- 实验 5（指南第 8 章）：解剖 B-tree 索引页 / BRIN vs B-tree 对比
- 实验 6（指南第 9 章）：两会话观察锁等待与快照隔离
- 实验 7（指南第 9 章）：在 HeapTupleSatisfiesMVCC 里单步一次可见性判断
- 实验 8（指南第 10 章）：pg_waldump 解读 WAL + kill -9 崩溃恢复
- 实验 9（指南第 11 章）：观察 autovacuum 触发与死元组回收
- 实验 10（指南第 4 章）：手算 EXPLAIN 的 cost 公式
- 实验 11（指南第 16 章）：列相关性导致的行数估错与扩展统计修复
- 附录：调试技巧速查

---

## 实验 1 · 观察一条 SQL 的三棵树（解析树 → 查询树 → 计划树）

**目标**：亲眼看到 parser / rewriter / planner 各阶段的输出，建立"SQL 是树的变换"的直觉。
**前置**：服务器运行中。

**步骤**：

```sql
-- psql 中执行
SET client_min_messages = log;   -- 让 LOG 输出直接回显到 psql
SET debug_print_parse = on;      -- ② 语义分析后的 Query 树
SET debug_print_rewritten = on;  -- ③ 重写后的 Query 树
SET debug_print_plan = on;       -- ④ 优化后的 Plan 树
SET debug_pretty_print = on;     -- 缩进美化

CREATE TABLE emp(id int, name text, dept_id int);
CREATE VIEW v_emp AS SELECT id, name FROM emp WHERE dept_id = 1;

SELECT * FROM v_emp WHERE id > 10;
```

**预期观察**：
1. `parse tree` 里视图 `v_emp` 还是一个普通的 RangeTblEntry（relkind='v'）；
2. `rewritten parse tree` 里视图**消失了**，被替换成 emp 上的子查询，`dept_id = 1` 被并入——这就是重写器的视图展开（`rewriteHandler.c`）；
3. `plan tree` 里只剩 `SEQSCAN` on emp，两个条件合并成了 qual 列表——优化器把子查询拉平了（`pull_up_subqueries`）。

**原理解读**：三棵树的结构体定义分别在 `parsenodes.h`（RawStmt/Query）与 `plannodes.h`（PlannedStmt）。输出格式由 `nodes/outfuncs.c` 生成——你看到的 `:rtable`、`:jointree` 等字段名与源码结构体字段一一对应，读树 = 读结构体。

**延伸思考**：把视图改成带聚合的（`SELECT dept_id, count(*) FROM emp GROUP BY dept_id`），重写后还能被拉平吗？为什么？（提示：`is_simple_subquery()`，src/backend/optimizer/prep/prepjointree.c）

---

## 实验 2 · lldb 单步走完 exec_simple_query 五阶段

**目标**：在调试器里走完指南 6.4 节全景图的主干，把"地图"变成"走过的路"。
**前置**：已执行 `sudo DevToolsSecurity -enable`（否则 lldb 无法 attach，报错 "no such process"——这是权限问题，不是进程不存在）。

**步骤**：

```bash
# 窗口 1（psql）
~/pgdev/bin/psql postgres
postgres=# SELECT pg_backend_pid();   -- 记下 PID，例如 12345

# 窗口 2（lldb）
lldb -p 12345
(lldb) br set -n exec_simple_query
(lldb) br set -n pg_parse_query
(lldb) br set -n pg_analyze_and_rewrite_fixedparams
(lldb) br set -n standard_planner
(lldb) br set -n ExecutorRun
(lldb) c

# 回窗口 1 执行：
postgres=# SELECT * FROM emp WHERE id = 42;
```

**预期观察**：断点按 `exec_simple_query → pg_parse_query → pg_analyze_and_rewrite_fixedparams → standard_planner → ExecutorRun` 的顺序依次命中（每次 `c` 继续）。在各断点处：

```
(lldb) call pprint(parsetree_list)   # 在 exec_simple_query 里打印原始解析树
(lldb) call pprint(querytree_list)   # 语义分析后
(lldb) call pprint(plantree_list)    # 优化后
(lldb) bt 5                          # 随时看调用栈，对照指南 6.4 节
```

`pprint()` 的输出会出现在**服务器日志**（`~/pgdev/server.log`）里，`tail -f` 盯着看。

**原理解读**：五个断点正是 `exec_simple_query`（postgres.c:1030）源码里依次调用的五个函数。单步（`n`）走一遍函数体，你会看到教科书上的"SQL 处理流水线"就是这不到 200 行代码。

**延伸思考**：用扩展协议（如 JDBC/psycopg 的 prepared statement）发同一条 SQL，断点会停在 `exec_parse_message` / `exec_bind_message` / `exec_execute_message`——对照读这三个函数，理解 plancache 的 generic/custom plan 切换。

---

## 实验 3 · pageinspect 直击堆页与 MVCC 版本链 ★已在本机验证

**目标**：亲眼看到"UPDATE 不是原地更新"，看到 xmin/xmax/ctid 组成的版本链和 HOT 更新。

**步骤**：

```sql
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE TABLE t(id int, val text);
INSERT INTO t VALUES (42, 'hello');
INSERT INTO t VALUES (43, 'world');

SELECT lp, lp_off, t_xmin, t_xmax, t_ctid, t_data
FROM heap_page_items(get_raw_page('t', 0));

UPDATE t SET val = 'hello-v2' WHERE id = 42;

SELECT lp, t_xmin, t_xmax, t_ctid, t_infomask2
FROM heap_page_items(get_raw_page('t', 0));
```

**预期观察**（2026-07-18 本机实测输出）：

```
-- UPDATE 前
 lp | t_xmin | t_xmax | t_ctid
  1 |  696   |   0    | (0,1)      ← id=42 'hello'
  2 |  697   |   0    | (0,2)      ← id=43 'world'

-- UPDATE 后：出现第 3 个元组！
 lp | t_xmin | t_xmax | t_ctid
  1 |  696   |  698   | (0,3)      ← 旧版本：xmax=698 标记"被事务 698 删除"，ctid 指向新版本
  2 |  697   |   0    | (0,2)
  3 |  698   |   0    | (0,3)      ← 新版本：xmin=698；与旧版本同页 → HOT 更新
```

**原理解读**：
- `t_xmin`/`t_xmax` 就是 `htup_details.h` 里 HeapTupleHeaderData 的字段；UPDATE 的三步（标记旧、插入新、串链）在 `heap_update()`（heapam.c:3267）；
- 检查 `t_infomask2`：新旧版本会带 `HEAP_HOT_UPDATED`/`HEAP_ONLY_TUPLE` 标志位（值需要与 `htup_details.h` 的宏对照解码），证明这是 HOT 更新——没有产生新的索引项；
- 事务号 696/697/698 连续递增：`GetNewTransactionId()`（varsup.c）的全局计数器。

**延伸思考**：
1. 给 val 建索引后再 UPDATE val，还会是 HOT 吗？用 `pg_stat_user_tables` 的 `n_tup_hot_upd` 验证；
2. 执行 `VACUUM t;` 后再看页内容——旧版本的 lp 变成什么状态？（提示：`LP_UNUSED`/`LP_REDIRECT`，重定向行指针是 HOT 的另一半设计）

---

## 实验 4 · lldb 在 heap_insert 断住一条 INSERT ★已在本机验证

**目标**：阶段 0 的验收实验——证明环境可断点、并验证指南 6.4 节调用栈。

**方式 A：单用户模式（不需要 Developer mode，本机已验证）**：

```bash
echo "INSERT INTO t VALUES (43, 'world');" > /tmp/insert.sql
~/pgdev/bin/pg_ctl -D ~/pgdata stop        # 单用户模式需独占数据目录
lldb -b \
  -o "br set -n heap_insert" \
  -o "process launch -i /tmp/insert.sql" \
  -o "bt 8" \
  -o "p relation->rd_rel->relname" \
  -o "p tup->t_len" \
  -o "continue" \
  -- ~/pgdev/bin/postgres --single -D ~/pgdata postgres
~/pgdev/bin/pg_ctl -D ~/pgdata -l ~/pgdev/server.log start
```

**方式 B：attach 到正常 backend**（需 Developer mode）：同实验 2 的两窗口流程，断点换成 `heap_insert`。

**预期观察**（2026-07-18 本机实测）：断点命中 `heapam.c:2008`，调用栈：

```
frame #0: heap_insert                        heapam.c:2008   ← 停在 GetCurrentTransactionId()
frame #1: heapam_tuple_insert                heapam_handler.c:161
frame #2: table_tuple_insert  ← Table AM 层  tableam.h:1461
frame #3: ExecInsert                         nodeModifyTable.c:1276
frame #4: ExecModifyTable                    nodeModifyTable.c:4970
frame #5: ExecProcNodeFirst                  execProcnode.c:469
frame #6: ExecProcNode                       executor.h:327
frame #7: ExecutePlan (CMD_INSERT)           execMain.c:1773
现场取证: relation->rd_rel->relname = "t"，tup->t_len = 34（23 字节头 + 数据）
```

**原理解读**：
- 断点停在 2008 行而非函数首行 2005：那是第一条可执行语句 `GetCurrentTransactionId()`。PG 刻意在拿页锁**之前**取 XID——XID 分配可能触发子事务栈分配等复杂路径，不能持锁执行；
- frame #2 的 `table_tuple_insert` 是 Table AM 抽象层（`tableam.h`）：把这层换掉就能接入别的存储引擎，这就是 PG 12+ 存储可插拔的含义；
- 单用户模式没有 postmaster、没有并发，是调试启动/恢复流程的唯一手段。

---

## 实验 5 · 解剖 B-tree 索引页 / BRIN vs B-tree 对比

**目标**：看到 B-tree 的页级结构（high key、右兄弟指针），并体会 BRIN 对时序数据的性价比。

**步骤（B-tree 解剖）**：

```sql
CREATE TABLE nums(n int);
INSERT INTO nums SELECT generate_series(1, 100000);
CREATE INDEX nums_btree ON nums(n);

SELECT * FROM bt_metap('nums_btree');                    -- 元页：root 在哪
SELECT * FROM bt_page_stats('nums_btree', 1);            -- 某页统计：level、右兄弟 btpo_next
SELECT itemoffset, ctid, itemlen, data
FROM bt_page_items('nums_btree', 1) LIMIT 8;             -- 页内条目
```

**预期观察**：
- `bt_metap` 给出 root 页号与树高（10 万行 int 大约 2 层）；
- 叶页的第 1 个条目是 **high key**（本页键值上界，Lehman-Yao 算法的核心道具），从第 2 条开始才是真实指向堆的条目（ctid = 堆的块号,行号）；
- `btpo_next` 是右兄弟页号——沿它可以横向扫完整个叶层，这就是范围扫描的物理路径。

**步骤（BRIN 对比）**：

```sql
CREATE TABLE ts(id bigint, created_at timestamptz, payload text);
INSERT INTO ts SELECT i, now() + (i || ' seconds')::interval, repeat('x', 100)
FROM generate_series(1, 5000000) i;                      -- 约 700MB，时间列天然有序

CREATE INDEX ts_btree ON ts(created_at);
CREATE INDEX ts_brin ON ts USING brin(created_at);

SELECT relname, pg_size_pretty(pg_relation_size(oid))
FROM pg_class WHERE relname IN ('ts_btree', 'ts_brin');

EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM ts
WHERE created_at BETWEEN now() + interval '1000 s' AND now() + interval '2000 s';
-- 分别 SET enable_indexscan/enable_bitmapscan 强制走两种索引对比
```

**预期观察**：BRIN 比 B-tree 小 **2–3 个数量级**（KB vs 上百 MB），而对有序数据的范围查询性能同量级——因为 BRIN 只存每 128 页的 min/max（`access/brin/`），本质是 Parquet footer 统计的思路。

**延伸思考**：把数据乱序重插（`ORDER BY random()`），BRIN 还有效吗？用 `brin_page_items()` 看摘要区间如何劣化。

---

## 实验 6 · 两会话观察锁等待与快照隔离

**目标**：把 pg_locks、行锁"藏在元组里"、READ COMMITTED vs REPEATABLE READ 的差异一次看全。

**步骤**：

```sql
-- 会话 A
BEGIN;
UPDATE t SET val = 'A-touched' WHERE id = 42;   -- 不提交，持有行锁

-- 会话 B
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM t WHERE id = 42;                  -- ① 立刻返回旧值：读不被写阻塞！
UPDATE t SET val = 'B-touched' WHERE id = 42;   -- ② 挂住：等 A 的行锁

-- 会话 C（观察者）
SELECT pid, mode, locktype, granted FROM pg_locks WHERE NOT granted;
SELECT pid, wait_event_type, wait_event, state, query
FROM pg_stat_activity WHERE state <> 'idle';

-- 会话 A
COMMIT;
-- 观察会话 B：REPEATABLE READ 下报错
-- ERROR: could not serialize access due to concurrent update
```

**预期观察**：
1. B 的 SELECT 不等锁直接返回旧版本——MVCC 读写不互斥；
2. `pg_locks` 里 B 等待的是 `locktype = transactionid`（A 的**事务锁**），而不是某个"行锁"条目——印证指南第 9 章：行锁不进锁表，锁信息写在元组 xmax 里，等行锁 = 等持有者的 XID 事务结束；
3. `wait_event` 显示 `Lock: transactionid`——与 `waiteventset.c` / 锁管理器对应；
4. A 提交后，REPEATABLE READ 的 B 报串行化错误（如果 B 用默认 READ COMMITTED，则会**重新拿快照**看到 A 的新值继续更新——这个行为差异的实现在 `ExecUpdate` 的 EPQ 机制，`execMain.c` 的 EvalPlanQual）。

**延伸思考**：把两个会话都改成 `ISOLATION LEVEL SERIALIZABLE`，构造经典 write-skew（两个事务各读对方要写的行再写自己的行），观察 SSI 报 `could not serialize access due to read/write dependencies`——这是 `predicate.c` 在工作。

---

## 实验 7 · 在 HeapTupleSatisfiesMVCC 里单步一次可见性判断

**目标**：把 MVCC 可见性规则从"背诵"变成"目击"。
**前置**：Developer mode 已开启；表 t 里有实验 3 留下的版本链。

**步骤**：

```bash
# 窗口 1：psql，记下 pg_backend_pid()
# 窗口 2：
lldb -p <PID>
(lldb) br set -n HeapTupleSatisfiesMVCC
(lldb) c
# 窗口 1：
SELECT * FROM t;
```

每次命中时：

```
(lldb) p HeapTupleHeaderGetRawXmin(tup->t_data)   # 元组的 xmin
(lldb) p *snapshot                                 # 快照的 xmin/xmax/xip
(lldb) n     # 单步，观察走进哪个分支（hint bit 命中？查 CLOG？）
(lldb) finish  # 看返回值 true/false
```

**预期观察**：对同一页的 3 个元组（版本链），函数分别返回 可见/可见/不可见（取决于你的快照与 696/698 事务的关系）；第二次扫描时路径明显变短——hint bit（`HEAP_XMIN_COMMITTED`）已在第一次判断时被写回元组头，不再查 CLOG。

**原理解读**：函数体（heapam_visibility.c:939）就是指南第 9 章那句判据的逐条实现："xmin 已提交且不在快照中 → 看得见诞生；xmax 未提交或不可见 → 看不见死亡"。配合 `TransactionIdIsCurrentTransactionId`、`XidInMVCCSnapshot` 两个子函数阅读。

**延伸思考**：为什么 PG 大量**只读**负载也会产生写 I/O？（hint bit 首次设置弄脏页面）`VACUUM` 与 checksum 如何影响 hint bit 的持久化？

---

## 实验 8 · pg_waldump 解读 WAL + kill -9 崩溃恢复

**目标**：读懂 WAL 记录的物理内容；亲历一次 ARIES 式（redo-only）崩溃恢复。

**步骤（WAL 解读）**：

```sql
SELECT pg_current_wal_lsn();          -- 记下起点，如 0/1830000
INSERT INTO t VALUES (99, 'wal-demo');
UPDATE t SET val = 'wal-demo-2' WHERE id = 99;
SELECT pg_current_wal_lsn();          -- 终点
```

```bash
~/pgdev/bin/pg_waldump -p ~/pgdata/pg_wal -s 0/1830000 -e <终点LSN>
```

**预期观察**（逐条对应你刚做的操作）：

```
rmgr: Heap  ... desc: INSERT off: N ...   blkref #0: rel .../base/5/xxxxx blk 0
rmgr: Transaction ... desc: COMMIT ...
rmgr: Heap  ... desc: HOT_UPDATE ...      ← UPDATE 打的是 HOT_UPDATE 记录
rmgr: Transaction ... desc: COMMIT ...
```

- 每条记录的 `rmgr`（资源管理器）对应 `rmgrlist.h` 的注册表，恢复时按它分发给 `heap_redo()` 等回调；
- 若这是 checkpoint 后第一次碰该页，INSERT 记录会带 `FPW`（full page write，整页镜像）——对比 InnoDB doublewrite 理解其防"页撕裂"的作用。

**步骤（崩溃恢复）**：

```bash
psql -c "INSERT INTO t VALUES (100, 'pre-crash'); "        # 已提交
kill -9 $(head -1 ~/pgdata/postmaster.pid)                  # 模拟断电
~/pgdev/bin/pg_ctl -D ~/pgdata -l ~/pgdev/server.log start
tail -20 ~/pgdev/server.log
psql -c "SELECT * FROM t WHERE id = 100;"                   # 数据还在！
```

**预期观察**：日志出现

```
LOG: database system was interrupted; ...
LOG: redo starts at 0/XXXXXXX          ← 从 pg_control 记录的 checkpoint 开始
LOG: redo done at 0/YYYYYYY
```

**原理解读**：恢复主循环在 `xlogrecovery.c` 的 `PerformWalRecovery()`；起点来自控制文件（`pg_controldata ~/pgdata` 可查看）。注意**没有 undo 阶段**——崩溃时未提交的事务，其修改可能已在页里，但 CLOG 中它们永远不是 committed，MVCC 可见性自动把它们过滤掉。这是 PG 与 ARIES/InnoDB 最大的分野（指南第 10 章）。

---

## 实验 9 · 观察 autovacuum 触发与死元组回收

**目标**：验证 autovacuum 的触发公式，理解 vacuum 三阶段与"空间不还操作系统"。

**步骤**：

```sql
ALTER SYSTEM SET log_autovacuum_min_duration = 0;  -- 记录所有 autovacuum
SELECT pg_reload_conf();

CREATE TABLE bloat_demo(id int, v text);
INSERT INTO bloat_demo SELECT i, repeat('x', 200) FROM generate_series(1, 100000) i;
SELECT pg_size_pretty(pg_table_size('bloat_demo'));         -- 记下初始大小

UPDATE bloat_demo SET v = v;                                 -- 全表更新 = 制造 10 万死元组
SELECT n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables WHERE relname = 'bloat_demo';

-- 等待约 1 分钟（autovacuum_naptime 默认 60s），重查上面的视图 + 看日志
SELECT pg_size_pretty(pg_table_size('bloat_demo'));         -- 大小变了吗？
```

**预期观察**：
1. UPDATE 后 `n_dead_tup ≈ 100000`，超过阈值 `50 + 0.2 × n_live_tup`，约一分钟内 autovacuum 启动；
2. 日志给出三阶段明细：`pages: ... removed`、`tuples: ... removed`、`index scan ...`；
3. **表大小不缩**（约翻倍后维持）——vacuum 只把空间标记为可复用，不还给操作系统；再做一轮同样的 UPDATE，表不再增长，因为复用了空洞。

**原理解读**：触发判断在 `autovacuum.c` 的 `relation_needs_vacanalyze()`；回收主体是 `vacuumlazy.c` 三阶段（扫堆收集死 TID → 扫索引删条目 → 回扫堆标记复用）。想把空间真正还回去要 `VACUUM FULL`（重写全表，`cluster.c`，需要排他锁）。

**延伸思考**：开一个 `BEGIN; SELECT 1;` 不提交的"长事务"再重复本实验——autovacuum 跑了但 `n_dead_tup` 不降。为什么？（`GetSnapshotData` 计算的全局 xmin 卡住了 `OldestXmin`，死元组"还可能被人看见"，不能删——这就是长事务导致膨胀的机制。）

---

## 实验 10 · 手算 EXPLAIN 的 cost 公式

**目标**：让优化器的 cost 数字从黑盒变成白盒。

**步骤**：

```sql
CREATE TABLE cost_demo(id int, v text);
INSERT INTO cost_demo SELECT i, 'x' FROM generate_series(1, 100000) i;
VACUUM ANALYZE cost_demo;

SELECT relpages, reltuples FROM pg_class WHERE relname = 'cost_demo';
-- 假设得到 relpages = P, reltuples = N

EXPLAIN SELECT * FROM cost_demo;
-- Seq Scan on cost_demo (cost=0.00..C rows=N width=W)
```

**验证公式**（`costsize.c` 的 `cost_seqscan()`）：

```
C = P × seq_page_cost + N × cpu_tuple_cost
  = P × 1.0 + N × 0.01
```

再加一个 WHERE：

```sql
EXPLAIN SELECT * FROM cost_demo WHERE id < 500;
-- total cost 变为: P × 1.0 + N × (0.01 + 0.0025)      ← 每行多一次 qual 求值 cpu_operator_cost
-- rows 变为: N × 选择率，选择率来自 pg_stats 直方图
SELECT histogram_bounds FROM pg_stats WHERE tablename = 'cost_demo' AND attname = 'id';
```

**预期观察**：手算值与 EXPLAIN 输出**精确一致**（误差只来自 reltuples 的估计值）。`rows=500` 左右——`scalarineqsel()`（selfuncs.c）在直方图 bucket 里线性插值的结果。

**延伸思考**：`SET seq_page_cost = 10;` 后哪些计划会翻转成索引扫描？为什么 SSD 环境普遍建议把 `random_page_cost` 从 4.0 调到 1.1？（cost 模型的两个 I/O 参数只在**相对比值**上有意义。）

---

## 实验 11 · 列相关性导致的行数估错与扩展统计修复

**目标**：亲手制造优化器最经典的翻车现场，再用 CREATE STATISTICS 修好——阶段 3 的核心实验。

**步骤**：

```sql
CREATE TABLE city(city_id int, state text, city_name text);
INSERT INTO city SELECT i, 'CA', 'SanFrancisco' FROM generate_series(1, 50000) i;
INSERT INTO city SELECT i, 'NY', 'NewYork'      FROM generate_series(50001, 100000) i;
ANALYZE city;

EXPLAIN ANALYZE SELECT * FROM city WHERE state = 'CA' AND city_name = 'SanFrancisco';
```

**预期观察**：实际 50000 行，但估计 `rows≈25000`——优化器假设两列独立：P(CA)×P(SF) = 0.5×0.5 = 0.25。而现实中 state 与 city_name 完全相关。在真实系统里，这种低估传导到上层 join 会引发灾难性的计划选择（该 Hash 的选了 NestLoop）。

**修复**：

```sql
CREATE STATISTICS city_dep (dependencies) ON state, city_name FROM city;
ANALYZE city;
EXPLAIN ANALYZE SELECT * FROM city WHERE state = 'CA' AND city_name = 'SanFrancisco';
-- rows 恢复到 ≈50000
SELECT stxdndistinct, stxddependencies FROM pg_statistic_ext_data ...;  -- 看函数依赖度
```

**原理解读**：独立性假设写在 `clauselist_selectivity()`（`optimizer/path/clausesel.c`）——默认就是把各条件选择率相乘；扩展统计的函数依赖修正在 `statistics/dependencies.c`。这一对文件是"优化器为什么估错/怎么修"的答案之书。

---

## 附录 · 调试技巧速查

```bash
# ── 编译与服务 ──────────────────────────────────────
ninja -C build install                # 改代码后增量重编（秒级）
meson test -C build                   # 全量回归测试
~/pgdev/bin/pg_ctl -D ~/pgdata start|stop|restart -l ~/pgdev/server.log

# ── lldb ──────────────────────────────────────────
lldb -p <PID>                         # attach（需 sudo DevToolsSecurity -enable）
br set -n <函数名>                     # 下断点
call pprint(任意Node指针)              # 打印节点树到服务器日志 ★最重要技巧
p *(HeapTupleHeader)tup->t_data       # 强转打印结构体
finish                                # 跑完当前函数看返回值
wa set v 变量名                        # 观察点：谁改了这个变量

# ── 服务器侧观察 ────────────────────────────────────
SET client_min_messages = log;        # LOG 回显到 psql
SET debug_print_parse/rewritten/plan = on;
SET trace_sort = on;                  # 排序落盘过程
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, WAL)  -- WAL 选项显示产生的 WAL 量

# ── 物理观察工具 ────────────────────────────────────
pg_waldump -p ~/pgdata/pg_wal -s <LSN>     # 读 WAL
pg_controldata ~/pgdata                     # 控制文件（checkpoint 位置等）
pageinspect / pg_buffercache / pg_stat_statements   # 页 / 缓冲池 / 语句统计
SELECT pg_relation_filepath('t');           # 表对应的物理文件路径

# ── 源码考古 ────────────────────────────────────────
git log -S'关键字' --oneline          # 找引入某段代码的 commit
git log -p --follow -- 文件路径       # 单文件全历史
# commit message 里通常有 Discussion: 链接指向邮件列表原始讨论
```
