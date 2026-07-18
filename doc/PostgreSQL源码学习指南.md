# PostgreSQL 源码深度学习指南
## —— 从大数据工程师 / MySQL 用户到数据库内核专家

> 本资料基于你本地的 PostgreSQL 源码树（`/Users/xujunhong/postgres`，版本 **20devel**，即社区 master 开发分支）撰写。
> 文中所有 `文件:行号` 引用都已在这份源码中核实过，可以直接打开对照阅读。
> 结构：**第一部分 · 执行路径**（一条 SQL 的一生）→ **第二部分 · 模块分析**（每个子系统的组成、职责与实现）→ **第三部分 · 学习路线图**（从现在到顶级专家的分阶段计划）。
>
> **配套资料**：本指南是"地图"。每章的逐段/逐行源码走读见 `doc/源码深入分析/`（15 篇，总索引在其 `00-README.md`）；可照做的实操步骤见 `doc/PostgreSQL动手实验手册.md`；环境搭建见 `doc/阶段0-环境搭建实录.md`。

---

# 第 0 章 · 起点：把你已有的知识映射到 PostgreSQL

在进入源码之前，先建立"翻译表"。你有 10 年大数据经验 + MySQL 使用经验，这两者都能大幅降低学习成本，但也各自埋着"惯性思维陷阱"。

## 0.1 从大数据体系映射

| 你熟悉的概念 | PostgreSQL 里的对应物 | 差异要点 |
|---|---|---|
| Spark 的 Logical Plan → Optimized Plan → Physical Plan | `Query`（查询树）→ `Path`（路径）→ `Plan`（计划树） | PG 的优化器是 **基于代价（cost-based）的 System R 风格动态规划**，而非 Spark Catalyst 那种规则驱动为主 |
| Volcano/Iterator 执行模型（Spark whole-stage codegen 之前的模型） | Executor 的 `ExecProcNode` 递归拉取（pull-based） | 完全同源。PG 是 Volcano 模型最经典的工业实现；PG 的 JIT 表达式编译对应 codegen 思想 |
| HDFS Block / Parquet Row Group | 8KB 的 Page（`src/include/storage/bufpage.h`） | PG 是**行存 + 原地页管理**，页是缓冲、WAL、锁的基本单位 |
| Kafka 的日志 + 消费者位点 | WAL + LSN（`XLogRecPtr`） | WAL 就是数据库界的"the log is the database"，复制、恢复、逻辑解码全靠它 |
| Flink 的 Checkpoint | PG 的 Checkpoint（`checkpointer.c`） | 都是为了截断日志、缩短恢复时间，但 PG 的是物理脏页刷盘而非算子状态快照 |
| 分布式系统里的 epoch / watermark | 事务 ID（XID）与快照（Snapshot） | MVCC 可见性判断的本质就是"我这个快照能看到哪些 XID 的写入" |

## 0.2 从 MySQL（InnoDB）映射 —— 更重要，也更容易踩坑

| 主题 | MySQL / InnoDB | PostgreSQL | 源码印证 |
|---|---|---|---|
| 进程模型 | 单进程多线程 | **多进程**：每个连接一个 backend 进程 | `src/backend/postmaster/postmaster.c` |
| 表的组织 | **聚簇索引组织表**（主键即数据） | **堆表（heap）**，所有索引都是二级索引，指向 `(块号,行号)` 即 TID | `src/backend/access/heap/heapam.c` |
| MVCC 实现 | Undo log：旧版本存在回滚段，行上有回滚指针 | **多版本直接存在堆里**：UPDATE = 插入新版本 + 旧版本打标记 | `heapam.c` 的 `heap_update()`（heapam.c:3267） |
| 旧版本回收 | Purge 线程清理 undo | **VACUUM** 清理堆里的死元组 | `src/backend/access/heap/vacuumlazy.c` |
| 崩溃恢复 | Redo log + Undo log（ARIES 风格） | **只有 Redo（WAL），没有 Undo** —— 因为旧版本就在堆里，回滚只需把 XID 标记为 aborted | `src/backend/access/transam/xlog.c` |
| 隔离级别默认 | REPEATABLE READ（间隙锁） | READ COMMITTED；SERIALIZABLE 用 **SSI**（可串行化快照隔离，不是锁） | `src/backend/storage/lmgr/predicate.c` |
| 复制 | binlog（逻辑/row 格式） | 物理 WAL 流复制为主，逻辑复制建立在 WAL 解码之上 | `src/backend/replication/` |
| 解析器 | 手写递归下降 | **Flex + Bison 生成** | `src/backend/parser/scan.l` / `gram.y` |

> **三个最重要的思维转换**：
> 1. **"UPDATE 不是原地更新"** —— PG 的 UPDATE 是 insert 新版本 + delete 旧版本，这解释了 PG 世界里一半的运维现象（表膨胀、VACUUM、HOT、fillfactor）。
> 2. **"回滚是免费的"** —— PG 回滚只是在 CLOG 里把事务标记成 aborted，不需要像 InnoDB 那样应用 undo。代价转移到了以后的 VACUUM。
> 3. **"进程而非线程"** —— 所有跨连接共享的状态必须显式放进共享内存（shared memory），这决定了锁、缓冲池、快照的实现方式。

## 0.3 源码树总览

```
src/
├── backend/            # 服务端全部核心代码（本资料的主战场）
│   ├── main/           # 进程入口 main()
│   ├── postmaster/     # 守护进程、连接派生、后台进程管理
│   ├── tcop/           # Traffic Cop：命令主循环，SQL 生命周期的"总调度"
│   ├── libpq/          # 服务端的前后端协议实现（认证、收发报文）
│   ├── parser/         # 词法/语法/语义分析
│   ├── rewrite/        # 规则系统与视图重写
│   ├── optimizer/      # 查询优化器（path/ plan/ prep/ geqo/ util/）
│   ├── executor/       # 执行器（Volcano 模型，每种算子一个 nodeXxx.c）
│   ├── access/         # 访问方法：heap/ nbtree/ gin/ gist/ brin/ transam/ ...
│   ├── storage/        # 存储管理：buffer/ smgr/ lmgr/ ipc/ freespace/ ...
│   ├── catalog/        # 系统目录（pg_class、pg_attribute 等的定义与维护）
│   ├── commands/       # DDL 与工具命令实现（CREATE TABLE、VACUUM、COPY…）
│   ├── replication/    # 物理复制 + logical/ 逻辑复制
│   ├── utils/          # cache/（relcache/syscache）、mmgr/（内存上下文）、adt/（内建类型）...
│   ├── nodes/          # 节点系统：所有树结构的定义、拷贝、序列化
│   └── jit/            # LLVM JIT 表达式编译
├── include/            # 头文件（与 backend 目录一一对应，看结构体先来这里）
├── common/ · port/     # 前后端共享代码、平台移植层
├── fe_utils/ · bin/    # 客户端工具（psql、pg_dump、pg_waldump…）
└── test/               # 回归测试（regress/）、隔离测试（isolation/）、TAP 测试
```

**读源码的黄金入口**：PostgreSQL 源码里散布着 60+ 个 `README` 文件，质量极高，是官方内核文档。第一批必读：
- `src/backend/access/transam/README`（事务系统总纲）
- `src/backend/storage/buffer/README`（缓冲管理）
- `src/backend/access/nbtree/README`（B-tree，Lehman & Yao 算法）
- `src/backend/optimizer/README`（优化器全景）
- `src/backend/storage/lmgr/README`（锁）与 `README-SSI`（可串行化）

---

# 第一部分 · 执行路径：一条 SQL 的一生

这一部分回答你的第一个问题：**数据库是怎么处理一个请求的**。我们跟着一条 `SELECT * FROM t WHERE id = 42;` 从 TCP 报文一路走到磁盘页，再看 INSERT/UPDATE 和 COMMIT 的写路径。

## 第 1 章 · 进程模型：请求到达之前

### 1.1 理论背景
数据库教科书（如 Hellerstein 的《Architecture of a Database System》）把 DBMS 进程模型分为三类：process-per-connection、thread-per-connection、进程/线程池。PG 选择了最古老也最稳健的 **process-per-connection**：地址空间隔离让一个连接崩溃很难污染别人，代价是连接昂贵（所以生产必配 pgbouncer 连接池——对应你在 MySQL 世界的 ProxySQL）。

### 1.2 源码路径

```
main()                                  src/backend/main/main.c:71
 └─ PostmasterMain()                    src/backend/postmaster/postmaster.c:497
     ├─ 读取 postgresql.conf、建立监听 socket
     ├─ reset_shared() → 创建共享内存与信号量（缓冲池、锁表、ProcArray 都在这里分配）
     ├─ 启动后台进程（checkpointer、bgwriter、walwriter、autovacuum launcher…）
     └─ ServerLoop()                    无限循环 select()/accept()
         └─ BackendStartup()           每来一个连接
             └─ postmaster_child_launch()   src/backend/postmaster/launch_backend.c
                 └─ fork() 出子进程 → 子进程进入 PostgresMain()
```

- **postmaster 永远不碰用户数据**，它只做三件事：监听、fork、给崩溃的子进程收尸（子进程异常退出会触发全库 crash restart，因为共享内存可能已被污染——这是多进程模型的自我保护）。
- fork 之后子进程**天然继承**共享内存映射和监听 socket，这就是 PG 不需要复杂线程同步启动逻辑的原因。
- 认证发生在子进程里：`InitPostgres()`（`src/backend/utils/init/postinit.c`）完成用户认证、加载数据库、初始化各种缓存。

### 1.3 前后端协议一瞥
PG 使用自己的报文协议（libpq protocol 3.x，见文档 protocol.sgml）。两类查询模式：
- **简单查询**：一个 `'Q'` 报文带完整 SQL 字符串 → 走 `exec_simple_query()`；
- **扩展查询**：`Parse('P') / Bind('B') / Execute('E')` 三段式，支持预编译语句和参数绑定 → 走 `exec_parse_message()` 等，对应 MySQL 的 prepared statement 协议。

## 第 2 章 · 命令主循环：tcop（Traffic Cop）

每个 backend 进程的一生都在这个循环里度过：

```c
/* src/backend/tcop/postgres.c:4364 */
PostgresMain(const char *dbname, const char *username)
    for (;;)
    {
        /* 1. 发送 ReadyForQuery，告诉客户端"我准备好了" */
        /* 2. ReadCommand() —— 阻塞读取下一个协议报文 */
        firstchar = ReadCommand(&input_message);
        switch (firstchar)
        {
            case PqMsg_Query:            /* 'Q' 简单查询 */
                exec_simple_query(query_string);   /* postgres.c:1030 */
            case PqMsg_Parse: ...        /* 'P' 扩展协议 */
            ...
        }
    }
```

`exec_simple_query()`（**postgres.c:1030**）是整条执行路径的"总目录"，五个阶段一目了然：

```c
exec_simple_query(const char *query_string)
{
    start_xact_command();                          /* ① 确保处于事务中 */
    parsetree_list = pg_parse_query(query_string); /* ② 词法+语法分析 → 原始解析树 */
    foreach(parsetree)
    {
        querytree_list = pg_analyze_and_rewrite_fixedparams(...);
                                                   /* ③ 语义分析 + 规则重写 → Query */
        plantree_list = pg_plan_queries(...);      /* ④ 优化 → PlannedStmt */
        portal = CreatePortal(...);                /* ⑤ 创建 Portal（游标抽象） */
        PortalRun(portal, ...);                    /*    执行并把结果发给客户端 */
    }
    finish_xact_command();                         /* 隐式提交（如果不在显式事务里） */
}
```

> **与 MySQL 对照**：这五步等价于 MySQL 的 `dispatch_command → parse → resolve → optimize → execute`，但 PG 的分层切得更干净——每一步的输入输出都是定义在 `src/include/nodes/` 里的不可变树结构，这是 PG 代码可读性远超 MySQL 的关键原因。

**贯穿始终的关键设计——节点系统（Node System）**：PG 中所有树（解析树、查询树、计划树、执行状态树）的节点都以 `NodeTag` 开头（`src/include/nodes/nodes.h`），配套自动生成的 `copyfuncs/equalfuncs/outfuncs/readfuncs`，相当于 C 语言里手工实现了一套"带反射的代数数据类型"。调试时用 `pprint(任意节点指针)` 可以在日志里打印整棵树——这是读执行路径源码的第一神器。

## 第 3 章 · 解析器：从字符串到 Query 树

### 3.1 理论背景
解析分两层：**语法分析**产出与语义无关的"原始解析树"（raw parse tree），**语义分析**（教科书叫 semantic analysis / binding）查系统目录，把名字解析成 OID，产出规范化的逻辑查询表示 `Query` —— 它本质上就是**关系代数表达式的一种具体语法树**：rtable 是关系集合，jointree 是选择+连接，targetList 是投影。

### 3.2 源码路径

```
pg_parse_query()                        postgres.c:617
 └─ raw_parser()                        src/backend/parser/parser.c
     ├─ 词法：scan.l   (Flex)  → core_yylex()，处理关键字、字面量、注释
     └─ 语法：gram.y   (Bison) → 约 6 万行语法规则，产出 SelectStmt 等原始树
pg_analyze_and_rewrite_fixedparams()    postgres.c:683
 └─ parse_analyze_fixedparams()         src/backend/parser/analyze.c
     └─ transformStmt() → transformSelectStmt()
         ├─ transformFromClause()   查 relcache/syscache，表名 → OID，建 RangeTblEntry
         ├─ transformWhereClause()  表达式类型推导、隐式类型转换、函数/操作符重载消解
         │                          （核心在 parse_expr.c / parse_oper.c / parse_func.c）
         └─ transformTargetList()   处理 SELECT 列表、`*` 展开
         → 产出 Query（src/include/nodes/parsenodes.h，PG 最重要的结构体之一）
```

**必须精读的结构体**：`Query`。它的字段就是 SQL 语义的完整清单：`rtable`（范围表）、`jointree`（FROM/WHERE）、`targetList`（投影）、`groupClause`、`havingQual`、`sortClause`、`cteList`……后续优化器的一切工作都是对这个结构体的变换。

### 3.3 重写器：视图与规则

```
pg_rewrite_query() → QueryRewrite()     src/backend/rewrite/rewriteHandler.c
```

理论上这一步做的是**视图展开（view inlining）**：视图在 PG 里就是"一条 `ON SELECT DO INSTEAD` 规则"，重写器把引用视图的 RangeTblEntry 替换为视图定义的子查询。这是 Stonebraker 规则系统（Rule System）的遗产——比触发器更早的声明式机制。行级安全策略（RLS）也在这一步注入过滤条件。

## 第 4 章 · 优化器：从 Query 到 PlannedStmt

这是 PG 最有理论深度的模块，直接继承 **System R（Selinger 1979）** 的动态规划框架。先读 `src/backend/optimizer/README`。

### 4.1 两个核心概念：Path 与 RelOptInfo
- `RelOptInfo`：优化过程中"一个（或一组）关系"的抽象，挂着它的所有候选访问路径；
- `Path`：轻量的"计划骨架"，只带代价（`startup_cost`/`total_cost`）和输出行数估计，不带执行细节。**先在 Path 空间里搜索，选出最优后才展开成重量级的 Plan** —— 这是典型的"先剪枝后物化"工程设计。

### 4.2 源码路径

```
pg_plan_queries()                       postgres.c:988
 └─ pg_plan_query() → planner()
     └─ standard_planner()              src/backend/optimizer/plan/planner.c:346
         └─ subquery_planner()          递归处理子查询；先做一系列"预处理式改写"：
             ├─ pull_up_sublinks()      IN/EXISTS 子链接 → 半连接（semi-join）
             ├─ pull_up_subqueries()    子查询上拉（展平）
             ├─ preprocess_expression() 常量折叠、SQL 函数内联
             └─ grouping_planner()      处理 GROUP BY/聚合/排序/LIMIT 等"上层"操作
                 └─ query_planner()     src/backend/optimizer/plan/planmain.c
                     │                  —— 核心：扫描/连接搜索 ——
                     └─ make_one_rel()  src/backend/optimizer/path/allpaths.c
                         ├─ set_base_rel_pathlists()  为每个基表生成候选路径：
                         │    SeqScan / IndexScan / IndexOnlyScan / BitmapHeapScan…
                         │    代价计算在 costsize.c，选择率估计在 utils/adt/selfuncs.c
                         └─ make_rel_from_joinlist()
                             ├─ ≤ geqo_threshold(12) 个表：standard_join_search()
                             │    System R 动态规划：先算所有 1 表最优，再枚举 2 表、
                             │    3 表…每层保留"每种有用排序(PathKeys)下的最优路径"
                             └─ 表太多：geqo() 遗传算法（optimizer/geqo/）
         ├─ create_plan()               src/backend/optimizer/plan/createplan.c
         │                              最优 Path → Plan 树（SeqScan/NestLoop/HashJoin…节点）
         └─ set_plan_references()       src/backend/optimizer/plan/setrefs.c
                                        变量编号"扁平化"，Var 改指下层算子输出槽位
         → 产出 PlannedStmt（src/include/nodes/plannodes.h）
```

### 4.3 必须掌握的理论 ↔ 源码对应

| 理论 | 源码落点 |
|---|---|
| 代价模型（CPU + I/O 加权） | `costsize.c`：`seq_page_cost`、`random_page_cost` 等 GUC 如何进入公式 |
| 选择率估计（直方图、MCV） | `selfuncs.c`：`eqsel()`、`scalarineqsel()` 读取 `pg_statistic` |
| System R 有趣顺序（interesting orders） | `PathKeys` 机制（`pathkeys.c`）——保留有排序价值的次优路径 |
| 连接搜索空间（left-deep vs bushy） | PG 枚举 **bushy tree**，`joinrels.c` 的层次枚举 |
| 随机化搜索 | `geqo/`：把连接排序当 TSP 用遗传算法求解 |

> **实验建议**：`EXPLAIN (ANALYZE, VERBOSE, COSTS)` 的每一个数字都能在 `costsize.c` 里找到出处。挑一条三表连接的查询，手工用公式算一遍 cost，再对照 EXPLAIN 输出——做完这个练习，你对优化器的理解会超过 95% 的 DBA。

## 第 5 章 · 执行器：Volcano 模型的教科书实现

### 5.1 理论背景
Graefe 的 **Volcano 迭代器模型**：每个算子实现 `open() / next() / close()` 接口，父节点向子节点"拉取"一行。优点是组合性极强——任意算子可以任意嵌套；缺点是每行一次函数调用开销大，这正是 PG 后来加入 JIT 和批量化（如 `HeapTupleSatisfiesMVCCBatch`）的动因。你从 Spark 的 whole-stage codegen 反推即可理解。

### 5.2 源码路径

```
PortalRun()                             src/backend/tcop/pquery.c:681
 └─ PortalRunSelect()
     └─ ExecutorRun()                   src/backend/executor/execMain.c:308
         └─ standard_ExecutorRun()      execMain.c:318
             └─ ExecutePlan()           循环：一次取一个 tuple，发给客户端
                 └─ ExecProcNode()      src/include/executor/executor.h（内联）
                     └─ node->ExecProcNode(node)   函数指针分发，递归下钻
```

三阶段生命周期（与 open/next/close 一一对应）：
- `ExecutorStart() → ExecInitNode()`（`execProcnode.c`）：按 Plan 树递归创建**执行状态树**（`PlanState` 树），初始化表达式、打开表、申请内存；
- `ExecutorRun() → ExecProcNode()`：拉取元组。以最简单的顺序扫描为例：
  ```
  ExecSeqScan()                nodeSeqscan.c
   └─ table_scan_getnextslot() 表访问方法接口（tableam.h）
       └─ heap_getnextslot()   heapam.c —— 逐页逐行读堆
           └─ heapgettup_pagemode() → ReadBuffer() 进入缓冲管理器（见第 8 章）
  ```
- `ExecutorEnd() → ExecEndNode()`：释放资源。

**算子清单**即 `src/backend/executor/` 下的 `nodeXxx.c` 文件列表：三种连接（`nodeNestloop.c`/`nodeMergejoin.c`/`nodeHashjoin.c`）、聚合（`nodeAgg.c`，含哈希聚合落盘）、排序（`nodeSort.c` → `tuplesort.c` 外排序，教科书的多路归并）、并行汇聚（`nodeGather.c`）等约 40 个。**每个文件独立可读**，是最好的"每天读一个"材料。

**表达式求值**是执行器里另一条暗线：`WHERE id = 42` 这类表达式在 `ExecInitExpr`（`execExpr.c`）中被编译成一串线性的操作码（steps），由 `ExecInterpExpr`（`execExprInterp.c`）用 computed-goto 解释执行——这就是一台小型虚拟机，JIT（第 17 章）做的事就是把这串操作码翻译成 LLVM IR。

## 第 6 章 · 写路径：INSERT / UPDATE / COMMIT 落到磁盘

读路径之外，写路径串起了堆、WAL、事务三大模块，是理解 PG 可靠性的钥匙。

### 6.1 INSERT 一行

```
ExecInsert()                            executor/nodeModifyTable.c
 └─ table_tuple_insert() → heap_insert()        heapam.c:2005
     ├─ GetCurrentTransactionId()       给元组打上 xmin = 我的 XID
     ├─ RelationGetBufferForTuple()     用 FSM（空闲空间地图）找到有空位的页
     ├─ 修改页内容（在缓冲区里，持有页的排他锁）
     ├─ XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT)    xloginsert.c:482
     │    → 只写 WAL 缓冲区，并把返回的 LSN 记到页头（page LSN）
     └─ 解锁。注意：**数据页此时并不刷盘**
```

两条铁律在这里体现：
- **WAL-before-data（先写日志）**：脏页刷盘前，必须保证其 page LSN 之前的 WAL 已落盘（bufmgr 刷页时检查，调 `XLogFlush`）；
- **提交时只刷 WAL**：数据页可以慢慢由 checkpointer/bgwriter 写出。

### 6.2 UPDATE 一行（PG 特色的关键）

`heap_update()`（heapam.c:3267）做的是：
1. 找到旧元组，把它的 `xmax` 设为当前 XID（"我删了它"）；
2. 在（尽量同一）页里插入新版本元组，`xmin` = 当前 XID；
3. 旧元组的 `ctid` 指向新版本，形成**版本链**；
4. 如果新旧版本同页且没有索引列变化 → **HOT 更新**（Heap-Only Tuple，见 `src/backend/access/heap/README.HOT`），不用插入新的索引项——这是 PG 对"写放大"最重要的优化。

对比 InnoDB：InnoDB 原地改行 + 旧值进 undo；PG 新旧两个版本都在堆里。**读者不需要重建旧版本（读快），写者留下垃圾（需要 VACUUM）** —— 一切 PG 运维特色皆源于此。

### 6.3 COMMIT

```
finish_xact_command() → CommitTransaction()     src/backend/access/transam/xact.c
 ├─ RecordTransactionCommit()
 │   ├─ XLogInsert(XLOG_XACT_COMMIT)    写一条提交记录
 │   └─ XLogFlush(recptr)               ★ 唯一必须同步等待磁盘的时刻
 │                                      （synchronous_commit=off 时可跳过等待）
 ├─ TransactionIdCommitTree()           在 CLOG（clog.c）里把 XID 标为 COMMITTED
 │                                      —— CLOG 是"每事务 2 个 bit"的巨大位图
 └─ ProcArrayEndTransaction()           从"运行中事务数组"移除自己
                                        —— 从这一刻起，别人的新快照认为我已提交
```

**回滚**（`AbortTransaction`）只是把 CLOG 标成 ABORTED——不碰任何数据页。这就是 0.2 节说的"回滚免费"。

### 6.4 全景图：一条 SQL 的完整调用栈

```
客户端 'Q' 报文
  └ PostgresMain (postgres.c:4364)                    [tcop]
     └ exec_simple_query (postgres.c:1030)
        ├ pg_parse_query ──── scan.l / gram.y         [parser]
        ├ pg_analyze_and_rewrite ─ analyze.c          [parser]
        │                        └ rewriteHandler.c   [rewrite]
        ├ pg_plan_queries ──── standard_planner       [optimizer]
        │                        (planner.c:346)
        └ PortalRun (pquery.c:681)                    [executor]
           └ ExecutorRun (execMain.c:308)
              └ ExecProcNode → nodeXxx.c
                 ├ 读: heapam.c ─ bufmgr.c ─ smgr/md.c ─ 文件   [access/storage]
                 ├ 写: heap_insert ─ XLogInsert                 [access/transam]
                 └ 可见性: HeapTupleSatisfiesMVCC (heapam_visibility.c:939)
        └ (提交) CommitTransaction ─ XLogFlush ─ CLOG  [transam]
```

把这张图打印出来贴在桌上。第二部分的每一章，都是对图中某一个方框的放大。

---

# 第二部分 · 模块深度分析

每章的结构固定为：**职责 → 理论背景 → 内部组成 → 实现要点（结合源码）→ 动手实验**。建议按章节顺序推进，因为后面的模块依赖前面的概念。

## 第 7 章 · 存储管理：页、缓冲池与物理文件

### 职责
把"表"这个逻辑概念落到操作系统文件，并在内存与磁盘之间搬运 8KB 的页。自底向上三层：

```
bufmgr (缓冲管理)  storage/buffer/     —— 共享缓冲池，页的缓存与淘汰
   ↓
smgr (存储管理器)  storage/smgr/       —— 抽象接口层
   ↓
md.c (magnetic disk)                   —— 具体实现：表 = 一组 1GB 的段文件
```

### 理论背景
教科书的 buffer pool：页表（哪个磁盘页在缓冲池哪个槽）+ 替换策略 + pin/unpin 引用计数 + 脏页写回策略（steal/no-force：PG 允许未提交脏页写盘、提交时不强制刷数据页——因此必须靠 WAL 保证正确性）。

### 内部组成与实现要点
1. **页布局**（`src/include/storage/bufpage.h`）：页头（含 **page LSN**、空闲空间指针）→ 行指针数组 `ItemId`（从前往后长）→ 元组数据（从后往前长）。行指针这层间接性是 VACUUM 能移动元组而索引 TID 不失效（同页内）的关键。
2. **缓冲池**（`bufmgr.c`）：
   - 定位页：共享哈希表 `buf_table.c`，键为 `(表文件, fork, 块号)`；
   - 读页入口 `ReadBufferExtended()`（**bufmgr.c:926**）→ 未命中则 `StartBufferIO` + `smgrread()`；
   - **淘汰策略是 clock-sweep**（`freelist.c`）：不是 LRU！每个 buffer 有 usage_count（上限 5），时钟指针扫过时递减，为 0 者被淘汰。选它而不选 LRU 的原因：LRU 需要全局链表+全局锁，clock-sweep 几乎无锁；
   - **环形缓冲（ring buffer）策略**：大顺序扫描 / VACUUM / COPY 只用一个小环（256KB 级），防止一次大扫描把整个缓冲池冲掉——对应你在大数据里熟悉的 scan-resistant cache 问题。
3. **fork 的概念**：每个表除了主数据文件（main fork），还有 **FSM**（free space map，`freespace/freespace.c`，一棵记录每页空闲空间的树，insert 找页就查它）和 **VM**（visibility map，`heap/visibilitymap.c`，每页 2 个 bit：全可见/全冻结——index-only scan 和 vacuum 跳页的依据）。

### 动手实验
装上 contrib 的 `pageinspect` 扩展，用 `heap_page_items(get_raw_page('t', 0))` 直接看页里每个元组的 xmin/xmax/ctid；做一次 UPDATE 再看，亲眼确认版本链。

## 第 8 章 · 访问方法：heap 与五种索引

### 职责
`src/backend/access/` 定义"数据以什么结构组织、怎么增删改查"。PG 把它做成了**双抽象接口**：
- **表访问方法**（Table AM，`src/include/access/tableam.h`）：PG 12 起表存储可插拔（heap 是默认实现；社区的 zheap、OrioleDB 都是替代实现）；
- **索引访问方法**（Index AM，`src/include/access/amapi.h`）：`ambuild/aminsert/ambeginscan/amgettuple...` 一组回调，五种内建索引都实现它。

### 各索引的理论与源码

| 索引 | 目录 | 理论根基 | 一句话本质 |
|---|---|---|---|
| **B-Tree** | `access/nbtree/` | **Lehman & Yao 高并发 B-link 树**（读 `nbtree/README`，社区最佳 README） | 每个节点有右兄弟指针 + high key，使得**读不阻塞写**：读时发现目标"跑到右边去了"就沿链追，无需锁耦合 |
| Hash | `access/hash/` | 线性哈希（hash/README 引用 Seltzer & Yigit 1991） | 只支持等值；PG 10 后才写 WAL |
| **GiST** | `access/gist/` | 广义搜索树（Hellerstein 1995） | "把 B-tree 的比较逻辑抽象成用户回调"的框架——几何、范围类型、全文检索都能建树 |
| SP-GiST | `access/spgist/` | 空间划分树（四叉树/基数树/kd 树） | 非平衡划分型索引框架 |
| **GIN** | `access/gin/` | 倒排索引 | 你最熟悉的结构：`(关键词 → posting list)`，JSONB/数组/全文检索用它；有 pending list 延迟合并（对应 LSM 的 memtable 直觉） |
| BRIN | `access/brin/` | 块范围摘要（类似 Parquet 的 min/max 统计） | 每 128 页存一个 min/max，适合天然有序的时序大表——大数据同学秒懂 |

**重点读 nbtree**：`_bt_search()`（**nbtsearch.c:100**）从根下降到叶的循环，配合 `_bt_moveright()` 处理并发分裂，是 Lehman-Yao 算法的直接实现；`nbtinsert.c` 的页分裂逻辑展示了 WAL 如何保证多页原子修改。

### 动手实验
`CREATE INDEX` 后用 pageinspect 的 `bt_page_items()` 看 B-tree 页内容；对一张 10GB 时序表分别建 btree 和 brin，对比大小与范围查询速度。

## 第 9 章 · 事务与 MVCC：PG 的灵魂模块

先通读 `src/backend/access/transam/README`。

### 理论背景
- **ACID 中的 I**：教科书给出两条路线——两阶段锁（2PL，MySQL SERIALIZABLE 的做法）与**多版本并发控制（MVCC）**。PG 是纯 MVCC：读永远不阻塞写，写永远不阻塞读。
- **快照隔离（SI）及其缺陷**：SI 不是可串行化（存在 write-skew 异常）。PG 9.1 用 **SSI**（Serializable Snapshot Isolation，Cahill 2008 论文的首个工业实现）修补——靠追踪读写依赖（rw-antidependency）形成"危险结构"时主动 abort，而非加锁。实现在 `storage/lmgr/predicate.c`（配 `README-SSI`）。

### 内部组成

```
access/transam/
├── xact.c          事务状态机：Start/Commit/Abort + 子事务（SAVEPOINT）
├── varsup.c        XID 分配（GetNewTransactionId）
├── clog.c          提交日志：每 XID 2bit（进行中/已提交/已中止/子事务）
├── subtrans.c      子事务 → 父事务映射
├── multixact.c     多个事务共享行锁时的"组合 XID"
└── procarray.c     [在 storage/ipc/] 运行中事务数组 —— 拍快照的地方
```

### 实现要点：可见性判断的完整链条

1. **元组上的版本信息**（`src/include/access/htup_details.h`）：每行头部有 `xmin`（创建者 XID）、`xmax`（删除者 XID）、`ctid`（版本链指针）和 infomask 提示位；
2. **拍快照**：`GetSnapshotData()`（**procarray.c:2114**）扫一遍 ProcArray，记录 `xmin`（最老运行事务）、`xmax`（下一个未分配 XID）、`xip[]`（正在运行的 XID 列表）。READ COMMITTED 每条语句拍一次，REPEATABLE READ 整个事务用第一张；
3. **判断可见**：`HeapTupleSatisfiesMVCC()`（**heapam_visibility.c:939**）：
   "元组的 xmin 已提交且不在我的快照运行列表里 → 我能看到它诞生；xmax 没提交或对我不可见 → 我看不到它死亡 → 该元组对我可见"。查"已提交与否"就去查 CLOG，并用 **hint bit** 把结果缓存在元组头上，避免反复查（这就是为什么 PG 大量读也会产生写 I/O——第一次读会写 hint bit）；
4. **XID 回卷问题**：XID 是 32 位循环计数器，比较用模运算"半圈"规则。老元组必须被 **freeze**（标记为"对所有人可见"），否则 40 亿事务后新旧颠倒——这就是 autovacuum 强制冻结和 `wraparound` 告警的来源。理解这一节，你就理解了 PG 运维中最凶险的故障模式。

### 锁体系（storage/lmgr/，配 README）
三层，各司其职：
- **spinlock**（`s_lock.c`）：几条指令级的临界区，硬件 TAS；
- **LWLock**（`lwlock.c`）：保护共享内存结构（如缓冲区头、WAL 缓冲），共享/排他两种模式，无死锁检测——所以内核代码必须按固定顺序拿；
- **重量级锁 / 常规锁**（`lock.c`）：保护数据库对象（表、行、页），8 个模式的冲突矩阵，有等待队列和**死锁检测**（`deadlock.c`：等待超过 `deadlock_timeout` 才触发，在等待图里找环——教科书 wait-for graph 算法的实现）。
- 行锁的巧妙之处：**不进锁表**，直接把 locker 的 XID 写进元组 `xmax`——千万行加锁也不会撑爆内存（对比 InnoDB 的锁内存结构）。等待行锁 = 等待持有者 XID 这把"事务锁"释放。

### 动手实验
开两个 psql：会话 A `BEGIN; UPDATE ...` 不提交，会话 B 查 `pg_locks`、`pg_stat_activity` 观察等待；再用 `SELECT xmin, xmax, ctid FROM t` 观察两个会话看到的版本差异。然后 gdb attach 到 backend，在 `HeapTupleSatisfiesMVCC` 下断点单步走一次可见性判断。

## 第 10 章 · WAL 与崩溃恢复

### 理论背景
对照经典 **ARIES**（Mohan 1992）三原则理解 PG 的取舍：
- WAL 先行 ✔（相同）；
- Redo 重演历史 ✔（相同，且 PG 的 redo 是**物理页级**重演）;
- Undo 阶段 ✘（**PG 没有 undo**——未提交事务的修改留在页里没关系，MVCC 可见性会把它们过滤掉，CLOG 里它们永远是 aborted）。
另一个 PG 特有设计：**full-page writes**——每个 checkpoint 后第一次改某页时，把整页写进 WAL，以防"页撕裂"（部分写）。InnoDB 解决同一问题用的是 doublewrite buffer，两相对照非常有启发。

### 内部组成

```
access/transam/
├── xlog.c           WAL 的心脏：WAL 缓冲区管理、XLogFlush、checkpoint 执行、LSN 推进
├── xloginsert.c     组装 WAL 记录（XLogBeginInsert/XLogRegisterBuffer/XLogInsert:482）
├── xlogreader.c     通用 WAL 读取器（恢复、复制、pg_waldump 共用）
├── xlogrecovery.c   崩溃/归档恢复主循环（PerformWalRecovery）
└── rmgrlist.h       资源管理器表：每类 WAL 记录由谁重放（heap_redo、btree_redo…）
```

### 实现要点
1. **WAL 记录的自描述性**：每条记录 = 头部（rmgr id + info）+ 涉及的块引用 + 数据。恢复时 `PerformWalRecovery()` 循环读记录，按 rmgr 分发给 `heap_redo()` 等回调。重放是**幂等**的：对比 page LSN 与记录 LSN，页已经比记录新就跳过；
2. **组提交**：`XLogFlush` 里多个等待提交的 backend 会合并成一次 fsync（对应 InnoDB 的 group commit）；
3. **Checkpoint**（`xlog.c` 的 `CreateCheckPoint` + `postmaster/checkpointer.c`）：把某时刻前所有脏页刷盘，然后在 WAL 里记录"从这里之前的 WAL 恢复时不需要了"。`checkpoint_completion_target` 控制刷盘节奏摊平 I/O；
4. **恢复起点**：控制文件 `pg_control`（`pg_controldata` 命令可看）记录最近 checkpoint 的位置——启动时 `StartupXLOG()` 从这里开始 redo 到 WAL 末尾。

### 动手实验
`pg_waldump` 是最好的老师：做一条 INSERT + COMMIT，然后 dump 出 WAL，逐条对照 `heap_insert` / `xact commit` 记录；再 `kill -9` 数据库，观察启动日志里的 redo 起止 LSN。

## 第 11 章 · VACUUM 与 autovacuum：为多版本付账

### 职责
回收死元组空间、更新 FSM/VM、冻结老 XID、更新统计信息回收阈值。这是 MVCC 选择"垃圾留在堆里"之后必然要付的账单，**没有对应的 MySQL 概念可以类比（purge 只是近似）**，必须当新知识学。

### 内部组成与实现要点
- **lazy vacuum**（`access/heap/vacuumlazy.c`）三阶段：
  ① 扫堆（借助 VM 跳过全可见页），收集死元组 TID；
  ② 扫所有索引删掉指向这些 TID 的项（这就是为什么索引多的表 vacuum 慢）；
  ③ 回扫堆，把死元组槽位标记可复用。注意：**普通 VACUUM 不还空间给操作系统**（只截断文件尾部空页），`VACUUM FULL` 才重写全表（`commands/cluster.c`）；
- **HOT prune**：普通查询路径上也会顺手清理同页版本链（`heap/pruneheap.c`）——"人人有责"的微型 vacuum；
- **freeze**：把足够老的 xmin 换成"冻结"标记（对所有快照可见），解除回卷风险；`vacuum_freeze_max_age` 触发强制的 aggressive vacuum；
- **autovacuum**（`postmaster/autovacuum.c`）：launcher + 动态 worker 的两级架构，按 `n_dead_tup > threshold + scale_factor * n_live_tup` 决定触发；`vacuum_cost_*` 参数实现 I/O 限流（token bucket 思想）。

### 动手实验
`SET log_autovacuum_min_duration = 0`，制造大量 UPDATE，观察 autovacuum 日志中每阶段耗时；用 `pg_stat_user_tables` 的 `n_dead_tup` 验证触发公式。

## 第 12 章 · 系统目录与缓存体系

### 职责
"数据库自举"问题：表的定义本身也存在表里（`pg_class`、`pg_attribute`、`pg_proc`、`pg_type`……）。`src/backend/catalog/` 定义并维护它们；`src/include/catalog/pg_*.h` + `.dat` 文件是初始内容（`initdb` 时由 `genbki.pl` 灌入）。

### 三层缓存（utils/cache/）——性能命脉
- **syscache**（`syscache.c`）：按键缓存单行目录元组（"给我 OID=16384 的 pg_class 行"）；
- **relcache**（`relcache.c`）：按表聚合的"表描述符" `Relation`——列定义、索引列表、触发器、分区信息全在里面。执行路径上拿到的 `Relation relation` 参数就是它；
- **plancache**（`utils/cache/plancache.c`）：预编译语句的计划缓存（generic vs custom plan 的五次尝试启发式就在这里）。

**缓存失效**（`inval.c`）是精华：DDL 提交时通过共享内存队列广播失效消息（`sinvaladt.c`），其他 backend 在下一个安全点处理。**这套机制 + 锁**回答了"为什么 PG 的 DDL 也是事务性的、可回滚的"——对比 MySQL 8.0 之前 DDL 不能回滚的历史，你会体会到目录事务化的价值。

## 第 13 章 · 内存管理：MemoryContext

### 职责与理论
数据库 C 代码最大的工程风险是内存泄漏与碎片。PG 的答案是**分层的区域分配器（region/arena allocator）**：所有 `palloc()` 都发生在"当前内存上下文"里，上下文可以整棵销毁——错误处理时（elog(ERROR) 的 longjmp）把事务上下文一键重置，就不会泄漏。

### 源码
- `utils/mmgr/mcxt.c`：上下文机制框架（树形层级：TopMemoryContext → ... → 每查询、每元组的上下文）；
- `utils/mmgr/aset.c`：默认实现 AllocSet——按 2 的幂分级的 freelist + 指数增长的块；
- `README`（`utils/mmgr/README`）解释了设计决策。
- 关键习惯用法：`per-tuple context` 每处理一行就 `MemoryContextReset`，所以执行器内层循环可以放心 palloc 不 pfree。

**这是读一切 PG 代码的前置知识**——不理解 CurrentMemoryContext 切换，看执行器代码会一头雾水。

## 第 14 章 · 进程间通信与后台进程

### 组成
- **共享内存**：启动时一次性 mmap 一大块（`storage/ipc/shmem.c`），缓冲池、锁表、ProcArray、WAL 缓冲都从中划分；PG 13+ 的动态共享内存（`dsm.c`）支撑并行查询；
- **等待/唤醒原语**：Latch + WaitEventSet（`storage/ipc/waiteventset.c`）——把"等 socket、等信号、等别的进程唤醒"统一成一个 epoll/kqueue 事件循环，`pg_stat_activity.wait_event` 里看到的名字就来自这里；
- **后台进程家族**（`postmaster/`）：checkpointer（刷 checkpoint）、bgwriter（提前刷脏页让 backend 少自己刷）、walwriter（异步刷 WAL 缓冲）、autovacuum launcher/worker、archiver、stats collector（PG 15 起进共享内存 `utils/activity/`）、logical replication launcher；
- **background worker 框架**（`bgworker.c`）：扩展可以注册自己的常驻进程——写你自己的"数据库内的守护程序"就靠它。

## 第 15 章 · 复制与高可用

### 物理流复制（主干）
```
主库: walsender (replication/walsender.c)
        └─ 读 WAL → 按复制协议流式发送
备库: walreceiver (replication/walreceiver.c) 收 WAL 落盘
        └─ startup 进程持续 redo（就是"永不结束的崩溃恢复"）
             └─ Hot Standby：redo 的同时允许只读查询
                 (冲突处理在 storage/ipc/standby.c)
```
- 同步复制：主库提交时等待备库 flush/apply 确认（`syncrep.c`），`synchronous_standby_names` 支持 quorum——分布式同学可对照 Raft 的 quorum ack 但注意它**不做选主**；
- 复制槽（`replication/slot.c`）：防止主库删掉备库还没消费的 WAL——语义上就是 Kafka 消费位点 + retention 保护。

### 逻辑复制（大数据工程师的富矿）
```
replication/logical/
├── decode.c         把物理 WAL 记录解码成"逻辑变更"
├── reorderbuffer.c  按事务重组变更（WAL 里各事务交错），提交时按序吐出
├── snapbuild.c      历史快照构建：解码老 WAL 时要能查"当时"的目录
├── logical.c        输出插件框架（pgoutput / wal2json / test_decoding）
└── worker.c         订阅端 apply worker
```
这就是 **CDC（Change Data Capture）的内核实现** —— Debezium 的 PG connector 底下就是这套机制。你做过大数据管道的话，读懂 reorderbuffer（含大事务落盘、流式解码）会非常有共鸣。

## 第 16 章 · 查询处理的高级模块

- **统计信息**：`commands/analyze.c` 采样（Vitter 水库采样算法）算出 MCV、直方图、ndistinct 存入 `pg_statistic`；`utils/adt/selfuncs.c` 消费它们估选择率；`statistics/` 目录是多列扩展统计（应对列间相关性——优化器估错行数的头号原因）；
- **并行查询**（PG 9.6+）：`access/transam/parallel.c` 的并行基础设施（把快照、GUC、事务状态序列化给 worker）+ `executor/nodeGather.c` 汇聚 + 各 `Parallel` 路径。worker 间用共享内存队列（`shm_mq.c`）传元组——一个进程内的小型 MPP，对照 Spark shuffle 理解 Gather/GatherMerge；
- **JIT**（PG 11+，`jit/llvm/llvmjit.c`）：把第 5 章的表达式操作码编译成 LLVM IR，再内联、优化。触发阈值 `jit_above_cost`。分析型长查询收益大，OLTP 短查询反而可能被编译开销拖慢；
- **分区表**：`partitioning/partprune.c` 的分区裁剪发生在三个时机（计划期、执行器启动期、运行期参数化裁剪），`executor/execPartition.c` 负责 INSERT 路由——对照 Hive 分区裁剪，但 PG 的运行期裁剪更精细。

## 第 17 章 · 扩展机制：PG 的"平台性"

PG 与 MySQL 最大的生态差异：PG 从 Berkeley 时代（Stonebraker 的 "extensible database" 论文）就把可扩展性作为第一设计目标。

- **Hook 体系**：内核在关键路径上留了函数指针（如 `planner_hook`、`ExecutorRun_hook`（execMain.c:71）、`post_parse_analyze_hook`），扩展的 `.so` 在 `_PG_init` 里接管它们。`pg_stat_statements`、TimescaleDB、Citus 全靠 hook 寄生；
- **扩展框架**：`CREATE EXTENSION`（`commands/extension.c`）= SQL 脚本 + 动态库 + 版本升级路径的打包机制；
- **FDW（外部数据包装器）**：`src/backend/foreign/` + 参考实现 `contrib/postgres_fdw`——把远程数据源伪装成表，优化器能把 WHERE/JOIN/聚合下推（pushdown）。这是 PG 系分布式方案（如 Citus 之外的 sharding 玩法）的地基，也是大数据联邦查询（Presto connector）思想的同类项；
- **自定义类型/操作符/索引 opclass**：PostGIS 就是"新类型 + GiST opclass"的极致案例。

> 对想成为顶级专家的人，扩展机制是**最好的练手场**：不用改内核、不用等社区 review，就能在真实内核 API 上写生产级代码。第三部分的里程碑项目会用到它。

---

# 第三部分 · 学习路线图：从现在到顶级专家

按 6 个阶段推进，每阶段给出 **目标 / 源码任务 / 理论配套 / 动手实验 / 验收标准**。节奏按"业余时间每周 8–10 小时"估算，全程约 18–24 个月；全职投入可减半。你的工程功底会让前两个阶段明显快于普通人。

## 阶段 0 · 环境搭建（第 1 周）

把源码变成"可以打断点的活物"，这是一切的前提。

```bash
# 在你的源码目录下（推荐 debug 构建）
meson setup build --prefix=$HOME/pgdev \
    -Ddebug=true -Dbuildtype=debug -Dcassert=true
ninja -C build && ninja -C build install

$HOME/pgdev/bin/initdb -D $HOME/pgdata
$HOME/pgdev/bin/pg_ctl -D $HOME/pgdata -l /tmp/pg.log start
$HOME/pgdev/bin/psql postgres
```

- `-Dcassert=true` 打开内核断言，学习期必开（生产别开）；
- 调试流程：psql 里 `SELECT pg_backend_pid();` → `lldb -p <pid>`（macOS）→ `b exec_simple_query` → 在 psql 执行 SQL → 单步。在断点里用 `call pprint(parsetree)` 打印任意节点树；
- 跑测试：`meson test -C build`（回归测试是你以后验证一切修改的安全网）；
- 工具箱：`pg_waldump`（读 WAL）、`pg_controldata`（控制文件）、扩展 `pageinspect`（看页）、`pg_buffercache`（看缓冲池）、`pg_stat_statements`。

**验收**：能在 `heap_insert` 断住一条 INSERT，打印出它的元组和所在缓冲区号。

## 阶段 1 · 走通执行路径（第 1–2 个月）

- **源码任务**：按本资料第一部分的顺序，把 `exec_simple_query` 五阶段各精读一遍；每个阶段用调试器走一次真实 SQL；把 0.3 节列的 5 个 README 读完；
- **理论配套**：
  - 课程：**CMU 15-445**（Andy Pavlo，B 站有搬运）——它的课程结构和本资料的模块划分几乎一一对应，边上课边在 PG 源码里找实现是最高效的学法；
  - 论文：Hellerstein & Stonebraker **《Architecture of a Database System》**（50 页综述，数据库内核的"世界地图"）；
- **动手实验**：`debug_print_parse / debug_print_rewritten / debug_print_plan` 三个 GUC 打开，观察同一条 SQL 的三棵树；手工对照 EXPLAIN 的 cost 公式（第 4 章实验）；
- **验收标准**：不看资料，能在白板上画出第 6.4 节那张全景调用栈图，并说清每层的输入输出数据结构。

## 阶段 2 · 存储与事务内核（第 3–6 个月）—— 本路线图的重心

- **源码任务**（按序）：bufmgr → heapam → 事务/MVCC（第 9 章全部）→ WAL（第 10 章）→ VACUUM（第 11 章）。这五块是 PG 内核的"发动机舱"，值得投入一半的总学习时间；
- **理论配套**：
  - 论文：**ARIES**（读前半部分即可，重点是 WAL/redo/undo 框架，然后对照 PG 为什么不需要 undo）；**Lehman & Yao 1981**（B-link tree，配合 nbtree/README）；**Cahill SSI 2008**（配合 README-SSI）；Stonebraker **《The Design of POSTGRES》1986**（理解 no-overwrite storage 的历史来源）；
  - 书：**《PostgreSQL 14 Internals》（Egor Rogov，官方免费 PDF，有中文版《PostgreSQL 指南：内幕探索》类似定位）**——它就是"给人读的 PG 源码"，与源码对照读效果拔群；
- **动手实验**：pageinspect 全家桶（第 7/8 章实验）；两会话锁观察（第 9 章实验）；pg_waldump 对照（第 10 章实验）；故意制造 XID 老化观察 freeze；
- **验收标准**：能独立回答这三道"内核面试题"并给出源码依据——
  ① 一条 UPDATE 语句会产生哪些 WAL 记录？② hint bit 为什么会让 SELECT 产生写 I/O？③ 为什么长事务会导致表膨胀（从 GetSnapshotData 的 xmin 说起）？

## 阶段 3 · 优化器与执行器进阶（第 7–10 个月）

- **源码任务**：精读 planner.c / allpaths.c / costsize.c / selfuncs.c / createplan.c；执行器侧精读 nodeHashjoin.c（含 hybrid hash join 落盘分批）、nodeAgg.c、tuplesort.c、execExpr*（表达式虚拟机）；
- **理论配套**：**Selinger 1979（System R）**——数据库史上最重要论文，读完再看 standard_join_search 会有"原文照搬"的震撼；Graefe《Volcano》与《Query Evaluation Techniques》综述；CMU **15-721**（高级课，讲现代内存数据库/向量化/编译执行，帮你看清 PG 在设计空间里的位置）；
- **动手实验**：找一个优化器"估错行数"的真实案例（列相关性），用 `CREATE STATISTICS` 修复并阅读扩展统计的实现；用 `pg_hint_plan` 强制不同计划对比代价模型的准确性；打开 `jit=on/off` 对比 TPC-H 单查询；
- **验收标准**：给你一条 5 表连接的慢 SQL，你能通过读 EXPLAIN + 查 selfuncs.c 的估计逻辑，说出优化器**为什么**选了这个计划，而不只是"它选错了"。

## 阶段 4 · 第一个里程碑项目：写扩展（第 11–13 个月）

从"读"转向"写"。推荐递进式三连：
1. **热身**：仿照 `contrib/pg_stat_statements` 写一个用 hook 统计"每个用户慢查询 Top-N"的扩展；
2. **中级**：写一个 FDW（比如包装 Kafka 或 Parquet 文件——正好用上你的大数据背景），实现 `GetForeignRelSize / IterateForeignScan`，体会优化器如何向你的代码要行数估计；
3. **进阶**：用 Table AM 或 Index AM 接口做一个玩具实现（比如内存表，或一个简化 bitmap 索引）。
- **理论配套**：读 `contrib/` 里同类扩展源码（模仿是最快的路）；
- **验收标准**：扩展能通过自己写的回归测试（`make installcheck` 风格），并能解释你的代码在崩溃恢复/并发下为什么是安全的（或明确声明不安全的边界）。

## 阶段 5 · 进入社区：从读者到贡献者（第 14–20 个月）

顶级专家的定义不是"读完了源码"，而是**社区认可你的判断**。PG 社区是少数完全靠邮件列表运转、无公司控制的顶级开源社区，参与路径非常明确：

1. **订阅 pgsql-hackers 邮件列表**——每天扫标题，挑一两个感兴趣的线程精读。这是世界上最高水平的数据库设计讨论，免费；
2. **做 patch review**：注册 [CommitFest](https://commitfest.postgresql.org)，认领别人的 patch 做 review（跑通、读代码、提意见）。社区极度缺 reviewer，这是建立声誉最快的路，而且**逼你读最新的、正在演进的代码**；
3. **提交第一个 patch**：从文档修正、报错信息改进、`git grep XXX_TODO`、或你在阅读中发现的注释错误开始；然后是小 bug fix（pgsql-bugs 列表里找可复现的）；
4. **跟一个大特性**：选一个正在进行的大 patch（近年热点：异步 I/O / DIO、64 位 XID、内置逻辑复制增强、SQL/JSON），从头跟完它的所有邮件讨论和版本迭代——**这等于免费旁听顶级工程师的设计评审**。

- **验收标准**：至少一个被 commit 的 patch（哪怕一行），至少三次有实质内容的 review 被作者采纳。

## 阶段 6 · 专家纵深（第 20 个月起，长期）

选 1–2 个纵深方向建立"领域权威"（社区的 committer 几乎都有明确领域）：

| 方向 | 适配你背景的理由 | 深入材料 |
|---|---|---|
| **逻辑复制 / CDC** | 直接复用你的大数据管道经验 | reorderbuffer.c、Debezium 源码、pgoutput 协议 |
| **优化器 / 统计** | 最缺人、最见功力的领域 | selfuncs.c 全文、扩展统计、learned cardinality 论文 |
| **并行执行 / 向量化** | 对接你对 MPP/Spark 的理解 | nodeGather、shm_mq、15-721 的向量化章节 |
| **存储引擎 / Table AM** | 造轮子最过瘾的领域 | OrioleDB / zheap 源码、LSM vs heap 的 trade-off 研究 |
| **分布式 PG** | 大数据工程师的天然主场 | Citus 源码（hook + FDW 的极致运用）、Spanner/Calvin 论文 |

持续动作：每年精读当年 SIGMOD/VLDB 的 3–5 篇工业界论文；给新版本的 release note 里每个大特性找到对应 commit 并读 diff；有机会就去 PGCon / PGConf 讲一次 talk。

## 附录 A · 书单与论文（按优先级）

**书**
1. 《PostgreSQL 14 Internals》Egor Rogov —— 与源码对照读的首选（免费 PDF）
2. 《Database System Concepts》(Silberschatz) 或《数据库系统实现》(Garcia-Molina) —— 补理论词汇表，按需查阅
3. 《Designing Data-Intensive Applications》—— 你可能读过；没读过的话它是连接你大数据经验与数据库内核的最好桥梁
4. 《The Internals of PostgreSQL》(Hironobu Suzuki，在线免费) —— 稍旧但图极好

**论文（内核六经）**
1. Selinger 1979 — System R 查询优化（→ 第 4 章）
2. Mohan 1992 — ARIES（→ 第 10 章）
3. Lehman & Yao 1981 — B-link tree（→ 第 8 章）
4. Stonebraker 1986 — The Design of POSTGRES（→ 全局）
5. Cahill 2008 — Serializable Snapshot Isolation（→ 第 9 章）
6. Hellerstein 2007 — Architecture of a Database System（→ 全局综述）

**课程**：CMU 15-445（基础）→ CMU 15-721（进阶）。

## 附录 B · 日常源码阅读方法论

1. **自顶向下问问题，自底向上读代码**：先带着"这条 SQL 怎么跑"的问题定位入口，再从叶子函数往上理解调用者的意图；
2. **README 优先，头文件次之，.c 最后**：结构体定义（`src/include/`）比函数实现的信息密度高得多；
3. **git blame / git log -p 是隐藏文档**：每个奇怪的判断背后都有一个 commit message 和一场邮件列表讨论，`git log -S'关键字'` 能挖出设计动机；
4. **用调试器代替想象**：任何"我猜它是这么工作的"都应该在 30 分钟内变成一次断点验证；
5. **写笔记时输出调用图**，格式就用本资料第 6.4 节那种缩进树——半年后你会攒出自己的"内核地图"；
6. **控制粒度**：不求每行都懂。第一遍搞清"数据结构 + 主流程 + 不变式（invariant）"，错误处理和边角分支留给第二遍。

---

*祝你顺利。两年后在 pgsql-hackers 上见。*
