# 第 8 章 · 事务、MVCC 与锁：PostgreSQL 的灵魂模块（v2）

> 对应源码：`src/backend/access/transam/`、`src/backend/storage/lmgr/`、`src/backend/storage/ipc/procarray.c`、`src/backend/access/heap/heapam_visibility.c`
>
> 必读官方文档：`src/backend/access/transam/README`（事务系统总纲）、`src/backend/storage/lmgr/README`（锁体系）、`src/backend/storage/lmgr/README-SSI`（可串行化快照隔离）
>
> 本章为 v2 深度修订版：新增七段完整函数走读（GetSnapshotData、HeapTupleSatisfiesMVCC、XidInMVCCSnapshot、LWLock 三函数、LockAcquireExtended+fastpath、DeadLockCheck 主线、SSI 冲突检测主线）、六组"所以然"论证、不变式清单、设计史与 what-if 推演。所有 `文件:行号` 均按当前 master（20devel）逐一核实。

## 本章导读

如果说执行器是 PostgreSQL 的四肢，那么事务系统就是它的心脏。本章要回答的核心问题只有一个：**当成百上千个会话同时读写同一张表时，每个会话到底"看见"什么、"改动"如何互斥、"冲突"如何裁决？**

这也是全系列理论密度最高的一章。数据库并发控制五十年的三条主要路线——两阶段锁（2PL）、快照隔离（SI）、可串行化快照隔离（SSI）——在 PostgreSQL 的源码里都能找到活体标本：重量级锁管理器是 2PL 的骨架（锁持有到事务结束、有死锁检测），MVCC 快照是 SI 的教科书实现，predicate.c 则是 SSI 论文（SIGMOD 2008）的第一个工业级落地。读懂这一章，等于把并发控制理论在一份生产级代码上从头验证了一遍。

我们按照"从一个事务的生命周期出发"的顺序展开：

1. 事务如何被启动、提交、回滚——`xact.c` 的双层状态机（8.1）；
2. 事务的"身份证"XID 如何分配，32 位计数器回卷这个 PG 运维中最凶险的问题如何被层层设防，以及"为什么社区拖了这么多年还没换 64 位"（8.2）；
3. 事务开始读数据前拿到的"快照"是什么、怎么拍的、为什么必须原子地拍（8.3）；
4. 拿着快照去看一行数据时，可见性判断的完整代码路径——逐分支走读（8.4）；
5. 可见性判断依赖的"提交日志"CLOG 与其底层 SLRU 缓冲（8.5）；
6. 三层锁体系：spinlock → LWLock → 重量级锁，含 LWLock 原子协议与 fastpath 正确性论证（8.6）；
7. 行锁为什么不进锁表——xmax 编码与 multixact，附内存量化对比（8.7）；
8. 死锁检测算法与懒触发的代价账（8.8）；
9. 快照隔离的"漏洞"write-skew 与 SSI 的修补，附四个隔离级别下的异常谱系推演（8.9）；
10. 全章不变式清单（8.10）与设计史（8.11）。

贯穿全章的主线是与 InnoDB 的对比：**InnoDB 用 undo log 把旧版本"藏"在回滚段里，PG 把新旧版本平铺在堆表中；InnoDB 靠记录锁/间隙锁（2PL 风格）实现隔离，PG 靠 MVCC + 事后检测（死锁检测、SSI）**。这一路线分野决定了两个数据库几乎所有的行为差异——包括为什么 PG 需要 VACUUM、为什么 PG 没有间隙锁、为什么 PG 的只读事务几乎零开销、为什么 PG 的 SERIALIZABLE 不阻塞读写却可能事后回滚。

## 前置知识

- **元组头**：每个堆元组头部（`src/include/access/htup_details.h`）有 `t_xmin`（创建该版本的事务 XID）、`t_xmax`（删除/锁定该版本的事务 XID）、`t_cid`（命令号）、`t_ctid`（版本链指针）和两个 infomask 标志字。UPDATE 在 PG 中 = 旧版本打上 xmax + 插入新版本，旧版本留在原地等 VACUUM 回收（对比 InnoDB：原地更新 + undo 链）。
- **快照隔离（SI）**：事务开始（或每条语句开始）时记录"当时哪些事务在跑"，之后只看"拍快照时已提交"的数据。读不阻塞写、写不阻塞读。
- **ProcArray**：共享内存中的活跃后端进程数组（`src/backend/storage/ipc/procarray.c`），每个 backend 一个槽位，记录其当前 XID、xmin 等，是"拍快照"的数据来源。PG 14 起相关状态被拆成若干按进程序号索引的紧凑数组（`ProcGlobal->xids[]` 等），8.3 详述。
- **LWLock 与 spinlock**：内核内部保护共享内存的轻量锁，与用户可见的表锁/行锁（重量级锁）是完全不同的东西，8.6 节详述。
- **原子操作与内存屏障**：本章多处走读依赖三个概念——CAS（compare-and-swap，`pg_atomic_compare_exchange_u32`：仅当当前值等于期望值时写入新值，失败则把当前值带回来）、原子加减（`pg_atomic_fetch_or/sub` 等）、屏障配对（写方的 `pg_write_barrier` 与读方的 `pg_read_barrier` 成对出现，保证"先写 A 再写 B"的顺序对读方可见）。LWLock 的获取/释放自带全屏障语义，这一点在 fastpath 锁的正确性论证里是关键前提。

---

## 8.1 事务状态机：xact.c

`xact.c`（约 6000 行）维护**两层状态机**：**低层 `TransState`**（`TRANS_DEFAULT/START/INPROGRESS/COMMIT/ABORT/PREPARE`）描述"事务本体"处于什么阶段，由 `StartTransaction()` 等底层函数驱动；**高层 `TBlockState`** 描述"用户视角的事务块"处于什么阶段，由 `BeginTransactionBlock()` 等响应 SQL 命令的函数驱动。

为什么要两层？因为 PG 中"一条不带 BEGIN 的 SQL"也是一个事务（隐式事务），而用户显式的 `BEGIN ... COMMIT` 块可以横跨多条命令、中途出错后还要停在"等待 ROLLBACK"的状态。高层状态机就是协调"命令边界"与"事务边界"错位的机构。

### 8.1.1 TBLOCK_* 状态全集

`src/backend/access/transam/xact.c:159`：

```c
typedef enum TBlockState
{
    TBLOCK_DEFAULT,             /* idle */
    TBLOCK_STARTED,             /* running single-query transaction */
    TBLOCK_BEGIN,               /* starting transaction block */
    TBLOCK_INPROGRESS,          /* live transaction */
    TBLOCK_IMPLICIT_INPROGRESS, /* live transaction after implicit BEGIN */
    TBLOCK_PARALLEL_INPROGRESS, /* live transaction inside parallel worker */
    TBLOCK_END,                 /* COMMIT received */
    TBLOCK_ABORT,               /* failed xact, awaiting ROLLBACK */
    TBLOCK_ABORT_END,           /* failed xact, ROLLBACK received */
    TBLOCK_ABORT_PENDING,       /* live xact, ROLLBACK received */
    TBLOCK_PREPARE,             /* live xact, PREPARE received */
    /* subtransaction states */
    TBLOCK_SUBBEGIN,            /* starting a subtransaction */
    TBLOCK_SUBINPROGRESS,       /* live subtransaction */
    ...
    TBLOCK_SUBRESTART,          /* live subxact, ROLLBACK TO received */
    TBLOCK_SUBABORT_RESTART,    /* failed subxact, ROLLBACK TO received */
} TBlockState;
```

关键状态的语义与典型转换路径：

```
单条 SQL（无 BEGIN）:
  DEFAULT --StartTransactionCommand--> STARTED --CommitTransactionCommand--> DEFAULT

显式事务块:
  DEFAULT --BEGIN--> BEGIN --(命令结束)--> INPROGRESS
  INPROGRESS --COMMIT--> END --(命令结束, 真正提交)--> DEFAULT
  INPROGRESS --(出错)--> ABORT --ROLLBACK--> ABORT_END --> DEFAULT
  INPROGRESS --ROLLBACK--> ABORT_PENDING --> DEFAULT
```

注意两个精妙设计：

1. **`TBLOCK_ABORT` 是"僵而不死"状态**。事务块内某条语句出错后，事务已在内部回滚（`AbortTransaction()` 已执行），但状态停在 `TBLOCK_ABORT`，后续所有命令都被 tcop 拒绝（"current transaction is aborted"），直到用户发 ROLLBACK。这是 PG 与 MySQL 的一个著名行为差异：MySQL 出错后事务通常还能继续，PG 必须显式回滚整个事务（或回滚到 SAVEPOINT）。
2. **COMMIT/ROLLBACK 命令本身不立即提交/回滚**，只是把状态改成 `TBLOCK_END`/`TBLOCK_ABORT_PENDING`，真正的动作推迟到本条命令结束时的 `CommitTransactionCommand()`（`xact.c:3210`，实际分发在 `CommitTransactionCommandInternal()`，`xact.c:3228`）里统一执行。命令边界的调度入口是三兄弟：`StartTransactionCommand()`（`xact.c:3112`）、`CommitTransactionCommand()`、`AbortCurrentTransaction()`，它们各自就是一个针对 `blockState` 的大 switch。

### 8.1.2 StartTransaction / CommitTransaction / AbortTransaction 的职责划分

**`StartTransaction()`（`xact.c:2106`）**——初始化一个顶层事务。核心动作（按代码顺序）：

- 把 `TopTransactionStateData` 设为当前状态，`s->state = TRANS_START`；
- **不分配 XID**（`s->fullTransactionId = InvalidFullTransactionId`）——这是 8.2.1 的重点；
- 决定隔离级别、只读属性（`XactIsoLevel = DefaultXactIsoLevel`）——注意：隔离级别在事务**启动瞬间**定格，事务中途 `SET TRANSACTION ISOLATION LEVEL` 只在第一条查询前有效；
- 分配虚拟事务号 vxid、事务内存上下文 `TopTransactionContext`、ResourceOwner；
- 记录事务启动时间戳（`now()` 的返回值由此定格），最后 `s->state = TRANS_INPROGRESS`。

**`CommitTransaction()`（`xact.c:2270`）**——提交的编排者，顺序极其讲究：

1. 先做**可能调用用户代码**的收尾：循环执行延迟触发器 + 关闭 portal（因为触发器可能开游标、游标可能又挂触发器，必须"洗涤-漂洗-重复"直到收敛）；
2. 之后进入不可再出错的区段：`s->state = TRANS_COMMIT`；
3. **`RecordTransactionCommit()`（`xact.c:1345`）**：写 commit WAL 记录、根据 `synchronous_commit` 决定是否等待刷盘、更新 CLOG 状态为已提交——这一步是事务**持久性**的分界点；
4. **`ProcArrayEndTransaction()`（`procarray.c:663`）**：拿 ProcArrayLock 排他锁（或搭"组提交式"顺风车，见下），把自己的 XID 从紧凑数组里清掉、推进 `latestCompletedXid`、`xactCompletionCount++`（`procarray.c:765-768`）——这一步是事务对**其他会话可见性**的分界点（此后新拍的快照会认为本事务已完成）；
5. 释放锁、smgr 层面真正删除本事务 DROP 的文件、释放内存上下文等资源清理（各种 `AtEOXact_*` 回调）。

第 3、4 步的顺序不能颠倒：必须先让 CLOG/WAL 说"我提交了"，再从 ProcArray 消失。若反过来，会出现一个窗口——新快照认为你已完成，去查 CLOG 却查到"未提交"，把你已提交的行判为中止而丢失更新。这是本章第一个"顺序即正确性"的例子，8.3.2 的原子快照论证是它的镜像。

另外注意 `ProcArrayEndTransaction()` 里的**组退出优化**（`ProcArrayGroupClearXid()`，`procarray.c:686` 调用）：拿不到 ProcArrayLock 的提交者把自己挂到一个原子链表上，由第一个拿到锁的进程替全组清理 XID——高并发短事务下把 N 次排他锁获取合并成 1 次。这与 WAL 的组提交是同一个思想在不同瓶颈上的复用。

**`AbortTransaction()`（`xact.c:2855`）**——异常路径的清道夫。它必须假设"系统可能处于任何烂摊子状态"：先 `HOLD_INTERRUPTS()` 防止清理过程再被打断，重置内存上下文和 ResourceOwner，然后写 abort WAL 记录、把 CLOG 标为 aborted、从 ProcArray 摘除、释放锁。一个安全设计值得注意：`StartTransaction` 阶段就预留了一块保底内存，使 `AbortTransaction` 在 OOM 场景下也能完成清理。

MVCC 让 PG 的回滚成本与 InnoDB 截然不同：**InnoDB 回滚要沿 undo 链逐条反向恢复（大事务回滚可能比执行还慢），PG 回滚只需把 CLOG 里的 2 个 bit 标成 aborted**——废版本留在原地，反正可见性判断会把它们过滤掉，清理交给 VACUUM。这是"平铺多版本"路线最漂亮的红利之一；代价在第 10 章（VACUUM）结账。

### 8.1.3 子事务（SAVEPOINT）的实现

`SAVEPOINT name` → `DefineSavepoint()` → `PushTransaction()`：在 `CurrentTransactionState` 链表上压入一个新的 `TransactionStateData`（`parent` 指针回链父事务），`nestingLevel` 加一，`blockState = TBLOCK_SUBBEGIN`。命令结束时 `StartSubTransaction()` 把它变成 `TBLOCK_SUBINPROGRESS`。

关键点：

- **子事务同样惰性分配 XID**。真的需要 XID 时，`AssignTransactionId()`（`xact.c:637`）会先保证所有祖先都有 XID（用显式数组消除深层递归——防御几千层 SAVEPOINT 打爆栈），再给自己分配，保证"子 XID 一定大于父 XID"这一不变式；
- **pg_subtrans 记录父子关系**。分配子 XID 后立刻调用 `SubTransSetParent()`（`src/backend/access/transam/subtrans.c:92`）把"子 XID → 父 XID"写入 pg_subtrans 这个 SLRU（8.5 节）。为什么需要它？因为快照里对子事务的记录可能溢出（每个 backend 的 PGPROC 只缓存 `PGPROC_MAX_CACHED_SUBXIDS`=64 个子 XID，`src/include/storage/proc.h:43`），溢出后别的会话要判断某个子 XID 的状态，只能通过 `SubTransGetTopmostTransaction()`（`subtrans.c:170`）逐级爬到顶层 XID 再查（8.4.3 会看到这条慢路径）。与 CLOG 不同，pg_subtrans 不必崩溃安全——崩溃后所有未提交事务都视为 aborted，父子关系无人再问津；
- **子事务提交不写 CLOG 的"已提交"**。`RELEASE SAVEPOINT` → `CommitSubTransaction()` 只是把子事务的 XID 挂进父事务的 `childXids` 数组，资源移交给父层。只有顶层事务提交时，`TransactionIdSetTreeStatus()`（`clog.c:192`）才把整棵 XID 树一起标成 committed。也就是说**子事务的命运完全捆绑在顶层事务上**——这就是 CLOG 里需要 `SUB_COMMITTED` 中间态的原因（8.5 节）；
- `ROLLBACK TO SAVEPOINT` → `RollbackToSavepoint()`：把目标层之上的所有子事务标为 `TBLOCK_SUBABORT_PENDING`/`SUBABORT_RESTART`，逐层 `AbortSubTransaction()` 后再重启一个同名子事务——所以 ROLLBACK TO 之后 savepoint 依然存在，可以反复回滚。

顺带一提：PL/pgSQL 的 `BEGIN...EXCEPTION` 块底层就是子事务，这也是"EXCEPTION 块很贵"的根源——每次进入都要 push 状态、可能分配子 XID、写 pg_subtrans。8.4.5 的 what-if 会推演它最恶劣的形态。

### 小结

- xact.c 是双层状态机：低层 TransState 管事务本体，高层 TBlockState 协调命令边界与事务边界的错位；
- COMMIT/ROLLBACK 命令只改状态，真正动作在命令结束的 `CommitTransactionCommand()` 里做；
- 提交的两个关键分界点顺序固定：先 `RecordTransactionCommit()`（持久性/CLOG）、后 `ProcArrayEndTransaction()`（对外可见性），颠倒会造成"快照认为已完成、CLOG 说未提交"的丢失窗口；
- 回滚几乎零成本（只标 CLOG），代价延后给 VACUUM——与 InnoDB undo 回放相反；
- 子事务 = 状态栈 + 惰性子 XID + pg_subtrans 父子映射，顶层提交时全家一起写 CLOG。

---

## 8.2 XID 体系：分配、比较与回卷防线

### 8.2.1 虚拟 XID：只读事务不领"身份证"

一个重要优化（PG 8.3 引入）：**事务不到真正修改数据的那一刻，绝不分配真 XID**。`StartTransaction()` 只给事务一个**虚拟事务号 vxid**：

```c
/* src/include/storage/lock.h:62 */
typedef struct VirtualTransactionId
{
    ProcNumber  procNumber;     /* proc number of the PGPROC */
    LocalTransactionId localTransactionId;  /* lxid from PGPROC */
} VirtualTransactionId;
```

vxid = 后端进程槽位号 + 该后端本地递增的计数器，纯本地生成，**不碰任何共享计数器、不写 CLOG、重启后可复用**。它的用途仅是"让别人能等待我结束"（作为 `LOCKTAG_VIRTUALTRANSACTION` 锁标签，比如 CREATE INDEX CONCURRENTLY 要等所有旧快照事务结束）。

只有当第一次执行 INSERT/UPDATE/DELETE 等需要在数据页上留下"作者签名"的操作时，才由 `AssignTransactionId()`（`xact.c:637`）调 `GetNewTransactionId()` 领取真 XID。收益是巨大的：**纯只读负载完全不消耗 XID 空间**——不推进回卷时钟、不写 CLOG、提交时连 WAL 都不用写。一个每秒十万查询的只读从库，XID 计数器纹丝不动。对比 InnoDB：只读事务同样有 trx_id 优化（read-only 事务不分配 trx_id），思路殊途同归，但 PG 由于 32 位 XID 的回卷压力，这个优化几乎是生死攸关的。

### 8.2.2 GetNewTransactionId 走读

`src/backend/access/transam/varsup.c:68`：

```c
FullTransactionId
GetNewTransactionId(bool isSubXact)
{
    ...
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);          // 95: 全局唯一的 XID 分配锁

    full_xid = TransamVariables->nextXid;             // 97: 共享内存中的下一个 XID
    xid = XidFromFullTransactionId(full_xid);

    if (TransactionIdFollowsOrEquals(xid, TransamVariables->xidVacLimit))
    {
        /* 108-113 注释: 进入回卷警戒区: 触发 autovacuum / 告警 / 拒绝服务, 见 8.2.4 */
        ...
    }

    ExtendCLOG(xid);        // 若跨入新 CLOG 页, 先把该页清零
    ExtendCommitTs(xid); ExtendSUBTRANS(xid);
    FullTransactionIdAdvance(&TransamVariables->nextXid);  // 计数器 +1

    /* 把新 XID 写入共享 ProcArray, 再释放 XidGenLock */
    ...
}
```

逐段解说：

- `varsup.c:95` 拿 `XidGenLock` 排他锁——XID 分配是全局串行点，这正是"惰性分配"值得的原因；
- `varsup.c:100-113` 的大段注释声明了警戒区三层规则（vacLimit 强制 autovacuum → warnLimit 告警 → stopLimit 拒绝服务），`varsup.c:114` 起进入检查体。注意 `varsup.c:127` 先释放 XidGenLock 再发信号/告警——持全局锁调 `get_database_name()` 可能死锁；`varsup.c:134` 的 `(xid % 65536) == 0` 把踢 postmaster 的信号限流到每 64K 个事务一次；
- ExtendCLOG 的顺序纪律：如果这个 XID 是新 CLOG 页的第一个事务，要**先**把 CLOG 页清零再推进计数器——顺序不能反，否则 ExtendCLOG 失败后会留下"已分配却无 CLOG 空间"的 XID；
- 函数尾部的大段注释是并发正确性的核心：**新 XID 必须在释放 XidGenLock 之前写进 ProcArray 的 xids 数组**（配合 `pg_write_barrier`），以保证任何比 `latestCompletedXid` 老的活跃 XID 一定出现在 ProcArray 里——这是快照正确性（8.3.2 的成员资格不变式）的根基，`transam/README` 有整节论证。8.3.3 走读里 `UINT32_ACCESS_ONCE` 与 `pg_read_barrier` 就是它的读方配对。

### 8.2.3 32 位回卷与模运算比较：TransactionIdPrecedes 逐行

XID 是 32 位无符号数，约 42.9 亿个。长期运行必然用完回绕，PG 的对策是**把 XID 空间看成一个环**："比较两个 XID 谁老"不用普通大小比较，而用模 2^32 的有符号差值：

```c
/* src/include/access/transam.h:263 */
static inline bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.  If both are normal, do a modulo-2^32 comparison.
     */
    int32       diff;

    if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
        return (id1 < id2);

    diff = (int32) (id1 - id2);
    return (diff < 0);
}
```

逐行拆解：

- XID 0/1/2 是保留值（`transam.h`：0=Invalid，1=Bootstrap，2=**Frozen**），它们"永远最老"，直接无符号比较；
- `(int32)(id1 - id2)` 是全部魔法：无符号减法自然回绕，强转 `int32` 后——若 id1 在环上位于 id2"后方半圈以内"，差值为负 → id1 更老。例如 id1=4294967290（接近 2^32）、id2=10：`(int32)(4294967290-10) = -16` < 0，判定 id1 更老，符合直觉；
- 推论：**这个比较只在两个 XID 相距不超过 2^31（约 21 亿）时才有意义**。一旦某个存活的旧 XID 与当前 XID 距离超过半圈，它会突然从"过去"翻转成"未来"——已提交的旧数据瞬间"不可见"，这就是灾难性的 wraparound 数据丢失。

### 8.2.4 freeze 与回卷保护的多道防线

既然比较半径只有 2^31，就必须保证磁盘上**不存在**年龄超过约 21 亿的普通 XID。手段是 **freeze（冻结）**：VACUUM 把足够老的元组标记为"对所有人可见"——现代 PG 通过设置 infomask 的 `HEAP_XMIN_FROZEN`（= `HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID` 两位同置，`htup_details.h:206`）实现，古老版本则是把 xmin 直接改写为 FrozenTransactionId(2)。冻结后的元组退出 XID 比较游戏，永远可见。

防线由外向内共四道（前三道全部实现在 `GetNewTransactionId` 的警戒区检查里）：

1. **xidVacLimit**：距回卷还剩 `autovacuum_freeze_max_age`（默认 2 亿）时，每 64K 个事务就踢一脚 postmaster 强制启动 autovacuum，做全表冻结（aggressive vacuum）；
2. **xidWarnLimit**：继续恶化则每次分配 XID 都打 WARNING："database must be vacuumed within N transactions"；
3. **xidStopLimit**：距回卷仅剩约 300 万时**拒绝分配新 XID**（`varsup.c:138` 起的 ereport(ERROR)），报错"database is not accepting commands..."——数据库变成只读，宁可停止服务也不损坏数据。单用户模式是留给 DBA 的逃生舱；
4. **兜底**：即便计数器真的绕回去，`pg_class.relfrozenxid`/`pg_database.datfrozenxid` 记录了每个表/库最老的未冻结 XID，`TransamVariables->oldestXid` 保证 CLOG 不会被过早截断。

### 8.2.5 所以然：为什么不直接换 64 位 XID？——页面格式兼容性论证

这是社区二十年的老争论（Postgres Pro 的 64-bit XID patchset 自 2017 年起反复提交，至今未入主干），值得完整推演一遍"为什么拖了这么多年"：

**方案 A：元组头直接加宽到 64 位 xmin/xmax。**

- 空间代价：每行元组头从 23 字节涨到 31 字节（+8）。对一张平均行宽 60 字节的表，这是约 13% 的永久性膨胀——所有表、所有行、永远。PG 的元组头本来就因 MVCC 背了"比 InnoDB 聚簇行更重"的名声，再加 8 字节是硬伤；
- **兼容性才是真正的死结**：PG 的升级承诺建立在 pg_upgrade 之上——不重写数据文件、只迁移 catalog，TB 级库分钟级完成。若新版本的堆页格式变了，旧页上的 32 位元组和新页上的 64 位元组会长期共存，heap AM 的每一处读写路径（可见性、pruning、freeze、TOAST、FPI 回放……）都要写两套分支并交叉测试。这个复杂度和风险面，社区评估后认为远超收益。

**方案 B：页级基准 + 元组内 32 位偏移（Postgres Pro 方案）。**每页 special 区存一个 64 位 base，元组里的 32 位 xmin/xmax 解释为相对 base 的偏移。行宽不变，但引入新问题：同页元组的 XID 跨度超过 2^32 时必须先"重定基"（搬移/冻结页内元组才能插入新行）——插入路径上出现了一个可能级联写放大的隐藏动作；pg_upgrade 旧页仍需兼容处理；multixact、2PC、逻辑复制里的 32 位 XID 都要跟着改。该 patchset 每轮 review 都在这些边角上被击穿。

**而现状的"惰性冻结"其实是一个摊销方案**：freeze 把"清理旧 XID"的成本摊到后台 VACUUM 里，正常运维下用户无感；付出的是运维尾部风险（autovacuum 被长事务/孤儿复制槽卡住时的回卷倒计时）。社区的实际路线是折中：**内部计量先 64 位化**——PG 12 引入 `FullTransactionId`（64 位 = 32 位 epoch + 32 位 xid，commit `2fc7af5e96`，2019-03-28，Thomas Munro），`nextXid` 等控制路径已经用 64 位（`GetNewTransactionId` 的返回类型即是，比较宏直接用 `.value` 无符号比较、无需模运算），**磁盘格式维持 32 位**。于是 freeze 在可预见的未来仍然必需。

InnoDB 对照：InnoDB 的 trx_id 是 48 位（约 281 万亿），purge 清理 undo 的压力类似 VACUUM，但没有"回卷停库"这种悬崖——这是 PG 为"元组头省空间 × 每行"付出的运维代价。理解这一节，你就理解了 PG 生产运维中最凶险的告警。

### 小结

- 只读事务只有 vxid（进程槽位号+本地计数器），不碰全局 XID 计数器——只读路径近乎零事务开销；
- `GetNewTransactionId` 在 XidGenLock 保护下分配 XID，且必须先扩展 CLOG、后推计数器、写完 ProcArray 才放锁；
- XID 比较是模 2^32 的有符号差值，有效半径 2^31，因此老元组必须冻结；
- 回卷防线：强制 autovacuum → 告警 → 拒绝写入 → relfrozenxid 兜底；
- 64 位 XID 之所以难产：方案 A 卡在行宽与 pg_upgrade 双格式兼容，方案 B 卡在页内重定基的级联复杂度；社区实际走的是"内部计量 64 位（FullTransactionId, PG 12）+ 磁盘 32 位 + 惰性冻结摊销"。

---

## 8.3 快照：SnapshotData 与 GetSnapshotData

### 8.3.1 SnapshotData 逐字段

`src/include/utils/snapshot.h:138`：

```c
typedef struct SnapshotData
{
    SnapshotType snapshot_type; /* type of snapshot */

    TransactionId xmin;         /* all XID < xmin are visible to me */
    TransactionId xmax;         /* all XID >= xmax are invisible to me */

    TransactionId *xip;         /* 运行中的顶层事务 XID 列表 */
    uint32      xcnt;           /* # of xact ids in xip[] */

    TransactionId *subxip;      /* 运行中的子事务 XID 列表 */
    int32       subxcnt;
    bool        suboverflowed;  /* has the subxip array overflowed? */

    bool        takenDuringRecovery;
    bool        copied;

    CommandId   curcid;         /* in my xact, CID < curcid are visible */
    ...
    uint64      snapXactCompletionCount;   /* PG 14 快照复用凭据 */
} SnapshotData;
```

- `snapshot_type`：普通 MVCC 快照之外还有 Dirty（看未提交数据，用于唯一约束检查）、Any、Toast 等变体，本章只关心 MVCC；
- `xmin`/`xmax`：可见性的快速通道。**注意 xmax 是开区间**："XID ≥ xmax 一律视为运行中（不可见）"、"XID < xmin 一律视为已完成"；
- `xip[]`：拍照瞬间正在运行的顶层事务列表，落在 [xmin, xmax) 区间内的 XID 才需要来这里查。语义上，**快照 = "xmin/xmax 定出的窗口 + 窗口内的黑名单 xip[]"**；
- `subxip[]`/`suboverflowed`：运行中的**子**事务 XID。每个 backend 最多向 ProcArray 缓存 64 个子 XID（`PGPROC_MAX_CACHED_SUBXIDS`，`proc.h:43`），任何一个 backend 溢出，整个快照标记 `suboverflowed`，此后可见性判断只能走 pg_subtrans 慢路径（见 8.4.3）——这就是"单个事务开几十个 SAVEPOINT 会拖慢全库"这一经典性能陷阱的机制；
- `takenDuringRecovery`：备库快照标志。备库上没有本地 ProcArray 活跃事务，改从 WAL 回放维护的 `KnownAssignedXids` 取数，且全部塞进 `subxip[]`（8.3.3 走读的 else 分支）；
- `curcid`：命令号，解决**事务内部**的可见性——同一事务里，后面的命令能看到前面命令的修改，但看不到本命令自己的（防止"Halloween 问题"：UPDATE 扫到自己刚插的行无限循环）；
- `snapXactCompletionCount`：见 8.3.4。

InnoDB 对照：这就是 InnoDB 的 ReadView（`m_low_limit_id`≈xmax、`m_up_limit_id`≈xmin、`m_ids`≈xip[]），概念一一对应，几乎是快照隔离的标准形态。差别在于用途：InnoDB 拿 ReadView 沿 undo 链回溯出正确版本；PG 拿快照在堆上平铺的多版本中**过滤**出正确版本。

### 8.3.2 所以然：快照为什么必须在 ProcArrayLock 下"原子拍摄"？

`GetSnapshotData` 全程持有 ProcArrayLock（共享模式），而 `ProcArrayEndTransaction` 清 XID 时持排他模式——也就是说**拍快照期间不允许任何事务"完成"**。这把锁在高并发下是有名的热点，那能不能不要它、边扫边拍？我们推演一次"撕裂快照"的后果就明白了。

设想无锁拍照：我顺序扫描 xids 数组，扫到一半时事务 T（XID=100，在我尚未扫到的槽位）提交了。分两种撕裂：

**撕裂一：黑名单漏项 + xmax 抬升。**T 提交把 `latestCompletedXid` 推到 100。若我在扫描**之后**才读 `latestCompletedXid` 计算 xmax，得到 xmax=101；而扫描时 T 已清掉 XID，xip[] 里没有 100。于是 XID=100 落在窗口内、又不在黑名单——被判"已完成"，进一步查 CLOG 得"已提交"→ **T 的修改对我可见**。但 T 是在我开始拍照**之后**才提交的，且和 T 同时提交的另一个事务 T'（XID=99，在我已扫过的槽位）被我记进了黑名单——**同一瞬间提交的两个事务，一个可见一个不可见**。如果 T 和 T' 之间有依赖（T' 读了 T 写的数据才提交），我就看到了"果"而看不到"因"：这不是任何一个时间点的数据库状态，一致性读从根上被破坏。

**撕裂二：xmin 计算错误。**若 T 恰好是当时最老的活跃事务，我扫过它的槽位前它清了 XID，我算出的 xmin 就偏大。xmin 会写进 `MyProc->xmin` 成为全局 OldestXmin 的一部分——VACUUM 据此判断"哪些死元组没人再看"。xmin 偏大意味着 VACUUM 可能清掉**我马上要according 我的快照读取的版本**，读到已被复用的行指针，返回垃圾数据或崩溃。

所以"原子性"在这里的精确含义是：**快照必须对应一条一致的分界线——`latestCompletedXid`、每个 proc 的 xid、xip[] 三者取自同一瞬间**。ProcArrayLock 共享锁正是这条分界线的实现：读者之间并行（拍快照互不阻塞），读者与"事务完成"互斥。这也解释了 `GetSnapshotDataReuse` 注释里的话（`procarray.c:2053-2058`）："持有 ProcArrayLock 期间，GetSnapshotData 认定的运行中事务集合不可能变化"——整个复用机制的正确性都建立在这条互斥上。

InnoDB 的 ReadView 生成（`ReadView::prepare()`）同样在 `trx_sys->mutex` 下拷贝活跃 trx_id 列表，问题与解法完全同构。并发控制里没有免费的一致性：要么锁一瞬间（PG/InnoDB），要么付出更复杂的多版本时间戳协议（如 Hekaton 的乐观时间戳）。

### 8.3.3 GetSnapshotData 全函数走读

`src/backend/storage/ipc/procarray.c:2114`。本节按执行顺序过完整个函数（约 350 行，含注释）；仅省略 `#ifdef` 调试代码与部分长注释原文（正文均已转述）。

**① 惰性分配 xip 数组（`procarray.c:2145-2172`）**

```c
    if (snapshot->xip == NULL)
    {
        snapshot->xip = (TransactionId *)
            malloc(GetMaxSnapshotXidCount() * sizeof(TransactionId));
        ...
        snapshot->subxip = (TransactionId *)
            malloc(GetMaxSnapshotSubxidCount() * sizeof(TransactionId));
        ...
    }
```

细节有三：数组按 `maxProcs`（上限）而非 `numProcs`（当前值）分配，因为 numProcs 要持锁才能读，宁可多分配也不把 malloc 挪进临界区；调用者全部传静态 SnapshotData（`CurrentSnapshotData` 等），所以这两块 malloc 全进程只发生一次，之后永久复用；subxip 分配失败时先 free(xip) 再报错（`procarray.c:2160-2170`），防止重试时看到半初始化的快照——防御性编程的典型样例。

**② 上锁与快照复用（`procarray.c:2178-2184`）**

```c
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    if (GetSnapshotDataReuse(snapshot))
    {
        LWLockRelease(ProcArrayLock);
        return snapshot;
    }
```

共享锁足矣（8.3.2 已论证）；复用检查见 8.3.4。

**③ 在锁内取齐所有"同一瞬间"的量（`procarray.c:2186-2204`）**

```c
    latest_completed = TransamVariables->latestCompletedXid;
    mypgxactoff = MyProc->pgxactoff;
    myxid = other_xids[mypgxactoff];
    Assert(myxid == MyProc->xid);

    oldestxid = TransamVariables->oldestXid;
    curXactCompletionCount = TransamVariables->xactCompletionCount;

    /* xmax is always latestCompletedXid + 1 */
    xmax = XidFromFullTransactionId(latest_completed);
    TransactionIdAdvance(xmax);
    Assert(TransactionIdIsNormal(xmax));

    /* initialize xmin calculation with xmax */
    xmin = xmax;

    /* take own xid into account, saves a check inside the loop */
    if (TransactionIdIsNormal(myxid) && NormalTransactionIdPrecedes(myxid, xmin))
        xmin = myxid;
```

- `other_xids` 即 `ProcGlobal->xids`——PG 14 列式化后的稠密 XID 数组（见 8.3.5）；
- **xmax = latestCompletedXid + 1** 而非 nextXid：凡 ≥ xmax 的事务要么在跑、要么快照之后才启动，一律判"运行中"即正确，且黑名单不必收录它们——比用 nextXid 当上界时 xip[] 更短；
- **xmin 从 xmax 起步往下压**，先在循环外单独考虑自己的 XID（`procarray.c:2203` 注释：省掉循环里每轮一次的 `pgxactoff == mypgxactoff` 之外的比较）。

**④ 主循环：扫描稠密数组（`procarray.c:2208-2312`）**

```c
    snapshot->takenDuringRecovery = RecoveryInProgress();

    if (!snapshot->takenDuringRecovery)
    {
        int         numProcs = arrayP->numProcs;
        TransactionId *xip = snapshot->xip;
        int        *pgprocnos = arrayP->pgprocnos;
        XidCacheStatus *subxidStates = ProcGlobal->subxidStates;
        uint8      *allStatusFlags = ProcGlobal->statusFlags;

        for (int pgxactoff = 0; pgxactoff < numProcs; pgxactoff++)
        {
            /* Fetch xid just once - see GetNewTransactionId */
            TransactionId xid = UINT32_ACCESS_ONCE(other_xids[pgxactoff]);
            uint8       statusFlags;

            if (likely(xid == InvalidTransactionId))
                continue;                              // (a) 无 XID: 只读事务
            if (pgxactoff == mypgxactoff)
                continue;                              // (b) 自己不进快照
            if (!NormalTransactionIdPrecedes(xid, xmax))
                continue;                              // (c) >= xmax 不必记
            statusFlags = allStatusFlags[pgxactoff];
            if (statusFlags & (PROC_IN_LOGICAL_DECODING | PROC_IN_VACUUM))
                continue;                              // (d) 逻辑解码/lazy vacuum

            if (NormalTransactionIdPrecedes(xid, xmin))
                xmin = xid;                            // (e) 顺手压低 xmin
            xip[count++] = xid;                        // (f) 记入黑名单

            if (!suboverflowed)                        // (g) 子事务采集
            {
                if (subxidStates[pgxactoff].overflowed)
                    suboverflowed = true;
                else
                {
                    int nsubxids = subxidStates[pgxactoff].count;
                    if (nsubxids > 0)
                    {
                        PGPROC *proc = &allProcs[pgprocnos[pgxactoff]];

                        pg_read_barrier();  /* pairs with GetNewTransactionId */
                        memcpy(snapshot->subxip + subcount,
                               proc->subxids.xids,
                               nsubxids * sizeof(TransactionId));
                        subcount += nsubxids;
                    }
                }
            }
        }
    }
```

逐项拆解：

- `UINT32_ACCESS_ONCE`（`procarray.c:2223`）：volatile 读，强制只从共享内存取一次 xid——若编译器重读两次，两次可能得到不同值（对方并发清 XID），后面的分支逻辑会精神分裂。它与 `GetNewTransactionId` 尾部"先写 xids 数组、屏障、再放 XidGenLock"配对；
- 分支 (a) 是 8.2.1 惰性 XID 的直接兑现：**只读事务连别人拍快照的成本都不增加**，一条 `likely()` 提示让纯读负载下这条分支几乎零开销；
- 分支 (c) 的正确性依赖一个不变式：**子 XID 必大于父 XID**（8.1.3），所以父被 xmax 挡掉时其子事务也必然 ≥ xmax，无需单独处理（`procarray.c:2251-2254` 注释）；
- 分支 (d)：lazy VACUUM 进程虽然可能持有 XID，但它绝不修改用户可见数据，把它排除能让 xmin 更紧、VACUUM 视界更激进；逻辑解码进程的 xmin 由复制槽单独管理（走读⑥）；
- (g) 的并发细节（`procarray.c:2280-2284` 注释）：对方 backend 可能**并发追加**子 XID（但绝不会删除），所以 `nsubxids` 只读一次；漏掉的新子 XID 必然 postdate xmax，不影响正确性——又一次靠"xmax 上界"兜底。`pg_read_barrier()`（`procarray.c:2302`）保证先看到 count 更新再 memcpy 数组内容。

**⑤ 备库分支（`procarray.c:2313-2349`）**

```c
    else
    {
        /* 热备: 从 KnownAssignedXids 取, 全部存进 subxip[] */
        subcount = KnownAssignedXidsGetAndSetXmin(snapshot->subxip, &xmin, xmax);

        if (TransactionIdPrecedesOrEquals(xmin, procArray->lastOverflowedXid))
            suboverflowed = true;
    }
```

备库上没有本地事务在跑，"活跃事务"是主库通过 WAL 广播的（`KnownAssignedXids`）。设计选择（`procarray.c:2318-2336` 注释写得很直白）：回放时分不清哪些 XID 是顶层、哪些是子事务，干脆**全塞进大得多的 subxip[]、xip[] 留空**——XidInMVCCSnapshot 对备库快照有专门的对称分支（8.4.3）。

**⑥ 收锁与全局视界维护（`procarray.c:2352-2466`）**

```c
    replication_slot_xmin = procArray->replication_slot_xmin;
    replication_slot_catalog_xmin = procArray->replication_slot_catalog_xmin;

    if (!TransactionIdIsValid(MyProc->xmin))
        MyProc->xmin = TransactionXmin = xmin;         // 2360-2361

    LWLockRelease(ProcArrayLock);

    /* maintain state for GlobalVis* (2365-2443, 详见第 10 章) */
    ...
    RecentXmin = xmin;

    snapshot->xmin = xmin;
    snapshot->xmax = xmax;
    snapshot->xcnt = count;
    snapshot->subxcnt = subcount;
    snapshot->suboverflowed = suboverflowed;
    snapshot->snapXactCompletionCount = curXactCompletionCount;
    snapshot->curcid = GetCurrentCommandId(false);
    snapshot->active_count = 0;
    snapshot->regd_count = 0;
    snapshot->copied = false;

    return snapshot;
```

- `MyProc->xmin = xmin`（`procarray.c:2360-2361`）必须在锁内完成：这是**向全世界公示"我依赖这条视界"**——VACUUM 计算 OldestXmin 时会扫到它。若在放锁后才写，会有一个窗口让 VACUUM 认为无人需要旧版本而清掉它们（8.3.2 撕裂二的镜像）。注意只有事务的**第一张**快照会设置它（`!TransactionIdIsValid` 条件）：READ COMMITTED 后续语句的新快照只更新 RecentXmin，`MyProc->xmin` 保持为事务内最老快照的 xmin——因为游标等可能仍持有旧快照；
- 放锁后的 GlobalVis* 维护段（`procarray.c:2365-2443`）是 PG 14 重构的产物：把"计算 VACUUM 全局视界"从拍快照路径**移出**（旧代码每次拍快照都算全局 horizon，是最大的可扩展性杀手之一，commit `dc7420c2c9`），这里只顺手更新上下界的缓存，精确计算推迟到 VACUUM 真正需要时。本章不展开，第 10 章详述；
- 省略说明：`ProcArrayInstallImportedXmin`（快照导入，`procarray.c:2478` 起）与 GlobalVis 细节不影响本章主线，故不走读。

### 8.3.4 GetSnapshotDataReuse：不重拍就复用

`procarray.c:2034`，PG 14 引入（commit `623a9ba79b`，2020-08-17）：

```c
static bool
GetSnapshotDataReuse(Snapshot snapshot)
{
    uint64      curXactCompletionCount;

    Assert(LWLockHeldByMe(ProcArrayLock));

    if (unlikely(snapshot->snapXactCompletionCount == 0))
        return false;                                  // 从未拍过, 不能复用

    curXactCompletionCount = TransamVariables->xactCompletionCount;
    if (curXactCompletionCount != snapshot->snapXactCompletionCount)
        return false;                                  // 有事务完成过, 必须重拍

    if (!TransactionIdIsValid(MyProc->xmin))
        MyProc->xmin = TransactionXmin = snapshot->xmin;

    RecentXmin = snapshot->xmin;
    snapshot->curcid = GetCurrentCommandId(false);
    snapshot->active_count = 0;
    snapshot->regd_count = 0;
    snapshot->copied = false;
    return true;
}
```

原理：共享内存 64 位计数器 `xactCompletionCount` **只在"带 XID 的事务完成"时**（`ProcArrayEndTransaction` 内，持 ProcArrayLock 排他，`procarray.c:590/768`）递增。若计数器与上次拍照时相同，则"运行中事务集合"一定没变——快照内容重拍也一样，直接复用，只刷新 curcid。三点精妙：

- 计数器**不因只读事务完成而递增**：只读事务本来就不影响快照内容，它们的来去不应作废缓存——惰性 XID 优化在这里第三次兑现红利（前两次：不占 XID 空间、不进别人快照）；
- 复用时重新执行 `MyProc->xmin = snapshot->xmin` 是安全的（`procarray.c:2060-2065` 注释）：运行集没变 ⇒ 快照可见的行不可能已被清理 ⇒ 重新公示同一个 xmin 不会使 OldestXmin 倒退；
- 对 READ COMMITTED（每条语句一张快照）的高频短查询负载，命中率非常高——一个只读为主的系统里，绝大多数语句间没有写事务完成。

### 8.3.5 PG 14 快照可扩展性重构：dense arrays 的动机

2020 年 Andres Freund 的系列 commit（按提交序）：

| commit | 内容 |
|---|---|
| `dc7420c2c9` (2020-08-12) | 拍快照时不再计算全局清理视界（RecentGlobalXmin 体系 → GlobalVis* 惰性体系） |
| `1f51c17c68` | PGXACT->xmin 移回 PGPROC |
| `941697c3c1` (2020-08-14) | **引入稠密 in-progress XID 数组**（`ProcGlobal->xids[]`） |
| `5788e258bb` / `73487a60fc` | vacuumFlags（statusFlags）、subxid 状态同样拆成稠密数组，PGXACT 结构体消亡 |
| `623a9ba79b` (2020-08-17) | xactCompletionCount 快照复用 |

动机要从内存层级讲起。PG 13 之前每个进程的事务状态集中在 per-proc 的 PGXACT 结构（为拍快照而从 PGPROC 拆出的"小结构"，那已是一轮缓存优化），但数组按 proc 槽位排列且**稀疏**——connection 池场景下几千个槽位里只有几十个活跃事务，拍快照要遍历全部槽位、每个都是一次独立的缓存行访问。连接数上千时 `GetSnapshotData` 成为全库最热函数，且 ProcArrayLock 持有时间被拉长，进一步放大锁争用——雪上加霜的正反馈。

重构后的布局是**列式思维**：把"拍快照要读的列"（xid、subxid 计数、statusFlags）从行式的 PGPROC/PGXACT 里抽出来，变成三个按 `pgxactoff` **紧凑连续**索引的数组，且只有"在 ProcArray 里的进程"占据条目（进程退出即压缩）。收益：主循环变成顺序内存扫描，硬件预取器全速工作；一个缓存行装 16 个 XID，几百连接的活跃集合只需几十次缓存行加载。配合快照复用，官方基准里数千连接的只读吞吐提升数倍，是近十年最重要的可扩展性改进之一。

维护代价也要诚实记账：pgxactoff 会随进程进出而变动（数组压缩），所有持有 pgxactoff 的地方要跟着更新（`procarray.c:2226` 的 Assert 就在校验 PGPROC 与数组的双向一致）；这是典型的"把复杂度从热路径搬到冷路径"。

### 8.3.6 各隔离级别拿快照的时机

PG 实际只有三种行为（SQL 标准四级中 READ UNCOMMITTED 在 PG 中等同 READ COMMITTED——MVCC 下实现脏读反而更麻烦，也无意义）：

| 隔离级别 | 拿快照时机 | 源码入口 |
|---|---|---|
| READ COMMITTED | **每条语句**开头拍一张新快照 | `GetTransactionSnapshot()`（`snapmgr.c:272`）每次都调 `GetSnapshotData` |
| REPEATABLE READ | 事务中**第一条语句**拍一张，全程复用 | `snapmgr.c` 中 `FirstSnapshotSet` 标志控制，之后返回 `CurrentSnapshot` |
| SERIALIZABLE | 同 RR 的快照 + SSI 冲突追踪 | `GetSerializableTransactionSnapshot()`（`predicate.c:1611`） |

注意细节：快照在**第一条语句**而非 BEGIN 时产生——`BEGIN; (发呆十分钟); SELECT ...` 的 RR 事务看到的是 SELECT 时刻的世界。另外 READ COMMITTED 的 UPDATE 遇到并发修改时执行 **EPQ（EvalPlanQual，`execMain.c:2687` 起）重检**：拿最新版本重新验证 WHERE 条件后更新；而 RR 遇到并发修改直接报 `serialization failure`——InnoDB RR 的 UPDATE 则用当前读（锁定读）静默改最新版本，这是两家 RR 语义最容易踩坑的差异。8.9.1 的异常谱系会把这组分叉推演到底。

### 8.3.7 what-if：一个长事务钉住 xmin 之后

推演"会话 A 开了 REPEATABLE READ 事务查了一行然后去吃午饭"的全链条影响（面试高频题，也是生产事故高频根因）：

1. **t0**：A 拍下快照，`MyProc->xmin = 1000` 被公示（`procarray.c:2360`）。此后 A 不需要做任何事，危害自动展开；
2. **VACUUM 失效**：所有表的 OldestXmin 被 A 的 xmin 摁在 1000。此后任何被删除/更新的行版本，只要 xmax ≥ 1000，VACUUM 都判定"可能还有人要看"而不能回收（`HeapTupleSatisfiesVacuum` 的 RECENTLY_DEAD 分支）。**注意：影响是全库的，哪怕 A 只查过一张小表**——因为快照语义上 A 随时可能去读任何表；
3. **膨胀滚雪球**：高更新表的死版本堆积 → 页内 HOT 剪枝也被同一视界挡住 → 表与索引物理膨胀 → 扫描变慢、缓存命中率下降；
4. **hint bit / freeze 延迟**：涉及的 CLOG 页迟迟不能截断（`oldestXid` 不能推进）；
5. **子事务慢路径放大**（若 A 同时滥用 SAVEPOINT）：见 8.4.5；
6. **恢复**：A 提交或回滚的瞬间，`ProcArrayEndTransaction` 清掉其 xmin，下一轮 VACUUM 视界恢复——但已经堆积的膨胀**不会自动消失**，轻则等 VACUUM 慢慢回收空间（页内空闲，文件不缩），重则需要 `VACUUM FULL`/`pg_repack` 重建。

InnoDB 的同款事故：长事务钉住 ReadView → purge 线程无法清理 undo → `history list length` 飙升 → undo 表空间膨胀 + 每次一致性读回溯更长的 undo 链而变慢。两家的病理同构，症状部位不同：PG 膨胀在**主表**（读起来是扫描变慢），InnoDB 膨胀在**undo**（读起来是版本回溯变慢）。监控口径也因此不同：PG 看 `pg_stat_activity` 里 `backend_xmin` 最老的会话，InnoDB 看 `trx_sys` 的 history list length。

### 小结

- 快照 = xmin/xmax 窗口 + 窗口内运行中事务黑名单 xip[]/subxip[]，等价于 InnoDB ReadView；
- 必须在 ProcArrayLock 下原子拍摄：无锁的撕裂快照会产生"同瞬提交的事务一个可见一个不可见"的因果悖论，以及 xmin 偏大导致 VACUUM 清掉在用版本；
- `GetSnapshotData` 主循环是对 PG 14 稠密数组的顺序扫描；xmax=latestCompletedXid+1，xmin 为活跃事务最小值并在锁内公示到 MyProc->xmin；备库从 KnownAssignedXids 取数全存 subxip；
- 快照复用凭据是 xactCompletionCount：只读事务的完成不作废缓存；
- PG 14 重构 = 列式稠密数组（缓存友好）+ 视界计算移出热路径 + 快照复用，三招合力解决千连接拍快照瓶颈；
- RC 每语句一拍、RR/SSI 首语句一拍；RC 用 EPQ 重检并发更新，RR 直接串行化失败；
- 长事务钉住 xmin 的危害是全库性的，且解除后膨胀不自动消退。

---

## 8.4 可见性判断：HeapTupleSatisfiesMVCC 全函数走读

现在把快照用起来。扫描堆表的每一行都要过这道门：`src/backend/access/heap/heapam_visibility.c:939`。判断逻辑一句话概括：**"它的出生（xmin）对我可见吗？它的死亡（xmax）对我可见吗？出生可见且死亡不可见 → 这行对我存在。"**

### 8.4.1 hint bit：把 CLOG 答案缓存在元组头上

判断"xmin 提交了没有"本该查 CLOG，但每行每次都查太贵。PG 把查询结果缓存在元组头 infomask 的 4 个 **hint bit** 里（`htup_details.h:204-208`）：

```c
#define HEAP_XMIN_COMMITTED     0x0100  /* t_xmin committed */
#define HEAP_XMIN_INVALID       0x0200  /* t_xmin invalid/aborted */
#define HEAP_XMAX_COMMITTED     0x0400  /* t_xmax committed */
#define HEAP_XMAX_INVALID       0x0800  /* t_xmax invalid/aborted */
```

设置入口 `SetHintBits()`（`heapam_visibility.c:199`，核心在 `SetHintBitsExt()`，`heapam_visibility.c:142`）：

```c
    if (TransactionIdIsValid(xid))
    {
        if (BufferIsPermanent(buffer))
        {
            /* NB: xid must be known committed here! */
            XLogRecPtr  commitLSN = TransactionIdGetCommitLSN(xid);

            if (XLogNeedsFlush(commitLSN) &&
                BufferGetLSNAtomic(buffer) < commitLSN)
            {
                /* not flushed and no LSN interlock, so don't set hint */
                return;
            }
        }
    }
```

（当前 master 上该函数还带一套批量模式的 `SetHintBitsState` 状态机，`heapam_visibility.c:91-99`：批量扫描先 `BufferBeginSetHintBits()` 申请页级"设 hint 权"，避免与页面 I/O 并发改写导致 checksum 撕裂——这属于 AIO 时代的新增防护，与本节主题正交，不展开。）

**异步提交下的 LSN 联锁**值得细品：hint bit 修改**不写 WAL**。但 `synchronous_commit=off` 时事务的 commit WAL 记录可能还没刷盘，如果此刻把 `HEAP_XMIN_COMMITTED` 打上并且**这个数据页**先于 commit 记录落盘，崩溃恢复后该事务其实没提交，页上却留着"已提交"的假 hint——数据损坏。所以规则是：**commit LSN 尚未刷盘、且页面 LSN 也没有大到能保证顺序（页刷出前必先刷到页面 LSN，顺带把 commit 记录也刷了）时，宁可不设 hint**，留给未来的访问者再试。

#### 所以然：hint bit 凭什么不写 WAL 也不怕崩溃？

论证它的**可再生性（regenerability）**：

1. hint bit 不是事实本身，只是**事实的缓存**。事实（事务提交与否）的权威存储是 CLOG，而 CLOG 的每次更新都受 WAL 保护（提交先写 commit 记录）；
2. 崩溃丢掉 hint bit（页没来得及刷盘）？无损——下一个读者在 `HeapTupleSatisfiesMVCC` 里查 CLOG 重新推导出同样的结论，再把 hint 盖回去。丢失的只是一点性能；
3. 崩溃留下**假** hint 才是灾难，而上面的 LSN 联锁恰好保证"页上任何已落盘的 COMMITTED hint，其对应 commit 记录必已先落盘"——WAL-before-data 纪律通过联锁间接维持。对 aborted hint 则无需联锁（`heapam_visibility.c:124-125` 注释："We can always set hint bits when marking a transaction aborted"）：崩溃恢复后"查无此人"的事务本来就按 aborted 处理，假 aborted hint 不可能出现（只有真 abort 或崩溃才会让 CLOG 查询走到 abort 结论）。

**那为什么 checksum 一开，又冒出 XLOG_FPI_FOR_HINT 了？**因为 checksum 破坏了上述论证的一个隐含前提——"页面部分写（torn page）无害"。不写 WAL 的 hint 修改会弄脏页面；脏页刷出途中断电，页面一半新一半旧。没有 checksum 时这无所谓：hint bit 差一位，语义照样可再生。有 checksum 时，半新半旧的页 **checksum 校验必然失败**，整页被判损坏——一次纯读操作引发的"数据损坏"。解法只能是恢复 full-page-write 保护：checkpoint 后第一次因 hint 弄脏页面时，写一条 `XLOG_FPI_FOR_HINT` 全页镜像（`MarkBufferDirtyHint` 路径；commit `50e547096c` 2013-12-13 引入 `wal_log_hints` GUC，`0bd624d63b` 2014-11-24 把这类 FPI 与普通 FPI 区分开）。这就是"开启校验和会放大写"的确切来源——**为可检测性付出的可再生性税**。

这个机制解释了 PG 一个著名现象：**大批量导入后的第一次全表扫描会产生大量写 I/O**——它在给每行盖"已提交"的章。运维上 `COPY ... FREEZE` 或让首扫发生在低峰就是为了这个。InnoDB 没有 hint bit——它把"提交与否"编码在锁标志与 undo 指针里，读旧版本的代价是回溯 undo 链而非查 CLOG，两家把同一笔成本记在不同科目下。

### 8.4.2 主体逐分支走读

以下按 `heapam_visibility.c:956-1096` 原文顺序覆盖**全部**分支，每个分支给一个能走到它的具体场景。省略项仅两处、均注明：函数开头三条 Assert（快照注册与元组自检，非逻辑分支）与 `HeapTupleCleanMoved`（`heapam_visibility.c:961`，处理 9.0 之前 VACUUM FULL 遗留的 MOVED_IN/MOVED_OFF 标志，现代数据库走不到）。

**xmin 阶段**（出生可见吗？）：

```c
    if (!HeapTupleHeaderXminCommitted(tuple))           // [X0] hint 没说"已提交"
    {
        if (HeapTupleHeaderXminInvalid(tuple))          // [X1]
            return false;
```

- **[X1] hint=aborted**：场景——事务 T1 插入一行后 ROLLBACK；此前已有读者扫过并盖了 `HEAP_XMIN_INVALID` 章。此后所有读者零成本跳过这具"出生即夭折"的尸体（等 VACUUM 收尸）。

```c
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {                                               // [X2] 我自己插入的行
            if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
                return false;   /* inserted after scan started */    // [X2a]
```

- **[X2] 自己插的行**。`TransactionIdIsCurrentTransactionId()`（`xact.c:943`）纯本地判断——遍历自己的事务状态栈（含子事务），不碰共享内存；这也是函数头注释（`heapam_visibility.c:922-936`）强调的设计：xmin 属于"运行中"事务时**故意不查 ProcArray 里的全局真相**——查了也不改变结论（对我的快照它照样算运行中），却要碰高争用共享结构。此时快照失效，改用 **curcid 比命令号**：
- **[X2a]** 场景——`UPDATE t SET x = x+1` 扫描途中扫到了本条 UPDATE 自己刚插入的新版本（cmin == curcid）：判不可见，否则无限循环（Halloween 问题）。

```c
            if (tuple->t_infomask & HEAP_XMAX_INVALID)  /* xid invalid */
                return true;                            // [X2b] 没被删 → 可见
            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
                return true;                            // [X2c] xmax 只是锁 → 可见
```

- **[X2b]** 场景——本事务前一条命令 INSERT 的行，无人动过：可见（这就是"事务内能看到自己之前命令的修改"）。
- **[X2c]** 场景——本事务 INSERT 后又 `SELECT ... FOR UPDATE` 了这行：xmax 里是锁不是删除（8.7），行仍然活着。

```c
            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) // [X2d] xmax 是 multixact
            {
                TransactionId xmax = HeapTupleGetUpdateXid(tuple);

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;                        // [X2d-1]
                else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                    return true;    /* updated after scan started */   // [X2d-2]
                else
                    return false;   /* updated before scan started */  // [X2d-3]
            }
```

- **[X2d]** 场景——我插入的行被"我 + 别人"同时持有行锁后又被更新，xmax 是组合的 MultiXactId：先用 `HeapTupleGetUpdateXid` 从成员里挑出**真正的 updater**（至多一个成员是更新者，其余是纯锁定者）。注意 [X2d-1] 的推理：行是我插的、还没提交，别人不可能更新它（会被行锁挡住等我结束），所以"updater 不是当前事务"只有一种可能——**是我的某个已中止的子事务**（SAVEPOINT 里更新后 ROLLBACK TO 了），更新无效 → 可见。[X2d-2/3] 则是自己更新自己插入的行：按 cmax 与 curcid 比较决定"扫描开始后才删（仍可见）"还是"扫描开始前已删（不可见）"。

```c
            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                /* deleting subtransaction must have aborted */
                SetHintBitsExt(tuple, buffer, HEAP_XMAX_INVALID,
                               InvalidTransactionId, state);
                return true;                            // [X2e]
            }

            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */   // [X2f]
            else
                return false;   /* deleted before scan started */  // [X2g]
        }
```

- **[X2e]** 场景——`INSERT; SAVEPOINT s; DELETE; ROLLBACK TO s; SELECT`：删除来自已中止的子事务，作废并顺手盖 `HEAP_XMAX_INVALID` 章 → 可见。
- **[X2f]** 场景——同一事务里 `DELETE` 之后同命令内的重扫（或后开的游标 curcid 更小）：删除发生在"扫描开始"之后 → 本快照仍看得见。**[X2g]**——前一条命令删的 → 不可见。

```c
        else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;                               // [X3] 拍照时它还在跑
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBitsExt(tuple, buffer, HEAP_XMIN_COMMITTED,
                           HeapTupleHeaderGetRawXmin(tuple), state);   // [X4]
        else
        {
            /* it must have aborted or crashed */
            SetHintBitsExt(tuple, buffer, HEAP_XMIN_INVALID,
                           InvalidTransactionId, state);
            return false;                               // [X5]
        }
    }
```

- **[X3] 在我的快照里** → 不可见。场景——并发事务 T2 插入了行且**已经提交**，但它提交发生在我拍快照之后（拍照时它在 xip[] 里）：尽管 CLOG 已说"committed"，对我的快照它仍算运行中。注意判断次序：**先查快照再查 CLOG**——顺序反了就会把"照后提交"的行放进来，REPEATABLE READ 立刻穿帮。也因此这个分支不设 hint（它设不了：事实尚无定论或与我无关）。
- **[X4]** 场景——T2 插入并提交，我拍照晚于其提交（不在 xip[]）：CLOG 确认已提交，顺手盖章，**继续落到 xmax 阶段**（注意没有 return！出生可见 ≠ 行可见，还得验死亡）。
- **[X5]** 场景——T2 插入后回滚（或它所在的 backend 崩溃了）：既不在快照也没提交 → 推断为 aborted/crashed，盖 `HEAP_XMIN_INVALID` 章 → 不可见。**崩溃的事务永远不会有人替它写 CLOG=aborted**，全靠这里"查无此人即作废"的推断（以及崩溃恢复对未决事务的批量标 abort）。

```c
    else
    {
        /* xmin is committed, but maybe not according to our snapshot */
        if (!HeapTupleHeaderXminFrozen(tuple) &&
            XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false;       /* treat as still in progress */   // [X6]
    }
```

- **[X6] hint 已说提交，但快照说"对我还没有"**。场景——T2 提交后，某个**新快照**的读者盖了 COMMITTED 章；随后**我**（拍照早于 T2 提交的老快照）来读：hint 是全局事实（提交与否），快照是局部视角（对我算不算完成），**两者缺一不可**。冻结元组（`HeapTupleHeaderXminFrozen`）则连快照都不查——冻结的定义就是"对所有快照可见"，这也让 freeze 成为可见性判断的终极快路径。

**xmax 阶段**（死亡对我可见吗？——到这里出生已确认可见）：

```c
    if (tuple->t_infomask & HEAP_XMAX_INVALID)  /* xid invalid or aborted */
        return true;                                    // [M1]
    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        return true;                                    // [M2]
```

- **[M1]** 场景——普通存活行（从未被删）或删除已被盖章作废：可见。绝大多数行走到这里结束，两次位测试搞定。
- **[M2]** 场景——行正被别人 `SELECT FOR UPDATE` 锁着：xmax 有值但只是锁（`HEAP_XMAX_LOCK_ONLY`，`htup_details.h:197`）→ 锁不影响可见性 → 可见。**MVCC 读完全无视行锁**，这就是"读不阻塞写、写不阻塞读"在代码里的落点。

```c
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)         // [M3] multixact
    {
        TransactionId xmax = HeapTupleGetUpdateXid(tuple);

        if (TransactionIdIsCurrentTransactionId(xmax))
        {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;   /* deleted before scan started */
        }
        if (XidInMVCCSnapshot(xmax, snapshot))
            return true;
        if (TransactionIdDidCommit(xmax))
            return false;       /* updating transaction committed */
        /* it must have aborted or crashed */
        return true;
    }
```

- **[M3]** 场景——别人先 `FOR KEY SHARE` 锁住行，我又 UPDATE 它：xmax 变成 multixact（8.7.3）。提取真正的 updater 后，重演与单 XID 完全相同的四连问（自己？→快照？→CLOG？→否则 aborted），只是不盖 hint（multixact 的成员状态在 pg_multixact 里，infomask 的两个 xmax hint 位对 multi 语义不同）。

```c
    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))     // [M4] 无 hint
    {
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
        {                                               // [M4a] 我删的
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true;    /* deleted after scan started */
            else
                return false;   /* deleted before scan started */
        }

        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;                                // [M4b] 删除者对我算在跑

        if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
        {                                               // [M4c] 删除被中止
            SetHintBitsExt(tuple, buffer, HEAP_XMAX_INVALID,
                           InvalidTransactionId, state);
            return true;
        }
        /* xmax transaction committed */
        SetHintBitsExt(tuple, buffer, HEAP_XMAX_COMMITTED,
                       HeapTupleHeaderGetRawXmax(tuple), state);   // [M4d]
    }
    else
    {
        /* xmax is committed, but maybe not according to our snapshot */
        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true;        /* treat as still in progress */   // [M5]
    }

    /* xmax transaction committed */
    return false;                                       // [M6]
```

- **[M4a]** 我自己删的：又是 cmax vs curcid（同事务内先删后查，本命令开始前删的就看不见了）。**[M4b]** 场景——T3 删了这行且已提交，但提交晚于我拍照：删除对我"尚未发生"→ 行**仍可见**。这正是 REPEATABLE READ 下"别人删了我还能读到"的机制现场。**[M4c]** T3 删后回滚：删除作废、盖章、可见。**[M4d]** 删除确已提交且对我可见：盖 COMMITTED 章后落到 [M6] 返回 false——行已死。
- **[M5]** 与 [X6] 完全对称：hint 说删除已提交，但对我的老快照删除还没发生 → 可见。
- **[M6]**：出生可见 ∧ 死亡也可见 → 这行对我**不存在**（我看到的是它的新版本——沿 ctid 链的下一跳，由扫描层去取）。

纵观全函数：**每个 XID 的判断次序固定为 hint bit → 是不是我自己（比 curcid）→ 在不在快照（局部视角）→ CLOG（全局事实）**，并把 CLOG 结论缓存为 hint bit。这个次序既是正确性次序（快照必须先于 CLOG），也是成本次序（本地位测试 → 本地栈遍历 → 本地数组搜索 → 共享 SLRU）。

### 8.4.3 XidInMVCCSnapshot 全函数走读

`src/backend/utils/time/snapmgr.c:1868`，全文如下（仅去掉注释原文，逻辑一行不落）：

```c
bool
XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot)
{
    /* Any xid < xmin is not in-progress */
    if (TransactionIdPrecedes(xid, snapshot->xmin))
        return false;                                   // ①
    /* Any xid >= xmax is in-progress */
    if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
        return true;                                    // ②

    if (!snapshot->takenDuringRecovery)                 // ③ 主库快照
    {
        if (!snapshot->suboverflowed)
        {                                               // ④ 快路径
            if (pg_lfind32(xid, snapshot->subxip, snapshot->subxcnt))
                return true;
            /* not there, fall through to search xip[] */
        }
        else
        {                                               // ⑤ 慢路径
            xid = SubTransGetTopmostTransaction(xid);

            if (TransactionIdPrecedes(xid, snapshot->xmin))
                return false;
        }

        if (pg_lfind32(xid, snapshot->xip, snapshot->xcnt))
            return true;                                // ⑥
    }
    else                                                // ⑦ 备库快照
    {
        if (snapshot->suboverflowed)
        {
            xid = SubTransGetTopmostTransaction(xid);
            if (TransactionIdPrecedes(xid, snapshot->xmin))
                return false;
        }

        if (pg_lfind32(xid, snapshot->subxip, snapshot->subxcnt))
            return true;
    }

    return false;                                       // ⑧
}
```

- **①②边界裁剪**：绝大多数 XID 在这两行就出局。注释（`snapmgr.c:1870-1876`）特别论证了对子事务也成立：子 XID < xmin ⇒ 其父 XID 更小也 < xmin（子必大于父）；子 XID ≥ xmax ⇒ 其父在拍照时必未提交（否则子早该结束）——所以**边界判断无需先折算到顶层**，这省掉了快路径上的 pg_subtrans 访问；
- **④快路径**：两次 `pg_lfind32`（SIMD 加速的线性查找，一次比对 4~16 个值）搜 subxip 和 xip。为什么线性查找而不是排序+二分？数组通常只有几十项、SIMD 线性搜完的时间和一次分支预测失败同数量级，且拍快照时省了排序；
- **⑤慢路径（suboverflowed）**：黑名单里的子事务不全，无法用"不在名单"证明"不在跑"，只能先 `SubTransGetTopmostTransaction()` 把 xid 折算成顶层 XID——这要按 XID 逐级读 pg_subtrans SLRU 页（每级一次，可能 I/O）——再搜 xip[]（顶层名单是完整的）。折算后重查一次 xmin（子折到父可能落到窗口外）；
- **⑦备库分支**与主库对称：全部 XID 都在 subxip 里（8.3.3 ⑤的存储决定），一次搜索完成；
- **⑧**：窗口内且不在名单 → 拍照时不在跑（已完成）。

### 8.4.4 CLOG 查询路径

`TransactionIdDidCommit()` → `TransactionLogFetch()`（`transam.c:52`）：先查单条 XID 的本地缓存 `cachedFetchXid`（`transam.c:33/61-62`，命中场景：连续扫到同一事务批量插入的行），未命中则 `TransactionIdGetStatus()` 读 CLOG SLRU 页取出 2 bit。注意 `DidCommit` 对 `SUB_COMMITTED` 状态还会递归到父事务——子事务的最终命运由顶层决定（8.1.3/8.5.1）。

### 8.4.5 what-if：第 65 个子事务——suboverflowed 性能悬崖

推演一个真实事故模式（多家云厂商都发过同题 postmortem）：某 Java 应用给每条语句包一层 SAVEPOINT（ORM 的"语句级回滚"仿真），一个批处理事务跑了 200 条语句。

1. 第 64 个子 XID 之前：都缓存在该 backend PGPROC 的 subxids 数组里，全库无感；
2. **第 65 个子 XID 分配的瞬间**：`subxidStates[i].overflowed = true`。此后**所有会话**拍的每一张快照都带 `suboverflowed = true`（8.3.3 ④(g)）——注意是全库连坐，不是肇事会话独享；
3. 每张溢出快照的每次可见性判断，凡 XID 落进 [xmin, xmax) 窗口，都从"SIMD 搜个小数组"退化为"逐级读 pg_subtrans SLRU"（8.4.3 ⑤）。写热点表上一行可能被扫上万次/秒，pg_subtrans 的 bank 锁（8.5.2）成为新的争用点；SLRU 缓冲少（默认按 shared_buffers 比例推导，PG 17 起可用 `subtransaction_buffers` 调）时还会打到磁盘；
4. **悬崖的形状**：性能不是随子事务数线性下降，而是在 64→65 处断崖——因为代价从 O(数组) 跳到 O(SLRU 链)，且从"肇事者付费"跳到"全库付费"；
5. 窗口宽度放大器：如果同时有个长事务把 xmin 摁得很低（8.3.7），[xmin, xmax) 窗口极宽，落进窗口的 XID 更多，慢路径命中率进一步升高——两个反模式相乘而非相加；
6. 事务结束后，含其 XID 的快照窗口滑过，全库恢复。

工程结论：SAVEPOINT 不是免费的语法糖；监控上 PG 16+ 可看 `pg_stat_slru` 里 subtrans 行的读次数突增。InnoDB 无此悬崖（savepoint 只是 undo 回滚点，不影响他人读路径），这是 PG"把子事务状态摊给读者"设计的独有税种。

### 小结

- 判定框架：出生可见 ∧ 死亡不可见 → 行可见；xmin/xmax 两阶段完全对称，每阶段五连问：hint → 自己(curcid) → 快照 → CLOG → 推断 aborted；
- hint bit 是可再生缓存：崩溃丢失无害（CLOG 可重推），假阳性被 LSN 联锁排除；checksum 打破"torn page 无害"前提，于是需要 XLOG_FPI_FOR_HINT 赎回；
- XidInMVCCSnapshot 先边界裁剪（对子事务也正确），快路径 SIMD 搜数组，溢出后走 pg_subtrans 折算——65 个子事务触发全库性能悬崖；
- 备库快照的所有 XID 集中在 subxip，判断逻辑对称。

---

## 8.5 CLOG 与 SLRU：事务的"户口本"

### 8.5.1 clog.c：每事务 2 bit

CLOG（pg_xact 目录）为每个 XID 记录 4 种状态（`src/include/access/clog.h:27-30`）：

```c
#define TRANSACTION_STATUS_IN_PROGRESS      0x00
#define TRANSACTION_STATUS_COMMITTED        0x01
#define TRANSACTION_STATUS_ABORTED          0x02
#define TRANSACTION_STATUS_SUB_COMMITTED    0x03
```

每事务 2 bit（`clog.c:64`：`CLOG_BITS_PER_XACT = 2`），一个字节装 4 个事务，一个 8KB 页装 32K 个事务。寻址是纯算术（`clog.c:90-91`）：

```c
#define TransactionIdToByte(xid)    (TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid)  ((xid) % (TransactionId) CLOG_XACTS_PER_BYTE)
```

写入端 `TransactionIdSetStatusBit()`（`clog.c:669`）在 SLRU bank 锁保护下做移位或运算；`TransactionIdSetTreeStatus()`（`clog.c:192`）负责提交时"父 + 全部子事务"的原子性：若父子跨 CLOG 页，先把其他页的子事务标成 `SUB_COMMITTED`（中间态），最后在父事务所在页一次性把父标 `COMMITTED`——读者要么看到整棵树未提交、要么看到父已提交（子经 pg_subtrans 归于父），不会看到"一半提交"。

`SUB_COMMITTED` 的含义务必分清：它**不是**"子事务提交了"，而是"子事务本地完成、命运待顶层裁决"的占位符。

### 8.5.2 slru.c：简易 LRU 缓冲池

CLOG、pg_subtrans、multixact（offsets/members 两个）、commit_ts、SSI 的旧事务摘要（pg_serial）、NOTIFY 队列……这些"按整数索引、可回卷截断的元数据"共用一套缓冲机制 SLRU（Simple LRU，`src/backend/access/transam/slru.c`，头部注释是最好的导读）：

- 共享内存中一池 8KB 页缓冲，按页号低位分成若干 **bank**，每个 bank 一把控制 LWLock（PG 17 起的银行化改造；之前是一把大锁，曾是高并发下 CLOG/subtrans 查询的著名瓶颈，对应新 GUC `transaction_buffers`、`subtransaction_buffers`、`multixact_offset_buffers` 等）；
- 淘汰算法是朴素 LRU，但**永不淘汰最新页**（写流量集中在最新页）；
- 与 buffer manager 的关系：SLRU 是独立的小型缓冲系统，不走 shared_buffers，页面没有 LSN 概念的完整支持（CLOG 靠"提交前 WAL 已刷"保证恢复正确性；pg_subtrans 干脆不持久化，崩溃即弃）。

InnoDB 没有 CLOG 的直接对应物——它把提交状态藏在 trx 系统与 undo 段头里，靠回滚段实现同类语义。PG 的 CLOG 把"事务命运"抽成一个全局位图，才使"回滚 = 标 2 个 bit"和"hint bit 缓存"这些设计成为可能。

### 小结

- CLOG 每事务 2 bit、纯算术寻址；父子事务用 SUB_COMMITTED 中间态实现跨页原子提交；
- SLRU 是一族小型 LRU 缓冲池（CLOG/subtrans/multixact/serial/...），PG 17 起按 bank 分锁、容量可调；
- CLOG + hint bit + 快照三件套配合，才撑起"提交/回滚 O(1)"的 MVCC。

---

## 8.6 锁三层体系：spinlock → LWLock → 重量级锁

PG 的锁分三层，保护对象、持有时长、功能各不相同（`storage/lmgr/README` 开篇即此表）：

| 层 | 保护对象 | 持有时长 | 等待方式 | 死锁处理 |
|---|---|---|---|---|
| spinlock | 几个字长的共享变量 | 几十条指令 | 忙等+退避 | 无（不许阻塞） |
| LWLock | 共享内存数据结构 | 一段临界区 | 原子操作+信号量睡眠 | 无（靠编码纪律） |
| 重量级锁 | 数据库对象（表/元组/XID/...） | 事务结束才放 | 等待队列+睡眠 | 死锁检测 |

InnoDB 对照：InnoDB 同样是三层——`mutex/rw_lock_t`（对应 spinlock/LWLock 的混合体）与 lock_sys 的记录锁/表锁（对应重量级锁）。差别在于 PG 的行锁不进 lock_sys 式的锁表（8.7），以及 PG 的 LWLock 自 9.5 起是纯原子实现而 InnoDB 的 rw_lock 长期保留 spin+event 混合。

### 8.6.1 spinlock：硬件 TAS

`src/include/storage/s_lock.h` 按体系结构给出实现，x86_64 版（`s_lock.h:196-226`）：

```c
#define TAS(lock) tas(lock)
#define TAS_SPIN(lock)    (*(lock) ? 1 : TAS(lock))

static inline int
tas(volatile slock_t *lock)
{
    slock_t     _res = 1;

    __asm__ __volatile__(
        "   lock            \n"
        "   xchgb   %0,%1   \n"
:       "+q"(_res), "+m"(*lock)
:       /* no inputs */
:       "memory", "cc");
    return (int) _res;
}
```

- `lock xchgb`：原子交换——把 1 换进锁字节，换出来的旧值是 0 说明抢到了。`"memory"` clobber 兼作编译器屏障，硬件上 locked 指令自带全屏障；
- `TAS_SPIN` 的非锁定预读（`s_lock.h:203-210` 注释引 Intel 白皮书）：自旋等待时先普通读，避免每次都发起总线锁定的缓存行独占请求（否则持有者想解锁都要跟一群自旋者抢缓存行）；配套的 `SPIN_DELAY` 是 `rep; nop` 即 PAUSE 指令；
- 自旋失败会指数退避乃至 sleep（`s_lock.c` 的 `perform_spin_delay`），自旋过久报 "stuck spinlock"——因此**持锁区内绝不允许任何可能阻塞/出错的操作**，连 elog 都不行。

### 8.6.2 LWLock：一个原子字 + 等待队列

LWLock（`src/backend/storage/lmgr/lwlock.c`）支持共享/排他两种模式，保护缓冲区头、WAL 插入位置、ProcArray 等一切共享内存结构。核心是**一个 32 位原子状态字** `lock->state`，位布局（`lwlock.c:96-108`）：

```c
#define LW_FLAG_HAS_WAITERS         ((uint32) 1 << 31)   /* 等待队列非空 */
#define LW_FLAG_WAKE_IN_PROGRESS    ((uint32) 1 << 30)   /* 一轮唤醒正在进行 */
#define LW_FLAG_LOCKED              ((uint32) 1 << 29)   /* 等待队列的内嵌自旋位 */

#define LW_VAL_EXCLUSIVE            (MAX_BACKENDS + 1)   /* = 2^18 */
#define LW_VAL_SHARED               1

#define LW_SHARED_MASK              MAX_BACKENDS         /* 低 18 位: 共享持有者计数 */
#define LW_LOCK_MASK                (MAX_BACKENDS | LW_VAL_EXCLUSIVE)
```

一个 32 位字被切成三段：低 18 位是共享持有者计数（`MAX_BACKENDS = 2^18 - 1`，`procnumber.h:38-39`，所以计数永不溢出到上段）、第 18 位是排他位、高 3 位是标志。**把锁状态和队列状态压进同一个原子字**是整个设计的关键——释放锁的那一次原子减法**顺便**读回了"有没有人在等"（见 LWLockRelease 走读），不需要第二次同步操作。

**① 抢锁 `LWLockAttemptLock()`（`lwlock.c:764`）**——一个纯 CAS 循环，逐行拆解：

```c
static bool
LWLockAttemptLock(LWLock *lock, LWLockMode mode)
{
    uint32      old_state;

    old_state = pg_atomic_read_u32(&lock->state);       // 774: 循环外读一次

    while (true)
    {
        uint32      desired_state;
        bool        lock_free;

        desired_state = old_state;

        if (mode == LW_EXCLUSIVE)
        {
            lock_free = (old_state & LW_LOCK_MASK) == 0;    // 786: 无任何持有者
            if (lock_free)
                desired_state += LW_VAL_EXCLUSIVE;
        }
        else
        {
            lock_free = (old_state & LW_VAL_EXCLUSIVE) == 0; // 792: 无排他持有者
            if (lock_free)
                desired_state += LW_VAL_SHARED;              // 共享计数 +1
        }

        if (pg_atomic_compare_exchange_u32(&lock->state,
                                           &old_state, desired_state))  // 807
        {
            if (lock_free)
                return false;   /* 得手 */
            else
                return true;    /* somebody else has the lock */
        }
        /* CAS 失败: old_state 已被更新为当前值, 直接重试 */
    }
}
```

- 排他模式检查 `LW_LOCK_MASK`（共享计数与排他位都得是 0），共享模式只检查排他位——共享者之间用**加法**并存（每人 +1），这就是"读读并行"的实现；
- 微妙之处（`lwlock.c:797-805` 注释）：**即使看到锁不空闲，也执行一次"原值换原值"的 CAS**——因为 CAS 兼作全内存屏障，后续"入队再重试"协议依赖这个屏障保证可见性顺序；实测拆成"仅在空闲时 CAS"并不更快；
- CAS 失败时 `old_state` 被硬件带回最新值，循环不需要重新 read——教科书级的 CAS 循环写法。

**② `LWLockAcquire()`（`lwlock.c:1150`）主循环**——解决经典的"检查-睡眠竞态（lost wakeup）"，手法是**入队后再试一次**：

```c
    HOLD_INTERRUPTS();                          // 1189: 临界区内不响应 cancel/die

    for (;;)
    {
        bool        mustwait;

        mustwait = LWLockAttemptLock(lock, mode);       // 1215: 第一次尝试
        if (!mustwait)
            break;                                      /* got the lock */

        LWLockQueueSelf(lock, mode);                    // 1235: 挂入等待队列

        /* we're now guaranteed to be woken up if necessary */
        mustwait = LWLockAttemptLock(lock, mode);       // 1238: 入队后再试!

        if (!mustwait)
        {
            LWLockDequeueSelf(lock);                    // 1245: 撤销排队, 拿锁走人
            break;
        }

        for (;;)
        {
            PGSemaphoreLock(proc->sem);                 // 1269: 睡在自己的信号量上
            if (proc->lwWaiting == LW_WS_NOT_WAITING)
                break;
            extraWaits++;                               // 吸收了多余的唤醒, 记账
        }

        /* Retrying, allow LWLockRelease to release waiters again. */
        pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_WAKE_IN_PROGRESS);  // 1276

        /* Now loop back and try to acquire lock again. */
        result = false;
    }

    held_lwlocks[num_held_lwlocks].lock = lock;         // 1301: 记入持有列表
    held_lwlocks[num_held_lwlocks++].mode = mode;

    while (extraWaits-- > 0)                            // 1307: 归还多吸收的信号量
        PGSemaphoreUnlock(proc->sem);
```

三个"为什么"：

- **为什么入队后必须再试一次**（`lwlock.c:1223-1231` 注释）？第一次失败到入队之间存在窗口：持有者可能恰在此间释放并检查等待队列——那时我还没入队，它认为无人可醒。若我径直睡去，就再也没人叫我（lost wakeup）。入队动作**先于**第二次检查（中间隔着 LWLockQueueSelf 内部的锁操作＝内存屏障），于是二者必居其一：要么第二次尝试看到锁已空闲（拿锁撤队），要么释放者看到我的队列条目（负责唤醒我）。不存在两边都看不见对方的交错；
- **为什么被唤醒者不直接继承锁（no lock handoff）**（`lwlock.c:1195-1205` 注释，出处是 2001-12-29 的邮件列表讨论）？LWLock 临界区极短，持有者在一个时间片内会反复拿放同一把锁。若释放时把锁"移交"给睡着的等待者，锁在移交完成前对所有人不可用，而唤醒-调度一个进程要几微秒——等于把纳秒级临界区拉长成微秒级，吞吐崩塌。所以释放者只"叫醒"，被叫醒者**重新竞争**；代价是刚醒的进程可能又输给插队的活跃进程（见下面的公平性讨论）；
- **为什么会有 extraWaits**？信号量可能被其他机制多 up 一次（历史上 ProcWaitForSignal 等共用信号量）；醒来发现 `lwWaiting` 还没被清就再睡，多吸收的计数最后原样归还——保证信号量计数守恒。

**③ `LWLockQueueSelf`/`LWLockDequeueSelf`（`lwlock.c:1018/1061`）**：队列由 `LW_FLAG_LOCKED` 位当自旋锁保护（`LWLockWaitListLock`，`lwlock.c:835`——用 `fetch_or` 抢标志位，抢不到就普通读自旋）。入队时置 `LW_FLAG_HAS_WAITERS`、`lwWaiting = LW_WS_WAITING`、按模式压入队尾。撤队时的棘手情形（`lwlock.c:1096-1119`）：可能**已经被别人从队列摘下并安排了唤醒**（`lwWaiting == LW_WS_PENDING_WAKEUP`）——那就得把这次多余的唤醒睡过去吸收掉，否则信号量计数错乱。

**④ `LWLockRelease()`（`lwlock.c:1767`）**：

```c
    /* 1778-1789: 从 held_lwlocks[] 反向找到本锁并移除 */

    if (mode == LW_EXCLUSIVE)
        oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_EXCLUSIVE);  // 1798
    else
        oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_SHARED);

    if ((oldstate & LW_FLAG_HAS_WAITERS) &&
        !(oldstate & LW_FLAG_WAKE_IN_PROGRESS) &&
        (oldstate & LW_LOCK_MASK) == 0)                 // 1812-1814
        check_waiters = true;
    else
        check_waiters = false;

    if (check_waiters)
        LWLockWakeup(lock);                             // 1827

    RESUME_INTERRUPTS();
```

一次原子减法（`sub_fetch` 返回减完的新值）同时完成"释放持有"与"读取队列状态"。仅当**有等待者 ∧ 没有一轮唤醒正在进行 ∧ 锁已完全空闲**（我是最后一个共享持有者/唯一排他者）时才进 `LWLockWakeup()`。`LW_FLAG_WAKE_IN_PROGRESS` 是防"唤醒风暴"的节流阀：一轮唤醒发出后、被唤醒者真正跑起来之前，后续的释放者不再重复唤醒（被唤醒者在 `lwlock.c:1276` 清掉此标志后风门重开）。

**⑤ `LWLockWakeup()`（`lwlock.c:904`）——唤醒的批量语义**：锁住等待队列，从队头开始摘人：

```c
    proclist_foreach_modify(iter, &lock->waiters, lwWaitLink)
    {
        PGPROC *waiter = GetPGProcByNumber(iter.cur);

        if (wokeup_somebody && waiter->lwWaitMode == LW_EXCLUSIVE)
            continue;                                   // 920: 已醒过人, 跳过排他等待者
        proclist_delete(&lock->waiters, iter.cur, lwWaitLink);
        proclist_push_tail(&wakeup, iter.cur, lwWaitLink);
        ...
        waiter->lwWaiting = LW_WS_PENDING_WAKEUP;       // 948
        if (waiter->lwWaitMode == LW_EXCLUSIVE)
            break;                                      // 955: 醒了一个排他者就停
    }
```

效果：队头若是排他等待者→只醒它一个；队头若是共享等待者→醒**一串连续的**共享等待者（直到遇上排他等待者为止）。这正是读写锁的经典批量唤醒策略。摘完后在一次 CAS 里同时清 `LW_FLAG_LOCKED`、按需置/清 `WAKE_IN_PROGRESS`、`HAS_WAITERS`（`lwlock.c:960-986`），最后逐个 `PGSemaphoreUnlock`。`lwlock.c:996-1006` 的写屏障保证"从链表摘除"先于"lwWaiting 置 NOT_WAITING"对外可见——否则刚醒的进程立刻去排别的锁队，链表指针还挂在旧队列上，队列结构直接损坏。

**⑥ 公平性：为什么（实践中）无饥饿？**诚实地说，LWLock **不提供理论上的无饥饿保证**——新来者可以在被唤醒者调度前插队抢锁（这正是不做 handoff 的代价）。但三个机制合力把饥饿压到实践中可忽略：

1. **FIFO 队列**：一旦进入队列，相对次序固定；唤醒永远从队头开始，排他等待者不会被后来的排他等待者越过；
2. **入队后再试**的窗口极窄（几条指令），插队只可能发生在"锁恰好空闲的一瞬"；
3. 临界区极短意味着锁快速轮转，被插队一次的等待者下一轮大概率轮到。

与之对照，重量级锁必须提供强得多的公平性（等待队列严格排队 + tuple 锁排队票，8.7.4），因为它的持有时长是"事务级"的——插队一次可能等几分钟。**锁的公平性设计必须与持有时长匹配**，这是贯穿三层锁的一条设计红线。

**⑦ 无死锁纪律**：LWLock 没有死锁检测，靠两条编码纪律兜底：(a) **固定顺序**——需要多把 LWLock 时按全局约定的顺序拿（如 buffer 锁按页号、lock 分区锁按分区号升序）；(b) **持锁不越界**——持 LWLock 期间不调用可能拿"更高层"锁或阻塞的代码（不进重量级锁、不做 I/O 等待用户）。`HOLD_INTERRUPTS()`（`lwlock.c:1189`）保证临界区内不响应 cancel/die——否则错误处理的 longjmp 会带着锁飞出临界区。违反纪律的后果见 8.10 不变式清单 I-7。

### 8.6.3 重量级锁：lock.c

这才是用户视角的"锁"（`pg_locks` 里看到的东西）。被锁对象用 16 字节的 LOCKTAG 描述（`src/include/storage/locktag.h`）：

```c
typedef struct LOCKTAG
{
    uint32      locktag_field1; /* a 32-bit ID field */
    uint32      locktag_field2;
    uint32      locktag_field3;
    uint16      locktag_field4;
    uint8       locktag_type;   /* see enum LockTagType */
    uint8       locktag_lockmethodid;
} LOCKTAG;
```

`locktag_type` 枚举了可锁对象：RELATION（field1=dbOid, field2=relOid）、PAGE、TUPLE、**TRANSACTION**（等一个 XID 结束——行锁等待的真身，8.7）、VIRTUALTRANSACTION、OBJECT、ADVISORY 等。

**8 级锁模式与冲突矩阵**：表锁有 8 个模式（AccessShare < RowShare < RowExclusive < ShareUpdateExclusive < Share < ShareRowExclusive < Exclusive < AccessExclusive），语义完全由一张位掩码表定义（`src/backend/storage/lmgr/lock.c:68`）：

```c
static const LOCKMASK LockConflicts[] = {
    0,
    /* AccessShareLock */
    LOCKBIT_ON(AccessExclusiveLock),
    /* RowExclusiveLock */
    LOCKBIT_ON(ShareLock) | LOCKBIT_ON(ShareRowExclusiveLock) |
    LOCKBIT_ON(ExclusiveLock) | LOCKBIT_ON(AccessExclusiveLock),
    ...
    /* AccessExclusiveLock: 与所有 8 个模式冲突 */
    ...
};
```

读懂两行就够了：SELECT 拿 AccessShare，只与 AccessExclusive（DDL）冲突——**读几乎不挡任何东西**；普通 DML 拿 RowExclusive，相互不冲突——**写与写在表级也不冲突**（行级冲突由 xmax 机制处理）。这张矩阵与 InnoDB 的意向锁体系（IS/IX/S/X）神似，PG 的前四级本质上就是意向锁，只是 PG 把 DDL 相关的模式也编进同一张表里。

**LockAcquireExtended 完整路径**（`lock.c:806` 的 `LockAcquire` 是薄封装 → `LockAcquireExtended()`，`lock.c:833`），按代码顺序：

1. **本地锁表**（`lock.c:891-944`）：以 (LOCKTAG, mode) 查 backend 私有哈希 `LockMethodLocalHash` 得 LOCALLOCK；`locallock->nLocks > 0` 说明本事务已持有同款锁，`GrantLockLocal` 加引用计数直接返回——**重入完全不碰共享内存**。这就是同一事务反复 `LOCK TABLE` 或嵌套函数重复加锁近乎免费的原因；
2. **热备防线**（`lock.c:861-869`）：恢复中的备库拒绝 RowExclusive 以上的对象锁——备库不可写，强锁只会与回放冲突；
3. **fastpath 快路径**（`lock.c:984-1026`，详见下）；
4. **强锁封路与搬运**（`lock.c:1034-1055`，详见下）；
5. **共享锁表**（`lock.c:1062-1095`）：按 LOCKTAG 哈希选中 16 个分区之一（`NUM_LOCK_PARTITIONS = 16`，`lwlock.h:86-87`），拿分区 LWLock 排他，`SetupLockInTable` 找/建 LOCK 对象（每把锁一个：持有者位掩码 grantMask、等待者位掩码 waitMask、PROCLOCK 链、等待队列）与 PROCLOCK 对象（锁×进程一个）。注意 `lock.c:1069-1073` 注释：LOCALLOCK 里缓存的 lock/proclock 指针**不可信**——零引用的共享对象随时可能被回收重建，必须重查；
6. **冲突检查**（`lock.c:1102-1122`）：先看 `conflictTab[lockmode] & lock->waitMask`——**与等待者冲突也算冲突**（防插队饿死排队者，这里就是重量级锁比 LWLock 公平的代码落点）；再 `LockCheckConflicts` 对持有者位掩码按位与。无冲突 `GrantLock`；有冲突则 `JoinWaitQueue` 挂入 `lock->waitProcs` 并在 `WaitOnLock → ProcSleep` 里睡眠（8.8 的死锁检测定时器在此武装）；
7. 事务结束时 `LockReleaseAll` 统一释放（重量级锁持有到事务末尾——这一点与 2PL 一致，也是死锁可能形成的前提）。

**fastpath 锁优化（PG 9.2，commit `3cba8999b3`，2011-05-28，Robert Haas）——弱锁不进共享锁表**。动机：每条查询都要给涉及的每张表拿 AccessShare 锁，全走共享哈希表意味着高并发下 16 个分区锁疯狂碰撞——单纯的 `SELECT 1` 压测都能把分区锁打成瓶颈——而这些弱锁 99.99% 的情况下根本无人冲突。判定条件（`lock.c:270`）：

```c
#define EligibleForRelationFastPath(locktag, mode) \
    ((locktag)->locktag_lockmethodid == DEFAULT_LOCKMETHOD && \
    (locktag)->locktag_type == LOCKTAG_RELATION && \
    (locktag)->locktag_field1 == MyDatabaseId && \
    MyDatabaseId != InvalidOid && \
    (mode) < ShareUpdateExclusiveLock)
```

即：本库的表锁、模式为最弱三级（AccessShare/RowShare/RowExclusive，称"弱锁"；ShareUpdateExclusive 及以上称"强锁"）。核心观察：**弱锁与弱锁互不冲突**（查冲突矩阵可证），所以只要能保证"没有强锁存在"，弱锁之间根本无需互查——各自记在自家账本上就行。

**弱锁方路径**（`lock.c:984-1026`）：

```c
    if (EligibleForRelationFastPath(locktag, lockmode))
    {
        if (FastPathLocalUseCounts[FAST_PATH_REL_GROUP(locktag->locktag_field2)] <
            FP_LOCK_SLOTS_PER_GROUP)                    // 组内还有空槽
        {
            uint32  fasthashcode = FastPathStrongLockHashPartition(hashcode);
            bool    acquired;

            LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE);   // 998
            if (FastPathStrongRelationLocks->count[fasthashcode] != 0)
                acquired = false;                       // 999: 有强锁在, 放弃快路
            else
                acquired = FastPathGrantRelationLock(locktag->locktag_field2,
                                                     lockmode); // 1002
            LWLockRelease(&MyProc->fpInfoLock);
            if (acquired)
            {
                locallock->lock = NULL;                 // 1012: 清掉陈旧指针!
                locallock->proclock = NULL;
                GrantLockLocal(locallock, owner);
                return LOCKACQUIRE_OK;                  // 全程未碰共享锁表
            }
        }
        ...
    }
```

`FastPathGrantRelationLock()`（`lock.c:2790`）在自己 PGPROC 的 fastpath 槽位数组里找空槽记下 (relid, 模式位图)——槽位按 relid 哈希分组，每组 16 槽（`FP_LOCK_SLOTS_PER_GROUP`，`proc.h:102`；组数自 PG 18 起随 `max_locks_per_transaction` 伸缩，commit `c4d5cb71d2`，2024-09-21，此前固定 16 槽全局、多分区表场景频繁溢出）。`MyProc->fpInfoLock` 是**每 backend 一把**的 LWLock——正常情况下只有自己拿它，零争用。

**强锁方路径**（`lock.c:1034-1055`）：

```c
    if (ConflictsWithRelationFastPath(locktag, lockmode))
    {
        uint32  fasthashcode = FastPathStrongLockHashPartition(hashcode);

        BeginStrongLockAcquire(locallock, fasthashcode);        // 1038: 计数器+1, 封路
        if (!FastPathTransferRelationLocks(lockMethodTable, locktag,
                                           hashcode))           // 1039: 搬运
        { ... /* 共享内存不足, 回滚强锁计数并报错 */ }
    }
```

`FastPathTransferRelationLocks()`（`lock.c:2869`）走读：

```c
    for (i = 0; i < ProcGlobal->allProcCount; i++)      // 2885: 遍历所有 backend!
    {
        PGPROC *proc = GetPGProcByNumber(i);

        LWLockAcquire(&proc->fpInfoLock, LW_EXCLUSIVE); // 2890: 拿对方的 fp 锁

        if (proc->databaseId != locktag->locktag_field1 ||
            proc->fpLockBits[group] == 0)               // 2909: 非本库/组内无锁, 跳过
        {
            LWLockRelease(&proc->fpInfoLock);
            continue;
        }

        for (j = 0; j < FP_LOCK_SLOTS_PER_GROUP; j++)
        {
            uint32 f = FAST_PATH_SLOT(group, j);

            if (relid != proc->fpRelId[f] || FAST_PATH_GET_BITS(proc, f) == 0)
                continue;

            LWLockAcquire(partitionLock, LW_EXCLUSIVE);
            for (lockmode = FAST_PATH_LOCKNUMBER_OFFSET; ...; ++lockmode)
            {
                if (!FAST_PATH_CHECK_LOCKMODE(proc, f, lockmode))
                    continue;
                proclock = SetupLockInTable(lockMethodTable, proc, locktag,
                                            hashcode, lockmode);   // 2937
                if (!proclock) { ...return false; }     /* 锁表满 */
                GrantLock(proclock->tag.myLock, proclock, lockmode);
                FAST_PATH_CLEAR_LOCKMODE(proc, f, lockmode);       // 2946
            }
            LWLockRelease(partitionLock);
            break;
        }
        LWLockRelease(&proc->fpInfoLock);
    }
    return true;
```

把每个 backend 藏在 fastpath 槽里的该表弱锁**以对方的名义**（注意 `SetupLockInTable` 的第二个参数是 `proc` 不是 `MyProc`）搬进共享锁表并清掉槽位，然后强锁方自己按常规流程做冲突检查——此时所有既存弱锁都已在锁表里现形，冲突检查不会漏人。

**正确性论证**（`lock.c:992-997` 注释是原始出处）：弱锁本地化后，凭什么保证强锁必能看到它们？关键是两个操作序列的**屏障交错**：

- 弱锁方序列：`Acquire(fpInfoLock)` → 读强锁计数器 C → 写自己的槽位 → `Release(fpInfoLock)`；
- 强锁方序列：原子递增计数器 C（BeginStrongLockAcquire 内部） → 对每个 proc：`Acquire(其 fpInfoLock)` → 读其槽位 → 搬运。

LWLock 的获取/释放自带全屏障。逐情形穷举：若弱锁方读 C 时看到非零 → 它退回常规路径，锁表冲突检查兜底；若看到零，说明它读 C **先于**强锁方的递增全局可见——而它写槽位被夹在同一把 fpInfoLock 临界区内，强锁方随后拿同一把 fpInfoLock 扫槽位时（锁的互斥+屏障保证）**必然看到**这个已写入的槽位并搬走它。不存在"弱锁方走了快路、强锁方又没看到槽位"的交错——两边至少有一方会退到共享锁表上相遇。这是一个教科书级的 Dekker 式发布协议：计数器是强锁方的"公告"，槽位是弱锁方的"登记"，fpInfoLock 是仲裁点。

**代价转嫁的账**：DDL（强锁）方要遍历 `allProcCount` 个 PGPROC、每个拿一次 fpInfoLock——把成本从每秒百万次的弱锁路径转嫁到每天几次的 DDL 路径。这就是 `pg_locks` 里 `fastpath = true` 行的来历，也是"连接数几千的库上 DDL/DROP 变卡"的一个原因（还叠加 8.8 的锁表全分区扫描）。工程启示：**优化高频路径时，把不变量的维护成本推给低频路径，是共享内存设计的通用套路**——本章已见三例（fastpath、GlobalVis 惰性化、组退出）。

### 小结

- spinlock：TAS 原子指令 + 自旋退避，保护几条指令的临界区，持锁区内禁止一切可出错操作；
- LWLock：单个 32 位原子字（18 位共享计数 + 排他位 + 3 个标志位）+ FIFO 等待队列；"入队后再试"消除 lost wakeup；释放者只唤醒不移交，被唤醒者重抢——理论上可饥饿，实践上由 FIFO 队列 + 窄插队窗 + 短临界区压制；WAKE_IN_PROGRESS 节流唤醒风暴；无死锁检测，靠固定顺序纪律；
- 重量级锁：LOCKTAG 标识对象、LockConflicts 位掩码矩阵定义 8 模式语义、共享哈希 16 分区、持有到事务结束、等待者优先（waitMask 也算冲突）、有死锁检测；
- fastpath：弱锁记在自家 PGPROC 槽位（每组 16 槽，PG 18 起组数可扩），强锁方"计数器封路 + 遍历搬运"；正确性由 fpInfoLock 的屏障交错保证——两边至少一方退到共享锁表相遇。

---

## 8.7 行锁：写进元组的锁

### 8.7.1 所以然：行锁为什么进 xmax 而不进锁表？——内存量化对比

InnoDB 的行锁（记录锁、间隙锁、next-key 锁）是内存中的锁结构；PG 的行锁**根本不在锁表里**：给一行加锁 = 把自己的 XID 写进该行的 `t_xmax`，并用 infomask 标注"这只是锁不是删除"。先把两种方案的账算清楚：

**方案 A（锁表方案）：`SELECT ... FOR UPDATE` 锁一百万行。**PG 的共享锁表每条锁要一个 LOCK 对象 + 一个 PROCLOCK 对象（合计约 200~300 字节，含哈希开销），一百万行 ≈ **200~300 MB 共享内存**。更致命的是：PG 的锁表是**启动时定容**的共享内存（容量 ≈ `max_locks_per_transaction × (max_connections + max_prepared_transactions)`，默认 64 × ~100 ≈ **6400 条**），一百万行锁在默认配置下连零头都装不下——第 6400 行就报 "out of shared memory"。传统数据库对此的补丁是**锁升级**（SQL Server/DB2：行锁太多就升成表锁），代价是并发度骤降和升级本身引发的死锁。InnoDB 用每页位图压缩（一个锁结构覆盖一页内多行），一百万行约几 MB，好得多但仍是 buffer pool 里的真实内存，且锁结构本身参与 lock_sys 互斥的争用。

**方案 B（PG 的 xmax 方案）：同样一百万行。**共享内存增量 **0 字节**——锁状态写在每行元组头**本来就存在**的 xmax 字段（4 字节，复用不新增）+ infomask 位里。无容量上限、无锁升级、锁多少行都不会 "out of memory"。

天下没有免费的午餐，代价三笔：

1. **写放大**：加锁要改元组头 → 弄脏页面 → 写 WAL（`XLOG_HEAP_LOCK`）→ 最终落盘。锁一百万行 = 弄脏几万个页 + 相应 WAL。InnoDB 加锁是纯内存操作（锁结构不落盘、不进 redo）。所以 PG 的 `SELECT FOR UPDATE` 大批量执行会产生惊人的 I/O，这在方案 A 里是不存在的；
2. **multixact 复杂度**（8.7.3）：xmax 只有一个槽位，多个事务共锁一行时必须引入"组合 ID"间接层——一整套 SLRU、独立回卷、独立 freeze，是 PG 代码里公认的高危区（9.3/9.4 的著名数据损坏 bug 群，见 8.11）；
3. **xmax 语义重载**：同一个字段既表示"删除者"又表示"锁定者"还可能是"multixact 组合"，靠 infomask 的位组合区分（`HEAP_XMAX_LOCK_ONLY`/`HEAP_XMAX_IS_MULTI`/...）。8.4.2 走读里 xmax 阶段的分支爆炸（[M2][M3][M4]…）正是这笔账——**每个读者都要替锁买单**：可见性判断必须先排除"xmax 只是锁"的情形。

还有一个结构性代价：**锁状态随元组走，释放就没有"遍历我的锁"的入口**——所以 PG 的行锁**不逐个释放**，事务结束时行上的 xmax 自然失效（读者查 CLOG 发现 locker 已完结）。这也是为什么等待行锁不能"等那一行"，而要绕道：**等行锁 = 等持有者的事务结束 = 对持有者的 XID 拿一把 `LOCKTAG_TRANSACTION` 共享锁**（每个事务在 `AssignTransactionId` 时就对自己的 XID 持有排他"事务锁"）。事务提交/回滚释放事务锁，等待者全部醒来。死锁检测因此依然有效——行锁等待在等待图里表现为"等某个事务锁"，这是 8.8 的输入。

顺带回答一个 MySQL 用户必问的问题：**PG 没有间隙锁**。防幻读不靠锁间隙，RR 靠快照天然无幻读（读已冻结的世界），SERIALIZABLE 靠 SSI 的谓词锁检测（8.9）。因此 PG 也没有 InnoDB 那些"间隙锁导致的意外死锁"，但代价是 SERIALIZABLE 下可能事后回滚。

### 8.7.2 四种行锁模式与 infomask 编码

`src/include/nodes/lockoptions.h:53-59` 定义四种模式（从弱到强）：

| 模式 | SQL | 冲突语义 |
|---|---|---|
| LockTupleKeyShare | `FOR KEY SHARE` | 只挡"改键/删行"，允许改非键列 |
| LockTupleShare | `FOR SHARE` | 挡一切修改，允许共享 |
| LockTupleNoKeyExclusive | `FOR NO KEY UPDATE`（普通 UPDATE 隐含） | 挡其他排他，允许 KeyShare |
| LockTupleExclusive | `FOR UPDATE`（DELETE/改键 UPDATE 隐含） | 全排他 |

四种模式塞进 infomask 的几个位（`htup_details.h:194-200`）：

```c
#define HEAP_XMAX_KEYSHR_LOCK   0x0010  /* xmax is a key-shared locker */
#define HEAP_XMAX_EXCL_LOCK     0x0040  /* xmax is exclusive locker */
#define HEAP_XMAX_LOCK_ONLY     0x0080  /* xmax, if valid, is only a locker */
#define HEAP_XMAX_SHR_LOCK  (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)
```

`HEAP_XMAX_LOCK_ONLY` 就是可见性判断里反复出现的那个"xmax 只是锁"标志（[M2] 分支）。KeyShare/NoKeyUpdate 这对模式是 PG 9.3 为外键并发引入的（commit `0ac5ad5134`，2013-01-23，Álvaro Herrera，"Improve concurrency of foreign key locking"）：外键检查只需锁"键不变"（KEY SHARE），普通 UPDATE 只锁"非键更新"（NO KEY UPDATE），两者不冲突——此前外键检查用 FOR SHARE，与一切 UPDATE 冲突，"带外键的表并发更新雪崩"是 9.3 之前的经典投诉。

### 8.7.3 multixact：多个事务锁同一行

共享类锁允许多个事务同时持有，但 xmax 只有一个 32 位槽位怎么办？答案是 **MultiXactId**（`src/backend/access/transam/multixact.c`，头部注释是最佳导读）：造一个新的"组合 ID"写进 xmax，并置 `HEAP_XMAX_IS_MULTI`（`htup_details.h:209`）标志；组合 ID 在 pg_multixact 的**两个 SLRU**（offsets：multi→成员区间起点；members：成员数组）里展开，每个成员 = (XID, 锁模式/更新标志)。

- 创建：`MultiXactIdCreate()`（`multixact.c:358`）/ `MultiXactIdCreateFromMembers()`（`multixact.c:715`）；已有 multixact 再加成员则要**新建**一个包含全部旧成员+新成员的 multixact（multixact 不可变——SLRU 里的成员数组是追加写的定长区间，没法原地扩）；
- 读取：`GetMultiXactIdMembers()`（`multixact.c:1172`）。可见性判断中 `HeapTupleGetUpdateXid()`（[X2d]/[M3] 分支）就是靠它从成员里挑出真正做了 UPDATE 的那一个（至多一个成员是 updater，其余是纯 locker）；
- 0ac5ad5134 带来的重大语义变化：9.3 起 multixact 的成员可以包含 **updater**（此前 multi 只装 locker）——于是 multixact 从"可丢弃的临时数据"变成了**参与可见性判断的持久数据**，必须崩溃安全、必须 freeze。这一步棋直接引出了 9.3/9.4 的 bug 风暴（members 区间回卷计算错误、freeze 丢 updater 等，如 commit `b0b263baab` "Wrap multixact/members correctly during extension, take 2"，2014-06-09）。运维上监控 `datminmxid` 年龄与监控 XID 年龄同等重要，`vacuum_multixact_freeze_*` 参数族因此存在。

### 8.7.4 SELECT FOR UPDATE 的路径：heap_lock_tuple

`SELECT ... FOR UPDATE` 的执行计划顶端是 LockRows 节点（`nodeLockRows.c`），对每行经 `table_tuple_lock()`（`heapam_handler.c:265`）落到堆实现 `heap_lock_tuple()`（`src/backend/access/heap/heapam.c:4728`）。主线流程：

1. 读元组，`HeapTupleSatisfiesUpdate()` 判断当前状态：无人占用 → 直接走 3；被占用 → 走 2；
2. **冲突等待**：根据双方模式查 `tupleLockExtraInfo` 表（四种模式对应的重量级锁级别）决定是否冲突。冲突则视 `wait_policy` 三选一：阻塞等待（`XactLockTableWait` 等持有者的事务锁，multixact 则逐成员等）、`SKIP LOCKED` 跳过、`NOWAIT` 报错。等待前还会先拿该元组的 `LOCKTAG_TUPLE` 重量级锁作为"排队票"，防止饥饿（醒来的人凭它优先接手，接手后立刻释放——所以 pg_locks 里 tuple 锁一闪而过）。回顾 8.6.2 ⑥的红线：行锁等待是分钟级的，必须有强公平性，排队票就是它的实现；
3. **落锁**：`compute_new_xmax_infomask()` 算出新 xmax（自己的 XID，或与现存 locker 合并成 multixact）与 infomask 位组合，写入元组头，记 WAL（`XLOG_HEAP_LOCK`），完事。

READ COMMITTED 下若行在等待期间被改了，上层 LockRows 用 EPQ 拿新版本重验 WHERE；REPEATABLE READ 下直接报串行化失败——与 8.3.6 的语义一致。

省略说明：`heap_lock_tuple` 全文近 700 行，其中约六成处理 multixact 模式合并与升级等待的组合爆炸（4 种已有状态 × 4 种请求模式 × wait_policy），主干即上述三步，故不逐行走读；`heap_update` 内嵌的"更新即加锁"路径与此同构。

### 小结

- 行锁写在元组 xmax + infomask 里：共享内存零占用、无容量上限、无锁升级；代价是加锁产生页写与 WAL、multixact 间接层的复杂度、xmax 语义重载摊薄每次可见性判断；
- 等行锁 = 等持有者的 LOCKTAG_TRANSACTION 事务锁；公平性由一闪而过的 LOCKTAG_TUPLE 排队票维持；
- 四种行锁模式（Key Share / Share / No Key Update / Update）为外键并发而精细化（9.3, 0ac5ad5134）；
- 多事务共持一行时 xmax 换成 MultiXactId（不可变、双 SLRU、独立回卷/freeze）；9.3 让 updater 进 multi 后它成了持久关键数据，酿出著名 bug 群；
- PG 无间隙锁：RR 靠快照防幻读，SERIALIZABLE 靠 SSI。

---

## 8.8 死锁检测：deadlock.c

### 8.8.1 所以然：为什么懒触发而不是即时检测？——代价账

PG 不在每次加锁时做死锁预防/即时检测，而是**先睡下去，睡满 `deadlock_timeout`（默认 1s，`proc.c:62`）才起来查一次**。实现：`ProcSleep` 挂起前 `enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout)`（`proc.c:1438`），定时器到点置标志，等待循环里发现后调用 `CheckDeadLock()`（`proc.c:1530` 调用，实现 `proc.c:1887`）。

把两种策略的成本摆开算：

- **即时检测**（InnoDB 8.0 的默认做法：每次锁等待都从等待者出发做 DFS）：设每秒 B 次锁等待事件，每次 DFS 成本 O(等待图规模)。更糟的是 PG 的检测前提——**锁住全部 16 个锁表分区**（`DeadLockCheck` 的调用约定，`deadlock.c:213` "Caller must already have locked all partitions"）——这等于每次锁等待都短暂冻结全库的加锁/放锁。高并发行锁冲突场景（秒杀式热点行）下，B 可达数千/秒，检测本身会成为比死锁更大的灾难。InnoDB 在 8.0 里也承认了这一点：加了 `innodb_deadlock_detect=OFF` 开关，热点场景下建议关掉检测、纯靠 `innodb_lock_wait_timeout` 兜底——绕了一圈回到 PG 的思路；
- **懒触发**：只有等待超过 1 秒的进程才付一次检测成本。两个概率事实支撑它：(a) 设计良好的应用里绝大多数锁等待在毫秒级结束——它们**永远不触发检测**；(b) 真死锁不会自己消失——晚 1 秒检测不损失正确性，只增加 1 秒的死锁定格时间，而死锁本身是应用 bug，罕见事件的首次响应延迟 1 秒完全可接受。定量地说：若 99.9% 的等待 < 1s，懒触发把检测频率压掉三个数量级，同时把"全分区冻结"从热路径彻底移除；
- 检测一次判"无死锁"后**不再重复检测**（继续睡）：死锁的形成需要"新边加入等待图"，而新边意味着**别的进程**开始等待，那个进程自己会武装定时器并负责检测包含新边的图。归纳可证：任何死锁环形成后，最后加入环的那个进程的定时器必然在环形成之后到期，它会看到完整的环——**每个环至少被检测一次**，无漏报。

`deadlock_timeout` 的调参哲学由此清晰：它是"检测灵敏度 vs 检测开销"的旋钮。锁冲突极多的系统可以调大（减少无谓检测），交互式系统可以调小（更快发现死锁）；它同时还是 `log_lock_waits` 的打点阈值。

### 8.8.2 DeadLockCheck 主线走读

等待图（wait-for graph, WFG）：节点 = 进程；**硬边** A→B = "A 等待 B 已**持有**的锁"；PG 还独有**软边** = "A 等待 B 只因 B 在同一把锁的等待队列里**排在 A 前面**且请求模式与 A 冲突"——软边对应的死锁可以通过**重排队列**化解，不必杀任何事务。这是 PG 死锁检测最优雅的一笔：它不仅判"有没有死锁"，还搜索"能否通过调整排队次序让死锁根本不成立"。

**入口 `DeadLockCheck()`（`deadlock.c:220`）**：

```c
DeadLockState
DeadLockCheck(PGPROC *proc)
{
    nCurConstraints = 0;
    nPossibleConstraints = 0;
    nWaitOrders = 0;
    blocking_autovacuum_proc = NULL;

    if (DeadLockCheckRecurse(proc))                     // 231
    {
        /* 无解: 重跑一次 FindLockCycle 填 deadlockDetails[] 供报错打印 */
        int  nSoftEdges;

        nWaitOrders = 0;
        if (!FindLockCycle(proc, possibleConstraints, &nSoftEdges))   // 242
            elog(FATAL, "deadlock seems to have disappeared");
        return DS_HARD_DEADLOCK;                        // 245: 调用者将中止本事务
    }

    /* 有解: 按 waitOrders[] 重排相关等待队列 */
    for (int i = 0; i < nWaitOrders; i++)               // 249
    {
        LOCK   *lock = waitOrders[i].lock;
        PGPROC **procs = waitOrders[i].procs;
        ...
        dclist_init(waitQueue);                          // 263: 清空重排
        for (int j = 0; j < nProcs; j++)
            dclist_push_tail(waitQueue, &procs[j]->waitLink);

        ProcLockWakeup(GetLocksMethodTable(lock), lock); // 272: 唤醒不再冲突者
    }

    if (nWaitOrders > 0)
        return DS_SOFT_DEADLOCK;
    else if (blocking_autovacuum_proc != NULL)
        return DS_BLOCKED_BY_AUTOVACUUM;                 // 279
    else
        return DS_NO_DEADLOCK;
}
```

**`DeadLockCheckRecurse()`（`deadlock.c:312`）——在"约束空间"里做回溯搜索**：

```c
static bool
DeadLockCheckRecurse(PGPROC *proc)
{
    int  nEdges = TestConfiguration(proc);              // 319
    if (nEdges < 0)
        return true;            /* hard deadlock --- no solution */
    if (nEdges == 0)
        return false;           /* good configuration found */
    ...
    for (i = 0; i < nEdges; i++)                        // 342: 逐个软边试翻转
    {
        curConstraints[nCurConstraints++] = possibleConstraints[...];
        if (!DeadLockCheckRecurse(proc))
            return false;       /* found a valid solution! */
        nCurConstraints--;      /* 回溯 */
    }
    return true;                /* no solution found */
}
```

一个"约束"= "让等待者 A 排到持有者 B 之前"（翻转一条软边）。算法：当前约束集下找环（`TestConfiguration` → `FindLockCycle`，`deadlock.c:378/446`）；环全是硬边 → 无解；环含软边 → 对每条软边尝试"翻转它"加入约束集后递归。本质是在搜索"是否存在某组队列重排使等待图无环"。最坏是软边数的指数，但实际死锁环极小（2~3 个进程），且约束存储有上限（`maxCurConstraints`）超限即放弃治疗判硬死锁——**宁可误杀不可挂死**。

**`FindLockCycle()`/`FindLockCycleRecurse()`（`deadlock.c:446/457`）——DFS 找环**：

```c
static bool
FindLockCycleRecurse(PGPROC *checkProc, int depth, ...)
{
    /* 469: 并行查询的锁组成员一律折算到组长 */
    if (checkProc->lockGroupLeader != NULL)
        checkProc = checkProc->lockGroupLeader;

    for (i = 0; i < nVisitedProcs; i++)                 // 475: 访问过?
        if (visitedProcs[i] == checkProc)
            return (i == 0);    /* 回到起点=有环; 撞上支路=无环(对起点而言) */

    visitedProcs[nVisitedProcs++] = checkProc;          // 501
    ...
}
```

真正展开出边的是 `FindLockCycleRecurseMember()`（`deadlock.c:536`）：取 `checkProc->waitLock`，对锁上每个 PROCLOCK 检查 `proclock->holdMask & conflictMask`（`deadlock.c:578-585`）——持有冲突模式者即硬边，递归进去；随后（本节未摘录的后半段，`deadlock.c:600` 起）再扫等待队列里**排在 checkProc 前面**的进程——请求模式冲突者即软边，同样递归，但环若经它闭合则把这条边记进 `softEdges[]` 输出。注意 469 行：**并行查询的一组 worker 在等待图里是一个节点**（组内不算死锁——他们共享锁），这是 9.6 并行执行落地时对死锁检测的静默改造。

回到起点的环使 `nDeadlockDetails = depth`（`deadlock.c:487`），沿递归栈回填每一跳的 (locktag, mode, pid)——这就是报错信息里"Process 123 waits for ShareLock on transaction 456; blocked by process 789"链条的来源。

**软死锁解法生效**：`TopoSort()`（`deadlock.c` 后部）对每个需要重排的等待队列在"A 必须在 B 前"约束下做拓扑排序产出新序，`DeadLockCheck` 主体应用之并 `ProcLockWakeup`——**没有任何事务被杀**。硬死锁则由发起检测的进程自杀（报 `deadlock detected`，`DeadLockReport()` 打印环路详情）。

注意"谁检测谁死"：被牺牲的是**碰巧先睡满 deadlock_timeout 的那个**，不是代价最小的——PG 没有 InnoDB 那种"选 undo 量小的事务回滚"的受害者选择策略。理由还是概率账：死锁太罕见，不值得为选受害者维护每事务的代价估计。另外 `DeadLockCheck` 顺带处理"阻塞了 autovacuum"的情形（`deadlock.c:278-279`）：返回 DS_BLOCKED_BY_AUTOVACUUM，调用者会直接取消那个 autovacuum——普通事务的优先级高于后台清理。

### 8.8.3 what-if：一次死锁的完整时序

两个会话、两行数据（id=1, id=2），推演从形成到裁决的全过程：

```
t0   A: BEGIN; UPDATE t SET v=v+1 WHERE id=1;
       -- A 拿到行1: xmax=XID_A; A 持有自己 XID_A 的事务锁(排他)
t1   B: BEGIN; UPDATE t SET v=v+1 WHERE id=2;
       -- 对称: 行2 xmax=XID_B
t2   A: UPDATE t SET v=v+1 WHERE id=2;
       -- heap_update 发现行2 xmax=XID_B 且 B 未完结
       -- A 先拿行2的 LOCKTAG_TUPLE 锁(排队票, 无人争, 立得)
       -- A 对 XID_B 的事务锁发起共享申请 → 冲突(B持排他) → 入队
       -- ProcSleep: enable_timeout_after(DEADLOCK_TIMEOUT, 1000)  [proc.c:1438]
       -- 等待图: A --硬边--> B; 无环。
t3   B: UPDATE t SET v=v+1 WHERE id=1;
       -- 对称: B 等 XID_A 的事务锁, B 也武装 1s 定时器
       -- 等待图: A --> B, B --> A: 环已闭合! 但此刻无人知晓。
t2+1s  A 的定时器先到期(A 先睡的):
       -- CheckDeadLock [proc.c:1887]: 依序拿下全部 16 个锁表分区 LWLock
       -- DeadLockCheck(A) → FindLockCycle: A→B(硬), B→A(硬), 回到起点
       -- 全硬边, 无软边可翻 → DS_HARD_DEADLOCK
       -- A 自杀: ERROR: deadlock detected
       --   DETAIL: Process A waits for ShareLock on transaction XID_B; ...
       -- A 的事务中止 → 释放 XID_A 事务锁 → B 被唤醒, 正常拿锁继续
t2+1s+ε  B 的 UPDATE 完成; B 可 COMMIT。
```

几个值得咀嚼的细节：(a) 若 t3 与 t2 间隔超过 1 秒，A 的第一次检测发生在环闭合**之前**，判无死锁后继续睡**且不再检测**——此时靠 B 的定时器兜底（8.8.1 的归纳论证在此兑现：环的最后一条边是 B 加的，B 的定时器必在环成立后到期）；(b) 死掉的是 A（先睡满的），哪怕 A 的事务改了一百万行而 B 只改了一行——无受害者加权；(c) 若把场景换成"A 先 FOR SHARE 锁行1、B 也 FOR SHARE 锁行1、然后两人都想 UPDATE 行1"，等待队列上会出现软边，PG 会尝试重排队列而可能无人被杀——InnoDB 的同款场景（S 锁升 X 锁）则直接判死锁杀一个。

### 小结

- 懒触发的账：检测需冻结全部 16 个锁分区，即时检测在热点冲突下自身成为灾难（InnoDB 8.0 提供关闭开关反向印证）；睡满 1s 才查把检测频率压低几个数量级，且"每环必有最后入环者的定时器在环成立后到期"保证无漏报；
- 算法分两层：外层在"软边翻转"的约束空间回溯搜索无环解（TopoSort 重排队列，不杀事务），内层 FindLockCycle 做带访问标记的 DFS（硬边=持有冲突，软边=排队在前且冲突；并行查询按锁组折叠为单节点）；
- 硬死锁的受害者 = 首个检测到环的等待者，无代价加权；阻塞 autovacuum 时优先取消 autovacuum。

---

<!-- CONTINUE -->
