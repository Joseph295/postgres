# 第 8 章 · 事务、MVCC 与锁：PostgreSQL 的灵魂模块

> 对应源码：`src/backend/access/transam/`、`src/backend/storage/lmgr/`、`src/backend/storage/ipc/procarray.c`、`src/backend/access/heap/heapam_visibility.c`
>
> 必读官方文档：`src/backend/access/transam/README`（事务系统总纲）、`src/backend/storage/lmgr/README`（锁体系）、`src/backend/storage/lmgr/README-SSI`（可串行化快照隔离）

## 本章导读

如果说执行器是 PostgreSQL 的四肢，那么事务系统就是它的心脏。本章要回答的核心问题只有一个：**当成百上千个会话同时读写同一张表时，每个会话到底"看见"什么、"改动"如何互斥、"冲突"如何裁决？**

我们按照"从一个事务的生命周期出发"的顺序展开：

1. 事务如何被启动、提交、回滚——`xact.c` 的双层状态机（8.1）；
2. 事务的"身份证"XID 如何分配，32 位计数器回卷这个 PG 运维中最凶险的问题如何被层层设防（8.2）；
3. 事务开始读数据前拿到的"快照"是什么、怎么拍的（8.3）；
4. 拿着快照去看一行数据时，可见性判断的完整代码路径（8.4）；
5. 可见性判断依赖的"提交日志"CLOG 与其底层 SLRU 缓冲（8.5）；
6. 三层锁体系：spinlock → LWLock → 重量级锁（8.6）；
7. 行锁为什么不进锁表——xmax 编码与 multixact（8.7）;
8. 死锁检测算法（8.8）；
9. 快照隔离的"漏洞"write-skew 与 SSI 的修补（8.9）。

贯穿全章的主线是与 InnoDB 的对比：**InnoDB 用 undo log 把旧版本"藏"在回滚段里，PG 把新旧版本平铺在堆表中；InnoDB 靠记录锁/间隙锁（2PL 风格）实现隔离，PG 靠 MVCC + 事后检测（死锁检测、SSI）**。这一路线分野决定了两个数据库几乎所有的行为差异——包括为什么 PG 需要 VACUUM、为什么 PG 没有间隙锁、为什么 PG 的只读事务几乎零开销。

## 前置知识

- **元组头**：每个堆元组头部（`src/include/access/htup_details.h`）有 `t_xmin`（创建该版本的事务 XID）、`t_xmax`（删除/锁定该版本的事务 XID）、`t_cid`（命令号）、`t_ctid`（版本链指针）和两个 infomask 标志字。UPDATE 在 PG 中 = 旧版本打上 xmax + 插入新版本，旧版本留在原地等 VACUUM 回收（对比 InnoDB：原地更新 + undo 链）。
- **快照隔离（SI）**：事务开始（或每条语句开始）时记录"当时哪些事务在跑"，之后只看"拍快照时已提交"的数据。读不阻塞写、写不阻塞读。
- **ProcArray**：共享内存中的活跃后端进程数组（`src/backend/storage/ipc/procarray.c`），每个 backend 一个槽位，记录其当前 XID、xmin 等，是"拍快照"的数据来源。
- **LWLock 与 spinlock**：内核内部保护共享内存的轻量锁，与用户可见的表锁/行锁（重量级锁）是完全不同的东西，8.6 节详述。

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
2. **COMMIT/ROLLBACK 命令本身不立即提交/回滚**，只是把状态改成 `TBLOCK_END`/`TBLOCK_ABORT_PENDING`，真正的动作推迟到本条命令结束时的 `CommitTransactionCommand()`（`xact.c:3210`）里统一执行。命令边界的调度入口是三兄弟：`StartTransactionCommand()`（`xact.c:3112`）、`CommitTransactionCommand()`（`xact.c:3210`）、`AbortCurrentTransaction()`（`xact.c:3504`），它们各自就是一个针对 `blockState` 的大 switch。

### 8.1.2 StartTransaction / CommitTransaction / AbortTransaction 的职责划分

**`StartTransaction()`（`xact.c:2106`）**——初始化一个顶层事务。核心动作（按代码顺序）：

- 把 `TopTransactionStateData` 设为当前状态，`s->state = TRANS_START`；
- **不分配 XID**（`s->fullTransactionId = InvalidFullTransactionId`，见 `xact.c:2129`）——这是 8.2.1 的重点；
- 决定隔离级别、只读属性（`XactIsoLevel = DefaultXactIsoLevel`，`xact.c:2175`）——注意：隔离级别在事务**启动瞬间**定格；
- 分配虚拟事务号 vxid、事务内存上下文 `TopTransactionContext`、ResourceOwner；
- 记录事务启动时间戳，最后 `s->state = TRANS_INPROGRESS`。

**`CommitTransaction()`（`xact.c:2270`）**——提交的编排者，顺序极其讲究：

1. 先做**可能调用用户代码**的收尾：循环执行延迟触发器 + 关闭 portal（`xact.c:2299` 的 for(;;) 循环，因为触发器可能开游标、游标可能又挂触发器，必须"洗涤-漂洗-重复"直到收敛）；
2. 之后进入不可再出错的区段：`s->state = TRANS_COMMIT`；
3. **`RecordTransactionCommit()`（调用点 `xact.c:2407`，实现在 `xact.c:1345`）**：写 commit WAL 记录、根据 `synchronous_commit` 决定是否等待刷盘、更新 CLOG 状态为已提交——这一步是事务持久性的分界点；
4. **`ProcArrayEndTransaction()`（`xact.c:2431`）**：把自己从"运行中事务"里摘掉——这一步是事务对**其他会话可见性**的分界点（此后新拍的快照会认为本事务已完成）；
5. 释放锁、smgr 层面真正删除本事务 DROP 的文件、释放内存上下文等资源清理（各种 `AtEOXact_*` 回调）。

**`AbortTransaction()`（`xact.c:2855`）**——异常路径的清道夫。它必须假设"系统可能处于任何烂摊子状态"：先 `HOLD_INTERRUPTS()` 防止清理过程再被打断，重置内存上下文和 ResourceOwner（`AtAbort_Memory()`/`AtAbort_ResourceOwner()`，`xact.c:2869` 附近），然后写 abort WAL 记录、把 CLOG 标为 aborted、从 ProcArray 摘除（`xact.c:3004`）、释放锁。一个安全设计值得注意：`StartTransaction` 阶段就预留了一块保底内存（`xact.c:1217` 附近的注释），使 `AbortTransaction` 在 OOM 场景下也能完成清理。

MVCC 让 PG 的回滚成本与 InnoDB 截然不同：**InnoDB 回滚要沿 undo 链逐条反向恢复（大事务回滚可能比执行还慢），PG 回滚只需把 CLOG 里的 2 个 bit 标成 aborted**——废版本留在原地，反正可见性判断会把它们过滤掉，清理交给 VACUUM。这是"平铺多版本"路线最漂亮的红利之一。

### 8.1.3 子事务（SAVEPOINT）的实现

`SAVEPOINT name` → `DefineSavepoint()`（`xact.c:4427`）→ `PushTransaction()`（`xact.c:5475`）：在 `CurrentTransactionState` 链表上压入一个新的 `TransactionStateData`（`xact.c:195` 定义，`parent` 指针回链父事务），`nestingLevel` 加一，`blockState = TBLOCK_SUBBEGIN`。命令结束时 `StartSubTransaction()`（`xact.c:5121`）把它变成 `TBLOCK_SUBINPROGRESS`。

关键点：

- **子事务同样惰性分配 XID**。真的需要 XID 时，`AssignTransactionId()`（`xact.c:637`）会先保证所有祖先都有 XID（`xact.c:662-683` 用显式数组消除深层递归——防御几千层 SAVEPOINT 打爆栈），再给自己分配，保证"子 XID 一定大于父 XID"这一不变式；
- **pg_subtrans 记录父子关系**。分配子 XID 后立刻调用 `SubTransSetParent()`（调用点 `xact.c:713`，实现在 `src/backend/access/transam/subtrans.c:92`）把"子 XID → 父 XID"写入 pg_subtrans 这个 SLRU（8.5 节）。为什么需要它？因为快照里对子事务的记录可能溢出（每个 backend 的 PGPROC 只缓存 `PGPROC_MAX_CACHED_SUBXIDS`=64 个子 XID），溢出后别的会话要判断某个子 XID 的状态，只能通过 `SubTransGetTopmostTransaction()`（`subtrans.c:170`）逐级爬到顶层 XID 再查（8.3/8.4 节会看到这条慢路径）。与 CLOG 不同，pg_subtrans 不必崩溃安全——崩溃后所有未提交事务都视为 aborted，父子关系无人再问津；
- **子事务提交不写 CLOG 的"已提交"**。`RELEASE SAVEPOINT` → `CommitSubTransaction()`（`xact.c:5158`）只是把子事务的 XID 挂进父事务的 `childXids` 数组（`xact.c:208`），资源移交给父层。只有顶层事务提交时，`TransactionIdSetTreeStatus()`（`clog.c:192`）才把整棵 XID 树一起标成 committed。也就是说**子事务的命运完全捆绑在顶层事务上**——这就是 CLOG 里需要 `SUB_COMMITTED` 中间态的原因（8.5 节）；
- `ROLLBACK TO SAVEPOINT` → `RollbackToSavepoint()`（`xact.c:4621`）：把目标层之上的所有子事务标为 `TBLOCK_SUBABORT_PENDING`/`SUBABORT_RESTART`，逐层 `AbortSubTransaction()`（`xact.c:5273`）后再重启一个同名子事务——所以 ROLLBACK TO 之后 savepoint 依然存在，可以反复回滚。

顺带一提：PL/pgSQL 的 `BEGIN...EXCEPTION` 块底层就是子事务，这也是"EXCEPTION 块很贵"的根源——每次进入都要 push 状态、可能分配子 XID、写 pg_subtrans。

### 小结

- xact.c 是双层状态机：低层 TransState 管事务本体，高层 TBlockState 协调命令边界与事务边界的错位；
- COMMIT/ROLLBACK 命令只改状态，真正动作在命令结束的 `CommitTransactionCommand()` 里做；
- 提交的两个关键分界点：`RecordTransactionCommit()`（持久性）与 `ProcArrayEndTransaction()`（对外可见性）；
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

`src/backend/access/transam/varsup.c:67`：

```c
FullTransactionId
GetNewTransactionId(bool isSubXact)
{
    ...
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);          // 全局唯一的 XID 分配锁

    full_xid = TransamVariables->nextXid;             // 共享内存中的下一个 XID
    xid = XidFromFullTransactionId(full_xid);

    if (TransactionIdFollowsOrEquals(xid, TransamVariables->xidVacLimit))
    {
        /* 进入回卷警戒区: 触发 autovacuum / 告警 / 拒绝服务, 见 8.2.4 */
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

- `varsup.c:96` 拿 `XidGenLock` 排他锁——XID 分配是全局串行点，这正是"惰性分配"值得的原因；
- `varsup.c:114-188` 是回卷警戒区检查（8.2.4 详述）；
- `varsup.c:199-201`：如果这个 XID 是新 CLOG 页的第一个事务，要**先**把 CLOG 页清零再推进计数器（`varsup.c:203-208` 的注释说得很清楚：顺序不能反，否则 ExtendCLOG 失败后会留下无 CLOG 空间的已分配 XID）；
- `varsup.c:211` 起的大段注释是并发正确性的核心：**新 XID 必须在释放 XidGenLock 之前写进 ProcArray**，以保证任何比 `latestCompletedXid` 老的活跃 XID 一定出现在 ProcArray 里——这是快照正确性（OldestXmin 计算）的根基，`transam/README` 有整节论证。

### 8.2.3 32 位回卷与模运算比较：TransactionIdPrecedes 逐行

XID 是 32 位无符号数，约 42.9 亿个。长期运行必然用完回绕，PG 的对策是**把 XID 空间看成一个环**："比较两个 XID 谁老"不用普通大小比较，而用模 2^32 的有符号差值：

```c
/* src/include/access/transam.h:262 */
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

- 第 271 行：XID 0/1/2 是保留值（`transam.h:32-34`：0=Invalid，1=Bootstrap，2=**Frozen**），它们"永远最老"，直接无符号比较；
- 第 274 行是全部魔法：`id1 - id2` 在无符号运算下自然回绕，强转 `int32` 后——若 id1 在环上位于 id2"后方半圈以内"，差值为负 → id1 更老。例如 id1=4294967290（接近 2^32）、id2=10：`(int32)(4294967290-10) = -16` < 0，判定 id1 更老，符合直觉；
- 推论：**这个比较只在两个 XID 相距不超过 2^31（约 21 亿）时才有意义**。一旦某个存活的旧 XID 与当前 XID 距离超过半圈，它会突然从"过去"翻转成"未来"——已提交的旧数据瞬间"不可见"，这就是灾难性的 wraparound 数据丢失。

### 8.2.4 freeze 与回卷保护的多道防线

既然比较半径只有 2^31，就必须保证磁盘上**不存在**年龄超过约 21 亿的普通 XID。手段是 **freeze（冻结）**：VACUUM 把足够老的元组标记为"对所有人可见"——现代 PG 通过设置 infomask 的 `HEAP_XMIN_FROZEN`（= `HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID` 两位同置，`htup_details.h:206`）实现，古老版本则是把 xmin 直接改写为 FrozenTransactionId(2)。冻结后的元组退出 XID 比较游戏，永远可见。

防线由外向内共四道（前三道全部实现在 `GetNewTransactionId` 的警戒区检查里，`varsup.c:114-188`）：

1. **xidVacLimit**（`varsup.c:114`）：距回卷还剩 `autovacuum_freeze_max_age`（默认 2 亿）时，每 64K 个事务就踢一脚 postmaster 强制启动 autovacuum（`varsup.c:135-136`），做全表冻结（aggressive vacuum）；
2. **xidWarnLimit**（`varsup.c:159`）：继续恶化则每次分配 XID 都打 WARNING："database must be vacuumed within N transactions"；
3. **xidStopLimit**（`varsup.c:138-158`）：距回卷仅剩约 300 万时**拒绝分配新 XID**，报错"database is not accepting commands..."——数据库变成只读，宁可停止服务也不损坏数据。单用户模式是留给 DBA 的逃生舱；
4. **兜底**：即便计数器真的绕回去，`pg_class.relfrozenxid`/`pg_database.datfrozenxid` 记录了每个表/库最老的未冻结 XID，`TransamVariables->oldestXid` 保证 CLOG 不会被过早截断。

**FullTransactionId**（PG 12 起）是治本方向：64 位 = 32 位 epoch + 32 位 xid（`transam.h:51-52` 的比较宏直接用 `.value` 无符号比较，无需模运算）。`nextXid` 等新代码路径已经用 64 位（`varsup.c:67` 返回类型即是），但**元组头里仍只存 32 位 xmin/xmax**（为了不加宽每行 8 字节），所以 freeze 机制在可预见的未来仍然必需。

InnoDB 对照：InnoDB 的 trx_id 是 48 位，purge 清理 undo 的压力类似 VACUUM，但没有"回卷停库"这种悬崖——这是 PG 为"元组头省 4 字节 × 每行"付出的运维代价。理解这一节，你就理解了 PG 生产运维中最凶险的告警。

### 小结

- 只读事务只有 vxid（进程槽位号+本地计数器），不碰全局 XID 计数器——只读路径近乎零事务开销；
- `GetNewTransactionId` 在 XidGenLock 保护下分配 XID，且必须先扩展 CLOG、后推计数器、写完 ProcArray 才放锁；
- XID 比较是模 2^32 的有符号差值，有效半径 2^31，因此老元组必须冻结；
- 回卷防线：强制 autovacuum → 告警 → 拒绝写入 → relfrozenxid 兜底；FullTransactionId(64 位) 已在内部推广，但元组头仍是 32 位。

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
- `xmin`/`xmax`（`snapshot.h:153-154`）：可见性的快速通道。**注意 xmax 是开区间**："XID ≥ xmax 一律视为运行中（不可见）"、"XID < xmin 一律视为已完成"；
- `xip[]`：拍照瞬间正在运行的顶层事务列表，落在 [xmin, xmax) 区间内的 XID 才需要来这里查。语义上，**快照 = "xmin/xmax 定出的窗口 + 窗口内的黑名单 xip[]"**；
- `subxip[]`/`suboverflowed`：运行中的**子**事务 XID。每个 backend 最多向 ProcArray 缓存 64 个子 XID，任何一个 backend 溢出，整个快照标记 `suboverflowed`，此后可见性判断只能走 pg_subtrans 慢路径（见 `XidInMVCCSnapshot`）——这就是"单个事务开几百个 SAVEPOINT 会拖慢全库"这一经典性能陷阱的机制；
- `curcid`：命令号，解决**事务内部**的可见性——同一事务里，后面的命令能看到前面命令的修改，但看不到本命令自己的（防止"Halloween 问题"：UPDATE 扫到自己刚插的行无限循环）；
- `snapXactCompletionCount`：见 8.3.3。

InnoDB 对照：这就是 InnoDB 的 ReadView（`m_low_limit_id`≈xmax、`m_up_limit_id`≈xmin、`m_ids`≈xip[]），概念一一对应，几乎是快照隔离的标准形态。差别在于用途：InnoDB 拿 ReadView 沿 undo 链回溯出正确版本；PG 拿快照在堆上平铺的多版本中**过滤**出正确版本。

### 8.3.2 GetSnapshotData 走读

`src/backend/storage/ipc/procarray.c:2114`。骨架如下（省略号处为原代码）：

```c
Snapshot
GetSnapshotData(Snapshot snapshot)
{
    ...
    LWLockAcquire(ProcArrayLock, LW_SHARED);          // 2178: 共享锁足矣

    if (GetSnapshotDataReuse(snapshot))               // 2180: PG14 快照复用
        { LWLockRelease(ProcArrayLock); return snapshot; }

    latest_completed = TransamVariables->latestCompletedXid;
    ...
    /* xmax is always latestCompletedXid + 1 */
    xmax = XidFromFullTransactionId(latest_completed);
    TransactionIdAdvance(xmax);                       // 2196

    xmin = xmax;                                      // 2200: xmin 从 xmax 起步往下压

    for (int pgxactoff = 0; pgxactoff < numProcs; pgxactoff++)   // 2220
    {
        TransactionId xid = UINT32_ACCESS_ONCE(other_xids[pgxactoff]);

        if (likely(xid == InvalidTransactionId))      // 2232: 只读事务, 跳过
            continue;
        if (pgxactoff == mypgxactoff)                 // 2240: 自己不进快照
            continue;
        if (!NormalTransactionIdPrecedes(xid, xmax))  // 2256: >=xmax 的不必记
            continue;
        statusFlags = allStatusFlags[pgxactoff];
        if (statusFlags & (PROC_IN_LOGICAL_DECODING | PROC_IN_VACUUM))
            continue;                                 // 2264: 跳过 lazy vacuum

        if (NormalTransactionIdPrecedes(xid, xmin))
            xmin = xid;                               // 2267: 顺手压低 xmin
        xip[count++] = xid;                           // 2271: 记入黑名单

        if (!suboverflowed) { ... memcpy 子事务 XID 或置 suboverflowed ... }
    }
    ...
    snapshot->xmin = xmin;                            // 2448
    snapshot->xmax = xmax;
    snapshot->xcnt = count;
    ...
}
```

逐段解说：

- **共享锁而非排他锁**（`procarray.c:2178`）：拍快照只读 ProcArray；但事务结束（`ProcArrayEndTransaction`）要拿排他锁。所以历史上"高并发短事务 + 高频拍快照"会在 ProcArrayLock 上剧烈碰撞——PG 14 优化的主战场；
- **xmax = latestCompletedXid + 1**（`procarray.c:2194-2196`）：不是"下一个待分配 XID"而是"最新已完成 XID + 1"。任何 ≥ xmax 的事务要么在跑、要么快照之后才启动，一律不可见，天然正确；
- **xmin 的算法**（`procarray.c:2200-2268`）：从 xmax 起步，扫描中被每个更小的活跃 XID 往下压，最终 = 所有运行中事务的最小 XID（含自己，`procarray.c:2203` 在循环外单独处理）。xmin 除了加速可见性判断，还会写入 `MyProc->xmin`，成为全局 OldestXmin 的组成部分——**它决定 VACUUM 能清多老的死元组**。一个挂着不提交的长事务把全库 xmin 摁在原地，就是"长事务导致膨胀"的机制；
- **`UINT32_ACCESS_ONCE`**（`procarray.c:2223`）：只读一次共享内存里的 xid，防止编译器重读时读到不同值——与 `GetNewTransactionId` 尾部的写屏障（`varsup.c:224` 附近注释）配对；
- **跳过项**：无 XID 的只读事务（这是 8.2.1 优化的直接兑现：只读事务连别人拍快照的成本都不增加）、自己、≥xmax 者、逻辑解码与 lazy VACUUM 进程；
- **子事务采集**（`procarray.c:2288-2310`）：从每个 PGPROC 的 subxids 缓存 memcpy；任何人溢出则整个快照 `suboverflowed = true`；
- 收尾（`procarray.c:2448-2455`）：写回 snapshot 各字段，并记录 `snapXactCompletionCount`。

**PG 14 的快照可扩展性重构**（Andres Freund, 2020, commit 系列 dc7420c2 等）背景：PG 13 之前 ProcArray 的每个条目是分散的 PGXACT 结构，拍快照要跳跃访问每个进程的缓存行；连接数上千时 GetSnapshotData 成为最热函数。重构做了两件事：

1. **列式化（dense arrays）**：把各 PGPROC 的 xid、subxid 状态、statusFlags 拆成 `ProcGlobal->xids[]`、`subxidStates[]`、`statusFlags[]` 三个紧凑数组（正是上面走读中循环访问的结构），扫描变成顺序内存访问，缓存友好；
2. **快照复用**：见下节。

这次重构使 PG 在数千连接下的只读吞吐提升了数倍，是近年最重要的可扩展性改进之一。

### 8.3.3 GetSnapshotDataReuse：不重拍就复用

`procarray.c:2034`。共享内存里有个 64 位计数器 `TransamVariables->xactCompletionCount`，**每当一个带 XID 的事务完成（提交/回滚）时加一**。拍快照时把当时的值存进 `snapshot->snapXactCompletionCount`；下次再拍时若计数器没变（`procarray.c:2043-2045`），说明"运行中事务集合"没有任何变化，直接复用上一张快照的全部内容，只刷新 curcid——省掉整个 ProcArray 扫描。对 READ COMMITTED（每条语句一张快照）的高频短查询负载，这个命中率非常高。

### 8.3.4 各隔离级别拿快照的时机

PG 实际只有三种行为（SQL 标准四级中 READ UNCOMMITTED 在 PG 中等同 READ COMMITTED——MVCC 下实现脏读反而更麻烦，也无意义）：

| 隔离级别 | 拿快照时机 | 源码入口 |
|---|---|---|
| READ COMMITTED | **每条语句**开头拍一张新快照 | `GetTransactionSnapshot()`（`snapmgr.c`）每次都调 `GetSnapshotData` |
| REPEATABLE READ | 事务中**第一条语句**拍一张，全程复用 | `snapmgr.c` 中 `FirstSnapshotSet` 标志控制，之后返回 `CurrentSnapshot` |
| SERIALIZABLE | 同 RR 的快照 + SSI 冲突追踪 | `GetSerializableTransactionSnapshot()`（`predicate.c`） |

注意细节：快照在**第一条语句**而非 BEGIN 时产生——`BEGIN; (发呆十分钟); SELECT ...` 的 RR 事务看到的是 SELECT 时刻的世界。另外 READ COMMITTED 的 UPDATE 遇到并发修改时执行 **EPQ（EvalPlanQual）重检**：拿最新版本重新验证 WHERE 条件后更新；而 RR 遇到并发修改直接报 `serialization failure`——InnoDB RR 的 UPDATE 则用当前读（锁定读）静默改最新版本，这是两家 RR 语义最容易踩坑的差异。InnoDB 也没有 EPQ 概念，因为它的写操作永远走当前读。

### 小结

- 快照 = xmin/xmax 窗口 + 窗口内运行中事务黑名单 xip[]/subxip[]，等价于 InnoDB ReadView；
- `GetSnapshotData` 持 ProcArrayLock 共享锁顺序扫描紧凑数组；xmax=latestCompletedXid+1，xmin 为活跃事务最小值并回写 MyProc->xmin（VACUUM 视界）；
- PG 14 重构：列式 dense arrays + xactCompletionCount 快照复用，解决千连接下的拍快照瓶颈；
- RC 每语句一拍、RR/SSI 首语句一拍；RC 用 EPQ 重检并发更新，RR 直接串行化失败。

---

## 8.4 可见性判断：HeapTupleSatisfiesMVCC 逐行走读

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

**这段 WAL 考量**（`heapam_visibility.c:152-166`）值得细品：hint bit 修改**不写 WAL**（否则读操作产生 WAL 就太荒谬了）。但异步提交（`synchronous_commit=off`）下有个坑：事务的 commit WAL 记录可能还没刷盘，如果此刻把 `HEAP_XMIN_COMMITTED` 打上并且**这个数据页**先于 commit 记录落盘，崩溃恢复后该事务其实没提交，页上却留着"已提交"的假 hint——数据损坏。所以规则是：**commit LSN 尚未刷盘、且页面 LSN 也没有更新到能保证顺序时，宁可不设 hint**，留给未来的访问者再试。此外，`checksum`/`wal_log_hints` 开启时，设 hint 弄脏页面可能触发 full-page write（`MarkBufferDirtyHint` 路径），这是"开启校验和会放大写"的来源。

这个机制解释了 PG 一个著名现象：**大批量导入后的第一次全表扫描会产生大量写 I/O**——它在给每行盖"已提交"的章。运维上 `VACUUM (FREEZE)` 或让第一次扫描发生在低峰就是为了这个。

### 8.4.2 主体逐行走读

```c
/* heapam_visibility.c:956 起 */
if (!HeapTupleHeaderXminCommitted(tuple))            // hint 没说"xmin已提交"
{
    if (HeapTupleHeaderXminInvalid(tuple))           // hint 说"xmin已中止"
        return false;                                //   → 出生即夭折, 不可见
    ...
    else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
    {                                                // 是我自己插入的行
        if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
            return false;   /* inserted after scan started */
        if (tuple->t_infomask & HEAP_XMAX_INVALID)   /* xid invalid */
            return true;                             //   没被删 → 可见
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;                             //   xmax 只是锁 → 可见
        ...
        if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
            return true;    /* deleted after scan started */
        else
            return false;   /* deleted before scan started */
    }
    else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
        return false;                                // 拍照时它还在跑 → 不可见
    else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
        SetHintBitsExt(tuple, buffer, HEAP_XMIN_COMMITTED, ..., state);
    else
    {   /* it must have aborted or crashed */
        SetHintBitsExt(tuple, buffer, HEAP_XMIN_INVALID, ..., state);
        return false;
    }
}
else
{   /* xmin is committed, but maybe not according to our snapshot */
    if (!HeapTupleHeaderXminFrozen(tuple) &&
        XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
        return false;       /* treat as still in progress */
}
```

**xmin 阶段**（`heapam_visibility.c:956-1024`）分五岔：

1. hint=aborted（958）→ 直接不可见，零成本；
2. **是我自己的事务**（963）：`TransactionIdIsCurrentTransactionId()`（`xact.c:943`）纯本地判断——遍历自己的事务状态栈，不碰共享内存。此时快照没用，改用 **curcid 比命令号**：`cmin >= curcid` 说明是本条命令插的（或后续命令），不可见——这就是 Halloween 问题的防线。随后还要看自己是否又删了它（965-1003），其中 992 行的细节：xmax 是本事务的**已中止子事务**（比如 SAVEPOINT 里删的又 ROLLBACK TO 了）→ 删除无效，可见；
3. **在我的快照里**（1005）→ 拍照时还在跑 → 不可见。注意顺序：**先查快照再查 CLOG**——它可能"此刻已经提交了"，但对我的快照而言仍算运行中；也因此无需在此设 hint（函数头注释 `heapam_visibility.c:925-936` 解释了为何这样反而省了 ProcArrayLock 争用）；
4. 不在快照里且 CLOG 说已提交（1007）→ 顺手盖 `HEAP_XMIN_COMMITTED` 章，本次继续往下走 xmax 阶段；
5. 不在快照里、CLOG 说没提交也不在跑（1010）→ 只能是中止或崩溃 → 盖 `HEAP_XMIN_INVALID` 章，不可见。（崩溃的事务永远不会有人替它写 CLOG=aborted，全靠这里"查无此人即作废"的推断，以及恢复时的处理。）
6. else 分支（1018-1024）：hint 已说提交，但若未冻结，仍要过一遍 `XidInMVCCSnapshot`——**hint bit 是全局事实（提交与否），快照是局部视角（对我而言算不算完成）**，两者缺一不可。冻结元组（`HeapTupleHeaderXminFrozen`）则连快照都不用查，无条件"出生可见"。

**xmax 阶段**（`heapam_visibility.c:1026-1095`）对称地反过来：

```c
/* by here, the inserting transaction has committed */
if (tuple->t_infomask & HEAP_XMAX_INVALID)   /* xid invalid or aborted */
    return true;                              // 没死 → 可见
if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    return true;                              // xmax 只是行锁, 不是删除 → 可见
if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) { ... }  // 从 multixact 提取真正的 updater
if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
{
    if (TransactionIdIsCurrentTransactionId(...))    // 我自己删的: 比 cmax
        return HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid;
    if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
        return true;                          // 删除者对我算运行中 → 死亡不可见 → 行可见
    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
    {   SetHintBitsExt(..., HEAP_XMAX_INVALID, ...); return true; }  // 删除被中止
    SetHintBitsExt(..., HEAP_XMAX_COMMITTED, ...);   // 删除已提交
}
else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
    return true;
return false;                                 // 死亡对我可见 → 行不可见
```

三个要点：xmax 可能只是**行锁**而非删除（1031，`HEAP_XMAX_LOCK_ONLY`，8.7 节）；可能是 **multixact**（1034，要 `HeapTupleGetUpdateXid` 从成员里挑出真正的 updater）；判断次序同样是 自己→快照→CLOG。

### 8.4.3 XidInMVCCSnapshot 与 CLOG 查询路径

**`XidInMVCCSnapshot()`**（`src/backend/utils/time/snapmgr.c:1868`）：

```c
if (TransactionIdPrecedes(xid, snapshot->xmin))
    return false;                       // < xmin: 必已完成
if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
    return true;                        // >= xmax: 必算运行中
if (!snapshot->suboverflowed)
{   /* 快路径: 直接搜两个数组 */
    if (pg_lfind32(xid, snapshot->subxip, snapshot->subxcnt)) return true;
}
else
{   /* 慢路径: 先经 pg_subtrans 折算成顶层 XID */
    xid = SubTransGetTopmostTransaction(xid);
    if (TransactionIdPrecedes(xid, snapshot->xmin)) return false;
}
if (pg_lfind32(xid, snapshot->xip, snapshot->xcnt)) return true;
```

先用 xmin/xmax 两个边界把绝大多数 XID 挡在数组搜索之外；数组搜索用 `pg_lfind32`（SIMD 优化的线性查找）。**suboverflowed 的代价在这里兑现**：溢出快照对每个落入窗口的 XID 都要去 pg_subtrans（一个 SLRU，可能引发 I/O）爬父链——大量 SAVEPOINT + 长窗口的组合能让纯读查询显著变慢。

**CLOG 查询路径**：`TransactionIdDidCommit()`（`transam.c:126`）→ `TransactionLogFetch()`（`transam.c:52`）：先查单条 XID 的本地缓存 `cachedFetchXid`（`transam.c:61-62`），未命中则 `TransactionIdGetStatus()`（`clog.c:743`）读 SLRU 页取出 2 bit。注意 `DidCommit` 对 `SUB_COMMITTED` 状态还会递归到父事务——子事务的最终命运由顶层决定（8.1.3）。

### 小结

- 判定框架：出生可见 ∧ 死亡不可见 → 行可见；xmin/xmax 两阶段完全对称；
- 每个 XID 的判断次序：hint bit → 是不是我自己（比 curcid）→ 在不在快照（局部视角）→ CLOG（全局事实），并把 CLOG 结论缓存为 hint bit；
- hint bit 不写 WAL，因此异步提交下要用"commit LSN 已刷盘或页 LSN 保序"作为设置前提；首次扫描盖章是 PG 读放大写的来源；
- 子事务溢出的快照要走 pg_subtrans 慢路径，这是 SAVEPOINT 滥用的隐性成本。

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

CLOG、pg_subtrans、multixact、commit_ts、SSI 的旧事务摘要、NOTIFY 队列……这些"按整数索引、可回卷截断的元数据"共用一套缓冲机制 SLRU（Simple LRU，`src/backend/access/transam/slru.c`，头部注释 `slru.c:1-50` 是最好的导读）：

- 共享内存中一池 8KB 页缓冲，按页号低位分成若干 **bank**，每个 bank 一把控制 LWLock + 每页一把 I/O LWLock（PG 17 起的银行化改造，之前是一把大锁，曾是高并发 CLOG 查询的著名瓶颈，对应 GUC `transaction_buffers` 等）；
- 淘汰算法是朴素 LRU，但**永不淘汰最新页**（写流量集中在最新页）；
- 与 buffer manager 的关系：SLRU 是独立的小型缓冲系统，不走 shared_buffers，页面没有 LSN 概念的完整支持（CLOG 靠"提交前 WAL 已刷"保证恢复正确性，pg_subtrans 干脆不持久化）。

InnoDB 没有 CLOG 的直接对应物——它把提交状态藏在 trx 系统与 undo 段头里，靠回滚段实现同类语义。PG 的 CLOG 把"事务命运"抽成一个全局位图，才使"回滚 = 标 2 个 bit"和"hint bit 缓存"这些设计成为可能。

### 小结

- CLOG 每事务 2 bit、纯算术寻址；父子事务用 SUB_COMMITTED 中间态实现跨页原子提交；
- SLRU 是一族小型 LRU 缓冲池（CLOG/subtrans/multixact/...），PG 17 起按 bank 分锁；
- CLOG + hint bit + 快照三件套配合，才撑起"提交/回滚 O(1)"的 MVCC。

---

## 8.6 锁三层体系：spinlock → LWLock → 重量级锁

PG 的锁分三层，保护对象、持有时长、功能各不相同（`storage/lmgr/README` 开篇即此表）：

| 层 | 保护对象 | 持有时长 | 等待方式 | 死锁处理 |
|---|---|---|---|---|
| spinlock | 几个字长的共享变量 | 几十条指令 | 忙等+退避 | 无（不许阻塞） |
| LWLock | 共享内存数据结构 | 一段临界区 | 原子操作+信号量睡眠 | 无（靠编码纪律） |
| 重量级锁 | 数据库对象（表/元组/XID/...） | 事务结束才放 | 等待队列+睡眠 | 死锁检测 |

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
- `TAS_SPIN` 的非锁定预读（`s_lock.h:205` 注释）：自旋等待时先普通读，避免每次都发起总线锁定的缓存行独占请求（否则持有者解锁都困难）；配套的 `SPIN_DELAY` 是 `rep; nop` 即 PAUSE 指令；
- 自旋失败会指数退避乃至 sleep（`s_lock.c` 的 `perform_spin_delay`），自旋过久报 "stuck spinlock"——因此**持锁区内绝不允许任何可能阻塞/出错的操作**。

### 8.6.2 LWLock：一个原子字 + 等待队列

LWLock（`src/backend/storage/lmgr/lwlock.c`）支持共享/排他两种模式，保护缓冲区头、WAL 插入位置、ProcArray 等一切共享内存结构。核心是**一个 32 位原子状态字** `lock->state`：低位是共享持有者计数（`LW_VAL_SHARED` 累加），高位一个排他位（`LW_VAL_EXCLUSIVE`），再加 `LW_FLAG_HAS_WAITERS`、`LW_FLAG_LOCKED`（保护等待队列的内嵌小自旋位）等标志位。

**抢锁 `LWLockAttemptLock()`**（`lwlock.c:764`）就是一个 CAS 循环：

```c
old_state = pg_atomic_read_u32(&lock->state);
while (true)
{
    desired_state = old_state;
    if (mode == LW_EXCLUSIVE)
    {
        lock_free = (old_state & LW_LOCK_MASK) == 0;   // 无任何持有者
        if (lock_free) desired_state += LW_VAL_EXCLUSIVE;
    }
    else
    {
        lock_free = (old_state & LW_VAL_EXCLUSIVE) == 0;  // 无排他持有者
        if (lock_free) desired_state += LW_VAL_SHARED;    // 共享计数 +1
    }
    if (pg_atomic_compare_exchange_u32(&lock->state, &old_state, desired_state))
        return lock_free ? false /*得手*/ : true /*需等待*/;
}
```

**`LWLockAcquire()`**（`lwlock.c:1150`）的主循环（`lwlock.c:1207` 起）解决经典的"检查-睡眠竞态"，手法是**入队后再试一次**：

1. 直接 `LWLockAttemptLock`，得手即返回；
2. 失败则 `LWLockQueueSelf` 把自己挂进等待队列，**然后再试一次**（`lwlock.c:1238`）——如果这次成了，撤销排队（`LWLockDequeueSelf`）拿锁走人。为什么必须再试？因为第一次失败到入队之间，持有者可能已经释放；入队动作先于第二次检查，保证释放者必能看到我；
3. 仍失败才 `PGSemaphoreLock(proc->sem)` 睡在自己的信号量上（`lwlock.c:1269`）。
4. 醒来后**从头循环再抢**——`lwlock.c:1195-1205` 的大段注释解释了为何被唤醒者不直接继承锁（handoff）而要重抢：LWLock 临界区极短，一个进程在一个时间片内会反复拿放同一把锁，若每次都把锁"移交"给睡着的等待者，等于强制进程切换，吞吐反而崩掉。这是"允许一点不公平换总吞吐"的经典取舍。

**`LWLockRelease()`**（`lwlock.c:1767`）：原子减去自己的份额（`lwlock.c:1797-1800`），然后看旧状态：仅当"有等待者 ∧ 没有唤醒已在进行 ∧ 锁已完全空闲"时（`lwlock.c:1812-1814`）才进入 `LWLockWakeup()`（`lwlock.c:904`）——锁住等待队列，摘下应唤醒的进程（唤醒一个排他等待者，或一串连续的共享等待者），逐个 `PGSemaphoreUnlock`。

LWLock **没有死锁检测**，全靠编码纪律：固定顺序拿锁、持锁期间不调用可能拿更多锁的复杂代码。`LWLockAcquire` 开头的 `HOLD_INTERRUPTS()`（`lwlock.c:1189`）保证临界区内不响应 cancel/die——LWLock 临界区必须快进快出。

### 8.6.3 重量级锁：lock.c

这才是用户视角的"锁"（`pg_locks` 里看到的东西）。被锁对象用 16 字节的 LOCKTAG 描述（`src/include/storage/locktag.h:64`）：

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

`locktag_type`（`locktag.h:35-50`）枚举了可锁对象：RELATION（field1=dbOid, field2=relOid）、PAGE、TUPLE、**TRANSACTION**（等一个 XID 结束——行锁等待的真身，8.7）、VIRTUALTRANSACTION、OBJECT、ADVISORY 等。

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

**LockAcquire 主流程**（`lock.c:806` 薄封装 → `LockAcquireExtended()`，`lock.c:833`）：

1. 查本地锁表 `LOCALLOCK`（backend 私有哈希）：同一把锁我已持有则只加引用计数返回（`lock.c:940-944`）——重入不碰共享内存；
2. **尝试 fastpath**（`lock.c:984-1026`，见下）；
3. 不行才进共享锁表：按 LOCKTAG 哈希选中 16 个分区之一，拿分区 LWLock（`lock.c:1062-1064`），在共享哈希表中找/建 LOCK 对象（每把锁一个，含持有者位掩码、等待队列）与 PROCLOCK 对象（锁×进程一个）；
4. 冲突检查（`LockCheckConflicts`）：将请求模式的 `LockConflicts` 掩码与现有持有掩码按位与；无冲突 `GrantLock`，有冲突则挂入 `lock->waitProcs` 等待队列并睡眠（`WaitOnLock` → `ProcSleep`，8.8 节死锁检测在此埋伏）；
5. 事务结束时 `LockReleaseAll` 统一释放（重量级锁持有到事务末尾——这一点与 2PL 一致）。

**fastpath 锁优化（PG 9.2）——弱锁不进共享锁表**。动机：每条查询都要给涉及的每张表拿 AccessShare 锁，全走共享哈希表意味着高并发下分区锁疯狂碰撞，而这些弱锁 99.99% 情况下根本无人冲突。判定条件（`lock.c:270`）：

```c
#define EligibleForRelationFastPath(locktag, mode) \
    ((locktag)->locktag_lockmethodid == DEFAULT_LOCKMETHOD && \
    (locktag)->locktag_type == LOCKTAG_RELATION && \
    (locktag)->locktag_field1 == MyDatabaseId && \
    MyDatabaseId != InvalidOid && \
    (mode) < ShareUpdateExclusiveLock)
```

即：本库的表锁、模式为最弱三级（AccessShare/RowShare/RowExclusive，称"弱锁"；ShareUpdateExclusive 及以上称"强锁"）。机制分三方：

- **弱锁方**（`lock.c:984-1017`）：只拿**自己 PGPROC 里的** `fpInfoLock`（一把无争用的 LWLock），先检查共享计数器 `FastPathStrongRelationLocks->count[分区]` 是否为零——零表示"这个哈希分区里没有任何强锁存在"——然后把 (relid, lockmode) 记进自己 PGPROC 的 fastpath 槽位数组（`FastPathGrantRelationLock()`，`lock.c:2790`：每组 16 槽，位图记模式）。整个过程**不碰共享锁表**；
- **强锁方**（`lock.c:1034-1055`）：想拿 ShareLock 以上的人先 `BeginStrongLockAcquire` 把对应分区的强锁计数器 +1（此后新的弱锁申请都会看到非零而放弃 fastpath），再执行 `FastPathTransferRelationLocks`：**遍历所有 backend 的 PGPROC**，把它们藏在 fastpath 槽里的该表弱锁**搬进共享锁表**，然后自己按常规流程做冲突检查。也就是说，把成本从高频的弱锁路径转嫁到罕见的强锁（DDL）路径——放大 DDL 的代价，换取 DML/查询路径近乎无锁；
- **正确性关键**（`lock.c:992-997` 注释）：弱锁方"检查计数器→写槽位"在 fpInfoLock 内完成，LWLock 自带内存屏障，保证强锁方要么看到我的槽位（搬走它），要么它的计数器先于我的检查可见（我放弃 fastpath）——不会两边都走快路而漏掉冲突。

这就是 `pg_locks` 里 `fastpath = true` 行的来历，也是"PG 表多、分区多时 DDL 变卡"的一个原因（强锁要扫全体 backend 的 fastpath 槽）。

### 小结

- spinlock：TAS 原子指令 + 自旋退避，保护几条指令的临界区；
- LWLock：单个 32 位原子字（共享计数+排他位）+ 等待队列；入队后重试消除竞态窗口；唤醒者不移交锁，被唤醒者重抢——以小小的不公平换吞吐；无死锁检测；
- 重量级锁：LOCKTAG 标识对象、LockConflicts 位掩码矩阵定义 8 模式语义、共享哈希 16 分区、持有到事务结束、有等待队列与死锁检测；
- fastpath：弱表锁只记在自家 PGPROC 槽位，强锁方负责"搬运+封路"，将共享锁表争用从常规路径上移除。

---

## 8.7 行锁：写进元组的锁

### 8.7.1 行锁不进锁表

InnoDB 的行锁（记录锁、间隙锁、next-key 锁）是内存中的锁结构，锁多了占内存。PG 的行锁**根本不在锁表里**：给一行加锁 = 把自己的 XID 写进该行的 `t_xmax`，并用 infomask 标注"这只是锁不是删除"。千万行加锁也只是改千万个元组头，共享内存零占用。

代价是等待机制要绕个弯：**等行锁 = 等持有者的事务结束 = 对持有者的 XID 拿一把 `LOCKTAG_TRANSACTION` 共享锁**（每个事务启动后就持有自己 XID 的排他"事务锁"，见 `xact.c:724` 附近 `AssignTransactionId` 里的加锁）。事务提交/回滚释放事务锁，等待者全部醒来。死锁检测因此依然有效——行锁等待在等待图里表现为"等某个事务锁"。另一个代价：**行锁信息随元组落盘**，这就是 `SELECT FOR UPDATE` 大批量执行会弄脏大量页面的原因。

顺带回答一个 MySQL 用户必问的问题：**PG 没有间隙锁**。防幻读不靠锁间隙，RR 靠快照天然无幻读（读已冻结的世界），SERIALIZABLE 靠 SSI 的谓词锁检测（8.9）。因此 PG 也没有 InnoDB 那些"间隙锁导致的意外死锁"，但代价是 SERIALIZABLE 下可能事后回滚。

### 8.7.2 四种行锁模式与 infomask 编码

`src/include/nodes/lockoptions.h:53-59` 定义四种模式（从弱到强）：

| 模式 | SQL | 冲突语义 |
|---|---|---|
| LockTupleKeyShare | `FOR KEY SHARE` | 只挡"改键/删行"，允许改非键列 |
| LockTupleShare | `FOR SHARE` | 挡一切修改，允许共享 |
| LockTupleNoKeyExclusive | `FOR NO KEY UPDATE`（普通 UPDATE 隐含） | 挡其他排他，允许 KeyShare |
| LockTupleExclusive | `FOR UPDATE`（DELETE/改键 UPDATE 隐含） | 全排他 |

四种模式塞进 infomask 的几个位（`htup_details.h:194-203`）：

```c
#define HEAP_XMAX_KEYSHR_LOCK   0x0010  /* xmax is a key-shared locker */
#define HEAP_XMAX_EXCL_LOCK     0x0040  /* xmax is exclusive locker */
#define HEAP_XMAX_LOCK_ONLY     0x0080  /* xmax, if valid, is only a locker */
#define HEAP_XMAX_SHR_LOCK  (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)
```

`HEAP_XMAX_LOCK_ONLY` 是可见性判断里反复出现的那个"xmax 只是锁"标志。KeyShare/NoKeyUpdate 这对模式是 PG 9.3 为外键并发引入的：外键检查只需锁"键不变"，不必挡住更新非键列的 UPDATE——大幅减少了外键引发的锁等待。

### 8.7.3 multixact：多个事务锁同一行

共享类锁允许多个事务同时持有，但 xmax 只有一个 32 位槽位怎么办？答案是 **MultiXactId**（`src/backend/access/transam/multixact.c`，头部注释 `multixact.c:1-27` 是最佳导读）：造一个新的"组合 ID"写进 xmax，并置 `HEAP_XMAX_IS_MULTI`（`htup_details.h:209`）标志；组合 ID 在 pg_multixact 的**两个 SLRU**（offsets + members）里展开成成员数组，每个成员 = (XID, 锁模式标志)。

- 创建：`MultiXactIdCreate()`（`multixact.c:358`）/ `MultiXactIdCreateFromMembers()`（`multixact.c:715`）；已有 multixact 再加成员则要**新建**一个包含全部旧成员+新成员的 multixact（multixact 不可变）；
- 读取：`GetMultiXactIdMembers()`（`multixact.c:1172`）。可见性判断中 `HeapTupleGetUpdateXid()` 就是靠它从成员里挑出真正做了 UPDATE 的那一个（至多一个成员是 updater，其余是纯 locker）；
- multixact 自身也是会回卷的计数器，有独立的 freeze（`vacuum_multixact_freeze_*` 参数族）——PG 9.3/9.4 时代多个惨痛的数据损坏 bug 都出在这里，运维监控 `datminmxid` 与监控 XID 年龄同等重要。

### 8.7.4 SELECT FOR UPDATE 的路径：heap_lock_tuple

`SELECT ... FOR UPDATE` 的执行计划顶端是 LockRows 节点（`nodeLockRows.c`），对每行经 `table_tuple_lock()` 落到堆实现 `heap_lock_tuple()`（`src/backend/access/heap/heapam.c:4728`）。主线流程：

1. 读元组，`HeapTupleSatisfiesUpdate()` 判断当前状态：无人占用 → 直接走 3；被占用 → 走 2；
2. **冲突等待**：根据双方模式查 `tupleLockExtraInfo` 表（`heapam.c:129-158`，四种模式对应的重量级锁级别与 multixact 状态）决定是否冲突。冲突则视 `wait_policy` 三选一：阻塞等待（`XactLockTableWait` 等持有者的事务锁，或 multixact 则逐成员等）、`SKIP LOCKED` 跳过、`NOWAIT` 报错。等待前还会先拿该元组的 `LOCKTAG_TUPLE` 重量级锁作为"排队票"，防止饥饿（醒来的人凭它优先接手，接手后立刻释放——所以 pg_locks 里 tuple 锁一闪而过）；
3. **落锁**：`compute_new_xmax_infomask()` 算出新 xmax（自己的 XID，或与现存 locker 合并成 multixact）与 infomask 位组合，写入元组头，记 WAL（`XLOG_HEAP_LOCK`），完事。

READ COMMITTED 下若行在等待期间被改了，上层 LockRows 用 EPQ 拿新版本重验 WHERE；REPEATABLE READ 下直接报串行化失败——与 8.3.4 的语义一致。

### 小结

- 行锁写在元组 xmax + infomask 里，不占锁表内存；等行锁转化为等持有者的"事务锁"；
- 四种行锁模式（Key Share / Share / No Key Update / Update）为外键并发而精细化；
- 多事务共持一行时 xmax 换成 MultiXactId，成员展开存在 pg_multixact 双 SLRU，有独立回卷风险；
- PG 无间隙锁：RR 靠快照防幻读，SERIALIZABLE 靠 SSI；`FOR UPDATE` 的等待礼仪由一闪而过的 tuple 锁维持公平。

## 8.8 死锁检测：deadlock.c

### 8.8.1 懒触发设计

PG 不在每次加锁时做死锁预防（那是 2PL 系统的做法），而是**先睡下去，睡过 `deadlock_timeout`（默认 1s，`proc.c:62` `DeadlockTimeout = 1000`）才起来查一次**。实现：`ProcSleep` 挂起前 `enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout)`（`proc.c:1438`），定时器到点置标志，等待循环里发现 `got_deadlock_timeout` 后调用 `CheckDeadLock()`（`proc.c:1528-1530`）。

理由是纯粹的概率账：死锁在设计良好的应用里极罕见，而检测需要**锁住整个锁表的全部 16 个分区**（`DeadLockCheck` 的调用前提，`deadlock.c:213`）、遍历等待图——每次加锁都做等于常态化地付灾难保险的全额保费。懒触发把成本压到"只有真的等了很久的进程才付"。若检测结果是"无死锁"，进程继续睡，**此后不再重复检测**（真死锁不会自己消失，第一次检测没有就永远不会有——除非等待关系变化，而那时会有别的进程去检测）。

### 8.8.2 DeadLockCheck 的算法

等待图（wait-for graph, WFG）：节点 = 进程；硬边 A→B = "A 等待 B 已**持有**的锁"；PG 还独有**软边** = "A 等待 B 只是因为 B 在同一个等待队列里**排在 A 前面**且模式冲突"——软边对应的死锁可以通过**重排队列**化解，不必杀事务。

入口 `DeadLockCheck()`（`deadlock.c:220`）：

```c
if (DeadLockCheckRecurse(proc))       // deadlock.c:231
    return DS_HARD_DEADLOCK;          // 无解: 调用者将中止本事务
/* 有解: 按 waitOrders 重排若干等待队列 */
for (int i = 0; i < nWaitOrders; i++) { ...重排 lock->waitProcs...; ProcLockWakeup(...); }
return nWaitOrders > 0 ? DS_SOFT_DEADLOCK : DS_NO_DEADLOCK;
```

- `DeadLockCheckRecurse()`（`deadlock.c:312`）：先 `TestConfiguration` → `FindLockCycle()`（`deadlock.c:446`，DFS 找环，递归体 `FindLockCycleRecurse`，`deadlock.c:457`）。找到的环若全是硬边 → 硬死锁，返回失败；若含软边，则对"翻转这些软边"的每种组合（约束集）递归尝试——本质是在搜索"是否存在某个等待队列重排方案使图无环"；
- `TopoSort()`（`deadlock.c:862`）：对每个需要重排的等待队列，在"必须 A 在 B 前"的约束下做**拓扑排序**，产出新队列顺序；
- 软死锁解法生效：重排队列 + `ProcLockWakeup` 唤醒如今不再冲突的等待者——**没有任何事务被杀**；硬死锁则由发起检测的进程自杀（报 `deadlock detected`，`DeadLockReport()` 打印从 `deadlockDetails[]` 收集的环路详情）。

注意"谁检测谁死"：被牺牲的是**碰巧先睡满 deadlock_timeout 的那个**，不是代价最小的——PG 没有 InnoDB 那种"选 undo 量小的回滚"的受害者选择策略（InnoDB 从 8.0 起用主动检测 + 权重选择受害者）。另外 `DeadLockCheck` 还顺带处理"阻塞了 autovacuum"的情形（`deadlock.c:278-279`：返回 DS_BLOCKED_BY_AUTOVACUUM，调用者会去踢掉 autovacuum）。

### 小结

- 懒触发：睡满 deadlock_timeout 才检测一次，把昂贵的全锁表扫描留给罕见场景；
- 算法：等待图 DFS 找环；软边（队列顺序造成的等待）可通过拓扑排序重排队列无损化解，硬环才杀事务；
- 受害者 = 首个检测到死锁的等待者，无代价加权选择。

---

## 8.9 SSI：可串行化快照隔离（predicate.c）

### 8.9.1 快照隔离的洞：write-skew

快照隔离（PG 的 REPEATABLE READ）并不可串行化。经典 **write-skew** 例子——值班约束"至少一名医生在岗"：

```sql
-- 初始: Alice、Bob 都在岗 (on_call = true)
-- 事务 T1 (Alice 请假):                 -- 事务 T2 (Bob 请假):
BEGIN ISOLATION LEVEL REPEATABLE READ;   BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM doctors             SELECT count(*) FROM doctors
  WHERE on_call;        -- 看到 2          WHERE on_call;        -- 看到 2
UPDATE doctors SET on_call = false       UPDATE doctors SET on_call = false
  WHERE name = 'Alice';                    WHERE name = 'Bob';
COMMIT;  -- 成功                          COMMIT;  -- 也成功 → 没人值班了!
```

两个事务改的是**不同的行**，行锁毫无用武之地；各自的快照里约束都成立；串行执行则必有一个会失败。这就是"读了对方将要写的、写了对方已经读的"交叉——一对互相的 **rw-antidependency**（读写反依赖：T1 读了被 T2 并发写掉的数据，记 T1 --rw--> T2）。

InnoDB 的 SERIALIZABLE 用 2PL（所有读隐式加共享 next-key 锁）粗暴地阻塞掉这一切；PG 9.1 起采用 **SSI**（Cahill, Röhm & Fekete, SIGMOD 2008 —— `README-SSI:20` 列为第一参考文献，PG 是该论文的首个工业级实现）：照常跑快照隔离，**只监测危险模式，事后精确回滚**。

### 8.9.2 危险结构：两条连续的 rw 边

理论核心（Fekete et al. 2005 + Cahill 2008，`README-SSI:155-168`）：快照隔离下每个串行化异常的依赖环里，**必然存在一个"枢轴"事务 Tpivot，身上挂着两条连续的 rw 反依赖边**：

```
      Tin ------> Tpivot ------> Tout
            rw             rw
```

且 Tout 必须是环中最先提交的。于是 SSI 只需：追踪并发事务间的 rw 边；一旦某事务集齐"一进一出"两条 rw 边（成为 pivot）且提交顺序条件满足，就地回滚其中一个。**不追踪 wr/ww 依赖、允许假阳性**（危险结构不一定真的闭合成环）——用少量误杀换极低的追踪成本，这是 SSI 实用化的关键取舍。

上面 write-skew 里 T1、T2 互为 rw 依赖：T1 --rw--> T2 且 T2 --rw--> T1，本身就是环，两个事务都是 pivot，后提交者被回滚，报 `could not serialize access due to read/write dependencies among transactions`。

### 8.9.3 实现：SIREAD 锁与冲突检测

`src/backend/storage/lmgr/predicate.c`（约 5000 行，配 `README-SSI`）。

**SIREAD 锁**——记录"我读过什么"的**不阻塞任何人的谓词锁**：SERIALIZABLE 事务每读一个元组/页/整表，就调 `PredicateLockTID` / `PredicateLockPage()`（`predicate.c:2528`）/ `PredicateLockRelation()`（`predicate.c:2505`）在独立的谓词锁表（不在 lock.c 的锁表里）登记。三级粒度自动升级（元组多了并成页锁、页多了并成表锁，`README-SSI:293` 附近）以控制内存；索引扫描在**索引页**上留 SIREAD 锁——这就是 PG 版的"谓词锁"：锁住"我搜过的范围"，一个走 B 树的范围查询留下的索引页锁，能捕捉到未来插入该范围的行（功能上对应 InnoDB 间隙锁的检测面，但**只记录、不阻塞**）。全表扫描则直接留表级 SIREAD 锁——所以 SERIALIZABLE 下顺序扫描大表极易与任何写冲突，实践中要保证查询走索引。SIREAD 锁还必须**在事务提交后继续保留**（直到所有并发事务结束），因为 rw 冲突可能在读者提交后才发生。

**rw 边的两个采集点**：

- **写时检查读者**：任何 INSERT/UPDATE/DELETE 调 `CheckForSerializableConflictIn()`（`predicate.c:4265`）——查谁在我要写的目标上留过 SIREAD 锁 → 建边 reader --rw--> me；
- **读时检查写者**：可见性判断附近调 `CheckForSerializableConflictOut()`（`predicate.c:3952`）——我读到的元组是否被某并发事务改过（哪怕那个版本对我不可见）→ 建边 me --rw--> writer。

**危险结构判定**：每次建边时 `OnConflict_CheckForSerializationFailure()`（`predicate.c:4465`）检查新边是否使 reader 或 writer 成为 pivot。该函数头注释（`predicate.c:4451-4461`）把判定规则写得极清楚，正文三段检查依次是：

1. writer 已提交且已有 conflict-out（`predicate.c:4485`）：`R --rw--> W --rw--> T2` 且 T2 先于 W 提交 → 必须回滚（只能杀还活着的 R）；
2. writer 因新边成为 pivot（`predicate.c:4510-4532`）：遍历 `writer->outConflicts` 找已 prepared/committed 的 T2，并应用两条豁免：reader/writer 谁在 T2 之前提交则无害；**Tin 只读且其快照晚于 Tout 提交则无害**（`README-SSI:183-198` 给出了这条 PG 原创优化的证明）；
3. reader 因新边成为 pivot（`predicate.c:4547` 起）：对称地查 `reader->inConflicts`。

提交前最后一道闸：`PreCommit_CheckForSerializationFailure()`（`predicate.c:4632`）。已提交事务的冲突信息保留在共享内存 SERIALIZABLEXACT 结构里，旧事务的摘要挤进 SLRU（`pg_serial`）。`DEFERRABLE` 只读事务则干脆等到一个"绝对安全"的快照再执行，全程免疫回滚。

**工程上的收益边界**：SSI 的全部开销只由 SERIALIZABLE 事务承担，RC/RR 事务一分钱不付；代价是 SERIALIZABLE 下需要准备重试（假阳性 + 真冲突都表现为 40001 错误）。应用侧的正确姿势就一句话：**循环重试串行化失败的事务**。

### 小结

- 快照隔离存在 write-skew：不相交的写 + 交叉的读依赖，行锁与快照都拦不住；
- SSI 定理：异常环必含 pivot（两条连续 rw 入出边）且 Tout 最先提交——只监测这个模式，允许假阳性；
- 实现三件套：不阻塞的 SIREAD 谓词锁（元组/页/表三级 + 索引页范围锁）、读写两侧的冲突采集（ConflictOut/ConflictIn）、建边与提交时的危险结构判定；
- 与 InnoDB SERIALIZABLE（2PL 阻塞）相比：SSI 无读写阻塞、无间隙锁死锁，换来"事后回滚需重试"。

---

## 自测问题

**1. 为什么 PG 的只读事务不分配真 XID？这带来哪几层好处？**

答：`StartTransaction()` 只分配本地生成的 vxid（进程槽位号+本地计数器），真 XID 推迟到第一次写操作时由 `AssignTransactionId()`（xact.c:637）领取。好处：不消耗 32 位 XID 空间（不推进回卷时钟）；不争抢 XidGenLock；不占 CLOG；别人拍快照时被 `xid == InvalidTransactionId` 一行跳过（procarray.c:2232），不进 xip[]；提交时无需写 WAL。只读负载因此近乎零事务开销。

**2. `TransactionIdPrecedes` 为什么用 `(int32)(id1 - id2) < 0` 而不是 `id1 < id2`？这个比较的有效范围是多少？**

答：XID 是回绕的 32 位环，普通比较在回卷后失效。无符号减法自然回绕，强转 int32 后：id1 落在 id2"后方半圈内"则差为负 → id1 更老（transam.h:262-276）。有效前提是两个 XID 相距不超过 2^31，因此必须靠 freeze 保证磁盘上不存在年龄超过约 21 亿的普通 XID。

**3. 快照的 xmax 为什么取 latestCompletedXid+1 而不是 nextXid？两者语义差在哪？**

答：见 procarray.c:2194-2196。取 latestCompletedXid+1 保证 [xmax, nextXid) 之间那些"已分配但都还在跑"的 XID 无需逐个塞进 xip[]——凡 ≥ xmax 一律视为运行中，语义正确且黑名单更短。若取 nextXid，则这一段活跃 XID 必须全部进 xip[]，快照更大、拍照更慢，结论却完全一样。

**4. hint bit 不写 WAL，为什么在异步提交下设置它之前要检查 commit LSN 是否已刷盘？**

答：SetHintBitsExt（heapam_visibility.c:142-166）。若事务的 commit WAL 记录未刷盘而带着 HEAP_XMIN_COMMITTED 的数据页先落盘，崩溃恢复后该事务实际未提交，页上却有"已提交"假章——违反 WAL 先行原则造成数据损坏。所以仅当 `XLogNeedsFlush(commitLSN)` 为假（commit 已持久化）或页面 LSN 能保证顺序时才设置，否则放弃，留待后人。

**5. 一个事务开了 100 个 SAVEPOINT 且都执行了写操作，会对整个数据库的读性能造成什么影响？机制是什么？**

答：该 backend 的 PGPROC 子 XID 缓存（64 个）溢出，此后所有会话拍到的快照都带 `suboverflowed = true`。可见性判断中 `XidInMVCCSnapshot`（snapmgr.c:1868）对每个落入 [xmin, xmax) 窗口的 XID 不能再查数组，必须调 `SubTransGetTopmostTransaction` 去 pg_subtrans（SLRU，可能引发 I/O）爬父链折算成顶层 XID——全库纯读查询都被拖慢，直到该事务结束且窗口滑过。

**6. fastpath 锁机制中，弱锁方和强锁方各做什么来保证不漏检冲突？**

答：弱锁方（lock.c:984-1017）：在自己 PGPROC 的 fpInfoLock 内先检查 `FastPathStrongRelationLocks->count[分区]`，为零才把 (relid, mode) 写进自己的 fastpath 槽位，全程不碰共享锁表。强锁方（lock.c:1034-1055）：先把该分区强锁计数 +1（封路，使后续弱锁申请退回常规路径），再遍历所有 backend 的 PGPROC 把已存在的 fastpath 弱锁搬进共享锁表，然后做常规冲突检查。fpInfoLock 的内存屏障语义保证两边的"检查/写入"不会交错出双双走快路的漏检。

**7. PG 的行锁为什么不会像 InnoDB 那样出现"锁内存不足/锁升级"问题？等待行锁时实际在等什么？**

答：行锁不进锁表，而是把 locker 的 XID（或 MultiXactId）写进元组 xmax、用 infomask 位标注模式（htup_details.h:194-209）——锁状态存储在数据页上，数量不受共享内存限制，也无需锁升级。等待行锁 = 对持有者 XID 的 `LOCKTAG_TRANSACTION` 锁做共享申请，持有者事务结束时释放其排他事务锁，等待者被唤醒；排队公平性由短暂持有的 LOCKTAG_TUPLE 锁维持。代价是加行锁产生页面写和 WAL。

**8. 用 write-skew 例子说明"危险结构"，并解释 SSI 为什么允许假阳性。**

答：两名医生并发请假：T1 读全表(看到 2 人在岗)改 Alice，T2 读全表改 Bob。T1 读了 T2 要写的行 → T1 --rw--> T2；对称地 T2 --rw--> T1，构成环，每个事务都有一进一出两条 rw 边（pivot），即危险结构 `Tin --rw--> Tpivot --rw--> Tout`（predicate.c:4451-4457, README-SSI:155-168），后提交者被回滚。SSI 只追踪 rw 边而不追踪 wr/ww 依赖，因此检测到的危险结构未必闭合成真实环——这是有意的取舍：完整环检测的追踪成本高得多，而假阳性只导致偶尔多回滚一个事务（应用重试即可），实践中比例很低。

---

> **动手实验建议**：开两个 psql，会话 A `BEGIN; UPDATE t SET ...` 不提交；会话 B 观察 `pg_locks`（注意 fastpath 列、granted=false 的 transactionid 锁）与 `SELECT xmin, xmax, ctid FROM t` 的版本差异；再用 gdb attach 到 backend，在 `HeapTupleSatisfiesMVCC`（heapam_visibility.c:939）与 `GetSnapshotData`（procarray.c:2114）下断点，单步走一次完整的"拍快照 → 判可见"流程。SSI 实验：两个 SERIALIZABLE 会话复现 8.9.1 的 write-skew，观察 40001 错误与 `pg_stat_database.deadlocks` 不增加（SSI 回滚不是死锁）。
