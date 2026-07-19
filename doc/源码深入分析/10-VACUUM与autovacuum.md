# 第 10 篇 · VACUUM 与 autovacuum：为 MVCC 付账（v2）

> 对应《PostgreSQL 源码学习指南》第 11 章。源码版本：本仓库 master 分支（20devel）。
> 文中所有 `文件:行号` 均已在本仓库核实；引用的代码片段为真实源码逐字摘录（省略处以 `...` 或文字注明）。
> v2 较 v1 的升级：七个核心函数从"摘录"升级为"全函数走读"；补齐"所以然"推演（三阶段顺序、LP_REDIRECT、freeze 时机、cost 限流、TidStore）；新增不变式清单、设计历史（git 考古）、四个 what-if 故障推演。

## 本章导读

第 09 篇讲过：PG 没有 undo，回滚免费，代价是垃圾（死元组）留在堆里。VACUUM 就是那张迟早要付的账单。这是 PG 与 MySQL 差异最大的运维领域——InnoDB 的 purge 只是遥远的近似，你必须把它当全新知识学。

本章路线（每一站都对应一个"全函数走读"）：

1. **谁算死**：`HeapTupleSatisfiesVacuum` / `HeapTupleSatisfiesVacuumHorizon` 全走读——五种返回值各自的场景；
2. **死到什么程度才能回收**：`ComputeXidHorizons` 全走读——本地快照、复制槽、walsender、prepared 事务，每一路"取小"的来源逐个解释谁会钉住视界；
3. **怎么回收**：`heap_vacuum_rel` → `lazy_scan_heap` → `lazy_scan_prune` 主线走读；三阶段为什么不可交换（推演悬垂指针灾难）；PG 17 的 TidStore；
4. **查询路径上的顺手回收**：`heap_page_prune_and_freeze` 与 `heap_prune_chain` 全走读——版本链清理的指针舞蹈与 LP_REDIRECT 的所以然；
5. **回卷保护**：freeze 为什么不能太早也不能太晚；xid_age 逼近 2^31 时数据库为什么必须拒绝写入（wraparound 的数学）；
6. **自动化**：`relation_needs_vacanalyze` 与 `vacuum_delay_point` 全走读；为什么用 cost-based 限流而不是 IO 带宽限流；
7. 不变式清单、设计历史、what-if 故障推演、与 InnoDB purge 的系统对照。

## 前置知识

- **元组头**（`src/include/access/htup_details.h`）：每行有 `xmin`（创建者 XID）、`xmax`（删除者 XID）、`ctid`（版本链指针）与 infomask 标志位。UPDATE = 插新版本 + 旧版本设 xmax。`HEAP_XMIN_FROZEN`（htup_details.h:206）= `HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID` 两位同置，表示"已冻结"。
- **行指针（ItemId / line pointer）**：页内槽位数组，四种状态——`LP_NORMAL`（指向元组）、`LP_UNUSED`（可复用）、`LP_DEAD`（元组已删、存储已回收，但槽位还被索引指着）、`LP_REDIRECT`（重定向到同页另一槽位）。**索引项里存的 TID = (块号, 槽位号)**，这一点是理解本章一半内容的钥匙。
- **CLOG**：每事务 2 bit 的提交状态位图；hint bit 把 CLOG 查询结果缓存在元组头。
- **VM（visibility map）**：每个堆页 2 个 bit：all-visible（页上全部元组对所有事务可见）、all-frozen（全部已冻结）。
- **XID 回卷**：XID 是 32 位循环计数器，比较按模 2^31 的"半圈"规则（§4.1 展开数学细节）。
- **对 MySQL 用户**：InnoDB 的旧版本在 undo 段里，由 purge 线程沿 history list 清理；PG 的旧版本就在堆页里，由 VACUUM 清理。`history list length` 的 PG 对应物大致是 `pg_stat_user_tables.n_dead_tup` + 最老快照年龄。InnoDB 的 `DB_TRX_ID` 是 6 字节（2^48），实践中不回卷，所以 InnoDB 没有 freeze 这一整个机制——这是两边清理负担的一个本质差异。

---

## 1. 为什么需要 VACUUM：死元组的判定与回收边界

### 1.1 MVCC 债务

每条 UPDATE/DELETE 都会留下旧版本元组。它们不能立即删除——可能还有老快照要读它们。于是产生三个必须回答的问题：

1. 一个元组什么时候从"可能还有人要读"变成"确定没人要读"（死元组）？
2. 这个边界（horizon）由谁、如何计算？
3. 谁负责把死元组占的空间收回来？

第 3 问的答案是 VACUUM（加上查询路径上的 HOT pruning）。前两问的答案在 `heapam_visibility.c` 和 `procarray.c`。

### 1.2 HeapTupleSatisfiesVacuum：给元组验尸（全函数走读）

先看外层。`src/backend/access/heap/heapam_visibility.c:1112`，函数完整只有 20 行：

```c
HTSV_Result
HeapTupleSatisfiesVacuum(HeapTuple htup, TransactionId OldestXmin,
                         Buffer buffer)
{
    TransactionId dead_after = InvalidTransactionId;
    HTSV_Result res;

    res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &dead_after);

    if (res == HEAPTUPLE_RECENTLY_DEAD)
    {
        Assert(TransactionIdIsValid(dead_after));

        if (TransactionIdPrecedes(dead_after, OldestXmin))
            res = HEAPTUPLE_DEAD;
    }
    else
        Assert(!TransactionIdIsValid(dead_after));

    return res;
}
```

结构是两层：内层 `HeapTupleSatisfiesVacuumHorizon` 做与视界无关的定性判断，把"死亡时刻"（删除者 XID）通过 `dead_after` 交给上层；上层只做一次 `dead_after < OldestXmin` 的比较。**为什么拆两层？** 为了让不同调用者用不同的视界做同一比较：VACUUM 用开工时一次算好的固定 `OldestXmin`（本函数）；HOT pruning 用可动态收紧的 `GlobalVisState`（`HeapTupleSatisfiesNonVacuumable`，heapam_visibility.c:1344，比较改为 `GlobalVisTestIsRemovableXid(snapshot->vistest, dead_after, true)`）；剪枝路径 `heap_prune_satisfies_vacuum`（pruneheap.c:1422）则两者都用（§3.2）。定性逻辑只写一份，比较策略各自选择——这个拆分是 PG 14（commit dc7420c2c9 的配套重构）引入的。

返回值五种（`src/include/access/heapam.h:136-143`，逐字）：

```c
typedef enum
{
    HEAPTUPLE_DEAD,             /* tuple is dead and deletable */
    HEAPTUPLE_LIVE,             /* tuple is live (committed, no deleter) */
    HEAPTUPLE_RECENTLY_DEAD,    /* tuple is dead, but not deletable yet */
    HEAPTUPLE_INSERT_IN_PROGRESS,   /* inserting xact is still in progress */
    HEAPTUPLE_DELETE_IN_PROGRESS,   /* deleting xact is still in progress */
```

### 1.3 HeapTupleSatisfiesVacuumHorizon 全函数走读

`heapam_visibility.c:1146-1329`。函数按"先验插入者，再验删除者"两大段展开，每个 return 对应一种明确场景。下面按控制流全量过一遍（代码逐字，只省略部分注释）。

**第一段：插入者提交了吗？（heapam_visibility.c:1163-1213）**

```c
    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return HEAPTUPLE_DEAD;
        else if (!HeapTupleCleanMoved(tuple, buffer))
            return HEAPTUPLE_DEAD;
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            if (tuple->t_infomask & HEAP_XMAX_INVALID)  /* xid invalid */
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            /* only locked? run infomask-only check first, for performance */
            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ||
                HeapTupleHeaderIsOnlyLocked(tuple))
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            /* inserted and then deleted by same xact */
            if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)))
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            /* deleting subtransaction must have aborted */
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        }
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
        {
            ...
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        }
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
                        HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
                        InvalidTransactionId);
            return HEAPTUPLE_DEAD;
        }
    }
```

逐分支的场景：

- **`HEAPTUPLE_DEAD`（分支 1：xmin invalid hint 已置，1165-1166）**：插入者已知中止。场景：`BEGIN; INSERT; ROLLBACK` 留下的行，之前某次访问已把 `HEAP_XMIN_INVALID` hint 写好。这种元组**从未对任何人可见过**，无需与任何视界比较，直接可回收——这正是"PG 没有 undo"的清理路径：InnoDB 回滚时用 undo 物理抹掉新行，PG 把烂摊子留给下一个路过的清理者。
- **`HEAPTUPLE_DEAD`（分支 2：`HeapTupleCleanMoved`，1167-1168）**：处理上古 `VACUUM FULL`（9.0 前的 MOVED_OFF/MOVED_IN）遗迹，移动它的老 vacuum 若已中止则判死。现代库几乎不会走到。
- **`HEAPTUPLE_INSERT_IN_PROGRESS` / `HEAPTUPLE_DELETE_IN_PROGRESS`（当前事务分支，1169-1182）**：插入者是**自己**。场景：一个事务内 INSERT 后又在同事务 UPDATE/DELETE 该行，然后这个事务自己触发了剪枝（例如页满）。注意"insert 后又被同事务删除"返回 DELETE_IN_PROGRESS，而"删除它的子事务已中止"仍返回 INSERT_IN_PROGRESS——调用者据此决定等谁。
- **`HEAPTUPLE_INSERT_IN_PROGRESS`（他人在跑，1183-1194）**：插入者是仍在运行的其他事务。函数头顶注释（1186-1191）解释了一个刻意的粗糙：此处**不去**细分"其实它已经被同一个在跑事务标记了删除"（那要看 xmax），统一报 INSERT_IN_PROGRESS——因为从其他 backend 视角这就是事实，且让调用者去等 xmin 比等 xmax 更有益。
- **提交分支（1195-1197）**：查 CLOG 确认提交，顺手 `SetHintBits` 写下 `HEAP_XMIN_COMMITTED`。**VACUUM 因此是 hint bit 的主要生产者之一**——大批量 COPY 后第一次 vacuum/顺序扫描往往伴随大量"读放大写"（hint bit 落盘，若开 checksum 还会产生 FPI，见 §3.2 的 `did_tuple_hint_fpi`）。
- **`HEAPTUPLE_DEAD`（分支 3：中止或崩溃，1198-1206）**：CLOG 说没提交也没在跑 ⇒ 中止或宕机遗留。写 `HEAP_XMIN_INVALID` hint 并判死。

**第二段：插入者已提交，看删除者（heapam_visibility.c:1219-1328）**

```c
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return HEAPTUPLE_LIVE;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        ...
        return HEAPTUPLE_LIVE;
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        TransactionId xmax = HeapTupleGetUpdateXid(tuple);
        ...
        if (TransactionIdIsInProgress(xmax))
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        else if (TransactionIdDidCommit(xmax))
        {
            ...
            *dead_after = xmax;
            return HEAPTUPLE_RECENTLY_DEAD;
        }
        else if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
        {
            ...
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }

        return HEAPTUPLE_LIVE;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
    {
        if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
                        HeapTupleHeaderGetRawXmax(tuple));
        else
        {
            ...
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                        InvalidTransactionId);
            return HEAPTUPLE_LIVE;
        }
        ...
    }

    /*
     * Deleter committed, allow caller to check if it was recent enough that
     * some open transactions could still see the tuple.
     */
    *dead_after = HeapTupleHeaderGetRawXmax(tuple);
    return HEAPTUPLE_RECENTLY_DEAD;
```

逐分支场景：

- **`HEAPTUPLE_LIVE`（xmax invalid，1219-1220）**：没人删过。最常见路径。
- **`HEAPTUPLE_LIVE`（locked only，1222-1260）**：xmax 只是 `SELECT ... FOR UPDATE/SHARE` 留下的行锁，不是删除。此分支还顺手把已结束的锁事务的 `HEAP_XMAX_INVALID` hint 写掉（1230-1251，含 multixact 锁的分情况），"We don't really care whether xmax did commit, abort or crash"（1254 原注释）——锁过≠删过，行永远是活的。
- **multixact 更新分支（1262-1297）**：xmax 是 MultiXactId（更新+并发锁并存时的复合体）。取出真正的 updater XID：在跑 ⇒ `DELETE_IN_PROGRESS`；已提交 ⇒ `RECENTLY_DEAD` 且 `*dead_after = xmax`——注意 1276-1283 的注释：即使 multixact 里还有锁事务在跑也**必须**允许剪枝（只要 updater 过了视界），否则会出现"updater 该被视界移除却没被剪掉"的矛盾状态；剩余锁者的锁在新版本元组上仍然存在，所以剪掉旧版本无害。updater 中止 ⇒ 清 hint、`LIVE`。
- **xmax 未带 committed hint（1299-1321）**：查 CLOG：在跑 ⇒ `DELETE_IN_PROGRESS`（场景：DELETE 尚未提交，此时任何人都不能当它死）；已提交 ⇒ 写 hint 落到最后一段；中止/崩溃 ⇒ 写 `HEAP_XMAX_INVALID`、`LIVE`（场景：DELETE 后 ROLLBACK，行复活）。
- **`HEAPTUPLE_RECENTLY_DEAD`（1323-1328）**：删除者确认提交。**函数到此为止不做视界判断**，只把死亡时刻 `xmax` 填进 `*dead_after` 交给上层。"最近死"是否升格为"真死"，完全取决于调用者拿它与哪个视界比。

VACUUM 对五种结果的处置：`DEAD` ⇒ 回收；`RECENTLY_DEAD` ⇒ 保留（计入日志的 "dead but not yet removable"）；`LIVE` ⇒ 保留并考虑冻结；两种 `IN_PROGRESS` ⇒ 保留。

### 1.4 ComputeXidHorizons 全函数走读：视界从哪来

`OldestXmin` 由 `GetOldestNonRemovableTransactionId`（procarray.c:1943）调用 `ComputeXidHorizons`（procarray.c:1673-1903）计算。这是整个膨胀问题的"案发第一现场"，值得全量走读。

**① 初始化：上界 = latestCompletedXid + 1（procarray.c:1684-1721）**

```c
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    h->latest_completed = TransamVariables->latestCompletedXid;
    ...
        initial = XidFromFullTransactionId(h->latest_completed);
        Assert(TransactionIdIsValid(initial));
        TransactionIdAdvance(initial);

        h->oldest_considered_running = initial;
        h->shared_oldest_nonremovable = initial;
        h->data_oldest_nonremovable = initial;
```

MIN() 归约需要一个安全初值：`latestCompletedXid + 1` 是"未来可能进入 ProcArray 的 XID 的下界"，保证即使当前没有任何活动事务，结果也不会高估（1688-1693 原注释）。临时表的视界单独处理（1705-1720）：只有本 backend 能改自己的临时表，所以 `temp_oldest_nonremovable` = 自己的 `MyProc->xid`（有则）或 initial——**临时表 vacuum 完全不受别人的长事务影响**。

**② 复制槽水位：在锁内取（procarray.c:1728-1729）**

```c
    h->slot_xmin = procArray->replication_slot_xmin;
    h->slot_catalog_xmin = procArray->replication_slot_catalog_xmin;
```

这两个值是所有复制槽 xmin 的最小值（由 slot 子系统维护）。**谁会钉住它**：
- 物理槽 + `hot_standby_feedback=on`：备库把自己最老查询的 xmin 反馈给主库 walsender，walsender 写进槽的 `xmin`——备库上一个 8 小时的报表查询，就这样把主库全库的视界钉住 8 小时；
- 逻辑槽：`catalog_xmin` 保护逻辑解码所需的**系统目录**行版本。下游（CDC/订阅者）停摆时槽不前进，目录表（以及被视为 catalog 的表）的死元组无法回收。

**③ 主循环：遍历所有 backend 取最小（procarray.c:1731-1804）**

```c
    for (int index = 0; index < arrayP->numProcs; index++)
    {
        int         pgprocno = arrayP->pgprocnos[index];
        PGPROC     *proc = &allProcs[pgprocno];
        int8        statusFlags = ProcGlobal->statusFlags[index];
        TransactionId xid;
        TransactionId xmin;

        /* Fetch xid just once - see GetNewTransactionId */
        xid = UINT32_ACCESS_ONCE(other_xids[index]);
        xmin = UINT32_ACCESS_ONCE(proc->xmin);
        ...
        xmin = TransactionIdOlder(xmin, xid);

        /* if neither is set, this proc doesn't influence the horizon */
        if (!TransactionIdIsValid(xmin))
            continue;
        ...
        h->oldest_considered_running =
            TransactionIdOlder(h->oldest_considered_running, xmin);
        ...
        if (statusFlags & (PROC_IN_VACUUM | PROC_IN_LOGICAL_DECODING))
            continue;

        /* shared tables need to take backends in all databases into account */
        h->shared_oldest_nonremovable =
            TransactionIdOlder(h->shared_oldest_nonremovable, xmin);
        ...
        if (proc->databaseId == MyDatabaseId ||
            MyDatabaseId == InvalidOid ||
            (statusFlags & PROC_AFFECTS_ALL_HORIZONS) ||
            in_recovery)
        {
            h->data_oldest_nonremovable =
                TransactionIdOlder(h->data_oldest_nonremovable, xmin);
        }
    }
```

这个循环里藏着四类"钉子"，逐个解释：

- **普通 backend 的快照 xmin（`proc->xmin`）**：backend 一拿快照（哪怕只读），`proc->xmin` 就记下快照的最老可能可见 XID。`REPEATABLE READ` 事务整个生命周期持有同一快照；`READ COMMITTED` 每条语句刷新，但一条跑 8 小时的大查询同样钉 8 小时。**`idle in transaction` 的连接**若已拿过快照，同样上榜。这就是"长事务危害"的机制根源：视界是全体最小值，一票否决。
- **backend 的 `xid` 与 xmin 双保险（1743-1750 注释）**：为什么两个都看？一个事务可能已分配 XID 但尚未设快照 xmin，反之亦然；取两者更老者，防止窗口期漏算。
- **prepared 事务（两阶段提交）**：`PREPARE TRANSACTION` 之后原 backend 断开，但 twophase 子系统为每个 prepared 事务在 ProcArray 里保留一个 **dummy PGPROC**（见 `src/backend/access/transam/twophase.c` 的 GXACT 机制），其 `xid` 就是 prepared 事务的 XID——所以它同样参与本循环取小。一个被遗忘三天的 prepared 事务 = 一个跑了三天的写事务，且**重启也不会消失**（它是持久化的）。`vacuum_get_cutoffs` 的告警 hint（vacuum.c:1174-1175）明确点名："You might also need to commit or roll back old prepared transactions, or drop stale replication slots."
- **walsender**：`hot_standby_feedback` 的反馈最终落在 walsender 自己的 `proc->xmin` 上，并带 `PROC_AFFECTS_ALL_HORIZONS` 标志（1788-1790 注释："This flag is used for hot standby feedback, which can't be tied to a specific database"）——因此**跨库生效**：备库查询钉住主库上所有数据库的视界，而普通 backend 只钉自己所在库（1796-1803 的 databaseId 过滤）。若 walsender 断开，这条钉子随之消失（这正是配复制槽的动机：让保护在断连时依然持久，见 1655-1664 的长注释）。

另外两个豁免/特例：`PROC_IN_VACUUM | PROC_IN_LOGICAL_DECODING` 的进程被跳过（1770-1771）——lazy vacuum 自己不写 XID 也不做快照读，互相不拖后腿（vacuum.c:1134-1140 注释同义）；`MyDatabaseId == InvalidOid`（backend 启动中）时保守地把所有库都算上（1781-1786）。

**④ 恢复模式补丁（procarray.c:1810-1828）**：standby 上 xid 由 KnownAssignedXids 机制管理，锁内取 `KnownAssignedXidsGetOldestXmin()`，释放锁后并入三个视界。

**⑤ 并入复制槽并派生四档视界（procarray.c:1838-1857）**

```c
    h->shared_oldest_nonremovable =
        TransactionIdOlder(h->shared_oldest_nonremovable, h->slot_xmin);
    h->data_oldest_nonremovable =
        TransactionIdOlder(h->data_oldest_nonremovable, h->slot_xmin);
    ...
    h->shared_oldest_nonremovable =
        TransactionIdOlder(h->shared_oldest_nonremovable,
                           h->slot_catalog_xmin);
    h->catalog_oldest_nonremovable = h->data_oldest_nonremovable;
    h->catalog_oldest_nonremovable =
        TransactionIdOlder(h->catalog_oldest_nonremovable,
                           h->slot_catalog_xmin);
```

槽的 `xmin` 打在 shared 与 data 两档上；`catalog_xmin` 只额外打在 catalog（和 shared，因为共享目录也是目录）档上——**这就是为什么滞留的逻辑槽首先毒害系统目录**：普通表还能回收，`pg_class`/`pg_attribute` 先膨胀（DDL 频繁的库尤其明显）。最终 `GetOldestNonRemovableTransactionId` 按表的类别四选一（procarray.c:1950-1960：shared / catalog / data / temp，分类逻辑 `GlobalVisHorizonKindForRel`，procarray.c:1909-1930）。

**⑥ 收尾（procarray.c:1902）**：`GlobalVisUpdateApply(h)` 把精确结果同步进近似缓存——1666-1671 的注释强调这不只是优化：若 vacuum 剪枝用的近似视界比后续判死用的还保守，HOT 清理会出现不一致，"breaking HOT"。

### 1.5 GlobalVisState：两条水位线的廉价近似

精确计算要拿 `ProcArrayLock` 扫全体 backend，读路径上的 HOT pruning 不能每次都付这个钱。`GlobalVisState`（procarray.c:184-191）用两条 64 位水位线夹出三段：

```c
struct GlobalVisState
{
    /* XIDs >= are considered running by some backend */
    FullTransactionId definitely_needed;

    /* XIDs < are not considered to be running by any backend */
    FullTransactionId maybe_needed;
};
```

- XID < `maybe_needed` ⇒ 一定可回收（两次比较，零锁）；
- XID ≥ `definitely_needed` ⇒ 一定不可回收；
- 夹在中间 ⇒ `GlobalVisTestIsRemovableXid`（procarray.c:4277）→ `GlobalVisTestIsRemovableFullXid`（procarray.c:4234）触发一次 `GlobalVisUpdate`（procarray.c:4212）重算并收紧水位，然后再答。

`GlobalVisTestFor(rel)`（procarray.c:4114）按表类别返回四档缓存（Shared/Catalog/Data/Temp）之一。这套机制来自 Andres Freund 2020 年的 snapshot scalability 系列（commit dc7420c2c9，见 §7），动机：把"计算全局视界"从拿快照的热路径上摘掉，改为惰性、按需、模糊但保守。

VACUUM 同时使用两者（vacuumlazy.c:786-801 注释，§2.1）：`OldestXmin` 是硬边界（freeze 正确性依赖它），`vistest` 用于剪枝（可能比 OldestXmin 更新、剪得**更多**，但绝不更少——`heap_prune_satisfies_vacuum` 先比 OldestXmin 再问 vistest，pruneheap.c:1439-1453）。

### 小结

死元组判定 = 两层验尸：内层定性（八种 return，各有明确场景，顺手生产 hint bit），外层拿"死亡时刻"与视界比较。视界 = min(所有 backend 的 xid/xmin、prepared 事务 dummy PGPROC、walsender 反馈 xmin、复制槽 xmin/catalog_xmin)，按 shared/catalog/data/temp 四档分发。任何一票都能把全库（或全目录）的回收边界钉死在过去。GlobalVisState 用两条水位线把高频判断降成两次 64 位比较。

---

## 2. lazy vacuum 主流程：heap_vacuum_rel 全走读

### 2.1 入口与准备（vacuumlazy.c:623-881）

`VACUUM` 命令经 `commands/vacuum.c` 调度，对每个堆表调用表 AM 的 `relation_vacuum` 回调即 `heap_vacuum_rel`（vacuumlazy.c:623）。按源码顺序过主线（省略计时/进度上报/错误上下文的样板代码）：

1. **进度与身份上报**（647-672）：`pgstat_progress_start_command`，并区分 started_by：手动 / autovacuum / autovacuum-wraparound——这就是 `pg_stat_progress_vacuum` 的数据源。
2. **状态体与索引**（686-709）：`vacrel = palloc0_object(LVRelState)` 承载本次 vacuum 的全部状态；`vac_open_indexes(..., RowExclusiveLock, ...)` 打开全部索引——**vacuum 只拿 `ShareUpdateExclusiveLock`（表）+ `RowExclusiveLock`（索引）**，与正常读写并发，这是"lazy"的含义。
3. **选项解析**（720-748）：`index_cleanup`（AUTO/ENABLED/DISABLED）与 `truncate` 参数落到 `do_index_vacuuming`/`do_index_cleanup`/`do_rel_truncate`/`consider_bypass_optimization` 四个布尔上；728 行先重置 `VacuumFailsafeActive`（递归调用时防串）。
4. **三组切点**（786-808）——这段注释值得逐字读（vacuumlazy.c:786-792）：

```c
    /*
     * Get cutoffs that determine which deleted tuples are considered DEAD,
     * not just RECENTLY_DEAD, and which XIDs/MXIDs to freeze.  Then determine
     * the extent of the blocks that we'll scan in lazy_scan_heap.  It has to
     * happen in this order to ensure that the OldestXmin cutoff field works
     * as an upper bound on the XIDs stored in the pages we'll actually scan
     * (NewRelfrozenXid tracking must never be allowed to miss unfrozen XIDs).
     */
    vacrel->aggressive = vacuum_get_cutoffs(rel, params, &vacrel->cutoffs);
    vacrel->rel_pages = orig_rel_pages = RelationGetNumberOfBlocks(rel);
    vacrel->vistest = GlobalVisTestFor(rel);

    /* Initialize state used to track oldest extant XID/MXID */
    vacrel->NewRelfrozenXid = vacrel->cutoffs.OldestXmin;
    vacrel->NewRelminMxid = vacrel->cutoffs.OldestMxact;
```

   顺序敏感：先算 OldestXmin **再**读 `rel_pages`，保证扫描范围内不会出现比 OldestXmin 更新、却被 NewRelfrozenXid 追踪漏掉的 XID。`NewRelfrozenXid` 从 OldestXmin 出发，扫描中每遇到一个保留的未冻结 XID 就往回收——终值就是能写进 `pg_class.relfrozenxid` 的新值。
5. **跳页开关**（815-827）：`VACUUM (DISABLE_PAGE_SKIPPING)` 强制 aggressive 且完全不信 VM（`skipwithvm = false`）——数据抢修用。
6. **eager scan 初始化**（834，`heap_vacuum_eager_scan_setup`）：PG 18 新机制（commit 052026c9b9，§7），普通 vacuum 按区域（`EAGER_SCAN_REGION_SIZE` = 4096 块，vacuumlazy.c:250）抽样扫描 all-visible-但未-frozen 页，把 aggressive vacuum 的冻结负担摊到平时。
7. **failsafe 预检 + dead_items 分配**（863-864）：`lazy_check_wraparound_failsafe`（§4.5）先行，保证表龄已危险时不再启动并行 vacuum；`dead_items_alloc(vacrel, params->nworkers)`（vacuumlazy.c:3416）按 `autovacuum_work_mem`（autovacuum worker）或 `maintenance_work_mem`（其余，3419-3421）创建 TidStore（§2.6）。
8. **主体**（881）：`lazy_scan_heap(vacrel)`——三阶段全部发生在这里面。
9. **收尾**（895-993）：`dead_items_cleanup`（顺带退出并行模式）→ 各索引 `pg_class` 统计（`update_relstats_all_indexes`）→ `should_attempt_truncation` ? `lazy_truncate_heap`（vacuumlazy.c:3122/3142，只截**尾部**连续空页，需要短暂 `AccessExclusiveLock`，有等待退让逻辑）→ 两个关键 Assert（928-935：aggressive 必须把 NewRelfrozenXid 推到 ≥ FreezeLimit）→ `skippedallvis` 时放弃推进（936-946，跳过了 all-visible 页就可能漏看未冻结 XID，只能保持原 relfrozenxid）→ `vac_update_relstats`（vacuum.c:1432）写回 relpages/reltuples/relallvisible/relallfrozen/relfrozenxid/relminmxid → `pgstat_report_vacuum` 让 `n_dead_tup` 归零。日志里那句 "removable cutoff: %u, which was %d XIDs old when operation ended"（vacuumlazy.c:1084-1086）直接打印 OldestXmin 的滞后量——**排查膨胀第一眼看这里**。

### 2.2 三阶段总览：顺序为什么不可交换（所以然 ①）

`lazy_scan_heap` 的头注释（vacuumlazy.c:1242-1277）给出三阶段结构与理由，其中 1256-1263 是全章最重要的一段不变式陈述（逐字）：

```
 *		Finally, invokes lazy_vacuum_heap_rel to vacuum heap pages, which
 *		largely consists of marking LP_DEAD items (from vacrel->dead_items)
 *		as LP_UNUSED.  This has to happen in a second, final pass over the
 *		heap, to preserve a basic invariant that all index AMs rely on: no
 *		extant index tuple can ever be allowed to contain a TID that points to
 *		an LP_UNUSED line pointer in the heap.  We must disallow premature
 *		recycling of line pointers to avoid index scans that get confused
 *		about which TID points to which tuple immediately after recycling.
```

三阶段：

```
阶段一 lazy_scan_heap：       顺序扫堆页 → 剪枝/冻结 → 残留 LP_DEAD 的 TID 存入 dead_items
阶段二 lazy_vacuum_all_indexes：对每个索引 ambulkdelete，删掉指向 dead_items 的索引项
阶段三 lazy_vacuum_heap_rel： 回到堆，把这些 LP_DEAD 槽位改成 LP_UNUSED（真正可复用）
```

**推演：如果先回收堆、再删索引项，会发生什么？**

设表 `t` 有索引 `idx(k)`，行 R（k=42）位于 TID (7,3)，已死。颠倒顺序执行：

1. t=0：把 (7,3) 从 LP_DEAD 置为 LP_UNUSED。此刻 `idx` 里仍有 `42 → (7,3)` 的索引项。
2. t=1：并发事务 `INSERT INTO t VALUES (k=99, ...)`。`heap_insert` 经 FSM 找到 7 号页，`PageAddItem` 复用了 LP_UNUSED 的 3 号槽——新行 S（k=99）现在就住在 (7,3)，并且 `idx` 里新增 `99 → (7,3)`。
3. t=2：查询 `WHERE k = 42` 走索引扫描，命中老项 `42 → (7,3)`，取回堆元组——**拿到的是 k=99 的行 S**。可见性检查救不了场：S 的 xmin 是刚提交的事务，对当前快照完全可见。查询返回了一条键值根本不匹配的行（对于 index-only scan 更直接：返回 42 但那行已经不存在）。
4. 更深的次生灾难：此时 `idx` 里有两个不同键（42、99）指向同一 TID。稍后 vacuum 想删 42 的老项时，B-tree 的去重/校验逻辑面对"同 TID 多键"的世界，任何基于 TID 的批量删除都可能误删 99 的合法项；唯一索引的存在性检查也会被幽灵项污染。

所以顺序必须是"索引先死透，堆才能转世"。反过来看正确顺序为什么**崩溃也安全**：阶段二完成后、阶段三之前宕机，留下的只是"索引没人指、堆上还是 LP_DEAD"的槽位——LP_DEAD 不可复用，无人受害，下次 vacuum 重新收集即可（幂等）。这也解释了两个工程结论：`dead_items` 内存不够时宁可多轮"阶段二+三"也绝不越序（每轮都是完整的 索引→堆）；表没有索引时三阶段坍缩为一阶段（`HEAP_PAGE_PRUNE_MARK_UNUSED_NOW`，lazy_scan_prune 里 2058-2059：直接把死元组置 LP_UNUSED，因为不存在会悬垂的索引项）。

**为什么要"攒一批再删"而不是逐行随删？** 这是与 InnoDB 的分岔点。InnoDB purge 拿着 undo 记录能重构索引键，可以精确定位并删除单条 delete-marked 索引项；PG 的索引 AM 接口没有"按 TID 找项"的能力，`ambulkdelete` 的契约就是**全索引扫描**+ 对每项回调问"这个 TID 死了吗"（`vac_tid_reaped`，vacuum.c:2710-2714，一行 `TidStoreIsMember`）。既然每轮都要付"扫全索引"的固定成本，批量越大摊销越好——`maintenance_work_mem` 因此直接决定索引被扫几遍。

### 2.3 阶段一主循环：lazy_scan_heap（vacuumlazy.c:1279-1621）

```c
    while (true)
    {
        ...
        vacuum_delay_point(false);

        if (vacrel->scanned_pages > 0 &&
            vacrel->scanned_pages % FAILSAFE_EVERY_PAGES == 0)
            lazy_check_wraparound_failsafe(vacrel);

        if (vacrel->dead_items_info->num_items > 0 &&
            TidStoreMemoryUsage(vacrel->dead_items) > vacrel->dead_items_info->max_bytes)
        {
            ...
            /* Perform a round of index and heap vacuuming */
            vacrel->consider_bypass_optimization = false;
            lazy_vacuum(vacrel);
            ...
        }

        buf = read_stream_next_buffer(stream, &per_buffer_data);

        /* The relation is exhausted. */
        if (!BufferIsValid(buf))
            break;
        ...
        got_cleanup_lock = ConditionalLockBufferForCleanup(buf);

        if (!got_cleanup_lock)
            LockBuffer(buf, BUFFER_LOCK_SHARE);

        /* Check for new or empty pages before lazy_scan_[no]prune call */
        if (lazy_scan_new_or_empty(vacrel, buf, blkno, page, !got_cleanup_lock,
                                   vmbuffer))
            continue;

        if (!got_cleanup_lock &&
            !lazy_scan_noprune(vacrel, buf, blkno, page, &has_lpdead_items))
        {
            ...
            Assert(vacrel->aggressive);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            LockBufferForCleanup(buf);
            got_cleanup_lock = true;
        }

        if (got_cleanup_lock)
            ndeleted = lazy_scan_prune(vacrel, buf, blkno, page,
                                       vmbuffer,
                                       &has_lpdead_items, &vm_page_frozen);
        ...
    }
```

（以上为 vacuumlazy.c:1321-1471 主干，省略进度上报、eager-scan 记账与 FSM 更新段。）要点：

- **限流点**（1332）：每页一次 `vacuum_delay_point`（§5.3）；
- **failsafe 巡检**（1343-1345）：每 `FAILSAFE_EVERY_PAGES`（4GB 对应的页数，vacuumlazy.c:193）复查一次表龄；
- **内存触顶 ⇒ 中途轮转**（1355-1386）：TidStore 超过 `max_bytes` 就地执行一轮 `lazy_vacuum`（阶段二+三），清空后继续扫。同时顺手 `FreeSpaceMapVacuumRange` 把新空闲空间发布到 FSM 上层节点；
- **读流**（1388）：PG 17 起用 read stream（预读）驱动，回调 `heap_vac_scan_next_block` 负责 VM 跳页（§2.4）；
- **cleanup 锁的退让**（1421-1453）：剪枝/碎片整理要求 cleanup 锁（排他 + 无其他 pin）。拿不到 ⇒ 降级 `lazy_scan_noprune`（vacuumlazy.c:2157）：只收集**已有**的 LP_DEAD（照样进 dead_items，2321-2333）、数活人死人、跟踪"如果不冻结这页 relfrozenxid 最多能到哪"（2172-2173、2300-2301）。唯一硬等的情形：aggressive vacuum 且页上有 XID < FreezeLimit 必须冻结（2222-2236 返回 false，主循环 1449-1452 转为 `LockBufferForCleanup` 阻塞等待）——vacuum 尽量不与前台查询死磕，但推进 relfrozenxid 的义务不可让；
- **循环后**（1603-1620）：最后一轮 `lazy_vacuum`（若有存货）→ 收尾 FSM → 每个索引的 `amvacuumcleanup`（`lazy_cleanup_all_indexes`）。

### 2.4 VM 跳页：heap_vac_scan_next_block 与 find_next_unskippable_block

回调 `heap_vac_scan_next_block`（vacuumlazy.c:1647）维护一个"下一个不可跳块"游标，三态机（1672-1731）；真正的决策在 `find_next_unskippable_block`（vacuumlazy.c:1747-1840），其 for 循环按优先级给出五条规则：

1. 非 all-visible ⇒ 不可跳（1781-1785）——可能有死元组/未冻结 XID；
2. 最后一页永远要扫（1797-1798）——为了让 `lazy_truncate_heap` 不必空拿 AccessExclusiveLock（注释 1787-1795）；
3. `DISABLE_PAGE_SKIPPING` ⇒ 全不跳（1801-1802）；
4. all-frozen ⇒ 任何 vacuum 都可跳（1808-1809）——不可能有需要处理的 XID；
5. all-visible 但未 frozen：aggressive ⇒ 不可跳（1815-1816，必须冻结推进 relfrozenxid）；普通 vacuum 在 eager-scan 预算内 ⇒ 也扫（1823-1827，PG 18）；否则跳过并置 `*skipsallvis = true`（1833）——这个标记最终导致 `skippedallvis`，放弃推进 relfrozenxid（§2.1 第 9 步）。

跳页还有一个反直觉的补丁（vacuumlazy.c:1684-1704）：可跳范围 < `SKIP_PAGES_THRESHOLD`（32 页，vacuumlazy.c:209）时**不跳**——顺序读时 OS 预读已经把页拉进来了，跳跃反而打断预读模式；而且顺路扫掉这些页意味着有机会保住 relfrozenxid 的推进资格（1694-1697 注释）。

### 2.5 lazy_scan_prune：把页交给剪枝引擎（vacuumlazy.c:2020-2135）

```c
    PruneFreezeParams params = {
        .relation = rel,
        .buffer = buf,
        .vmbuffer = vmbuffer,
        .reason = PRUNE_VACUUM_SCAN,
        .options = HEAP_PAGE_PRUNE_FREEZE | HEAP_PAGE_PRUNE_SET_VM,
        .vistest = vacrel->vistest,
        .cutoffs = &vacrel->cutoffs,
    };
    ...
    if (vacrel->nindexes == 0)
        params.options |= HEAP_PAGE_PRUNE_MARK_UNUSED_NOW;
    ...
    heap_page_prune_and_freeze(&params,
                               &presult,
                               &vacrel->offnum,
                               &vacrel->NewRelfrozenXid, &vacrel->NewRelminMxid);
```

自 PG 17（commit 6dbb490261 "Combine freezing and pruning steps in VACUUM"）起，vacuum 的剪枝、冻结、设 VM 三件事**共用一个引擎、一个临界区、一条 WAL 记录**（`heap_page_prune_and_freeze`，§3.2），与 HOT pruning 完全同源，仅 options 不同。返回后（2093-2107）：把页上残留 LP_DEAD 的偏移排序（TidStore 要求升序）交给 `dead_items_add`（vacuumlazy.c:3481）；累加各类计数；`presult.hastup` 决定该页是否阻止尾部截断。

### 2.6 dead_items 的存储：TidStore 与所以然 ⑤（为什么换 radix tree）

PG 16 及以前，死 TID 存放在一个**平面有序数组**（每 TID 6 字节 `ItemPointerData`）：

- **1GB 硬上限**：数组是单块 `palloc`，受分配上限约束——`maintenance_work_mem` 设 10GB 也只能用约 1GB，约 1.78 亿个 TID。一张 5 亿死行的大表注定多轮；
- **多轮的代价是索引**：每轮都要对**每个索引全量扫描**一次。10 个索引 × 3 轮 = 30 次全索引扫描，阶段二成为超大表 vacuum 的绝对瓶颈；
- **查找是 O(log n) 二分**：`ambulkdelete` 对索引里每一项都要问一次"你的 TID 死了吗"，几十亿次二分查找，缓存极不友好。

PG 17 引入 TidStore（commit 30e144287a 定义数据结构 + 667e65aac3 接入 vacuum，见 §7）。`src/backend/access/common/tidstore.c` 文件头（tidstore.c:6-8，逐字）：

```
 * TidStore is a in-memory data structure to store TIDs (ItemPointerData).
 * Internally it uses a radix tree as the storage for TIDs. The key is the
 * BlockNumber and the value is a bitmap of offsets, BlocktableEntry.
```

键 = 块号（基数树，`lib/radixtree.h`，按字节分层、前缀共享），值 = 该块内偏移位图（偏移很少时直接内嵌在指针字里，`NUM_FULL_OFFSETS`，tidstore.c:38）。三重收益：

1. **查找 O(键长)**：块号 4 字节 ⇒ 最多 4 层下探 + 一次位测试，替代 O(log n) 二分——阶段二最热路径（`vac_tid_reaped` → `TidStoreIsMember`，vacuum.c:2714）的每次调用都受益；
2. **内存密度**：同页多个死元组共享一个条目，位图 + 前缀共享通常比 6 字节/TID 省一个数量级（死元组密集时尤甚）；
3. **没有 1GB 上限**：radix tree 按需分配节点，`maintenance_work_mem` 给多少用多少 ⇒ 超大表一轮扫完的概率大增，索引扫描轮数（`num_index_scans`）下降。并行 vacuum 时用 DSA 上的共享版（`TidStoreCreateShared`/`TidStoreAttach`），worker 零拷贝共享。

阶段三的遍历接口也随之自然：`TidStoreBeginIterate` + `TidStoreIterateNext` 按块号升序吐出 (块号, 偏移集)（lazy_vacuum_heap_rel 的读流回调 `vacuum_reap_lp_read_stream_next`，vacuumlazy.c:2601-2620）。

### 2.7 阶段二：lazy_vacuum 与 index bypass（vacuumlazy.c:2368-2592）

`lazy_vacuum`（2368）先考虑**跳过整个索引阶段**（PG 14，commit 5100010ee4）：

```c
        threshold = (double) vacrel->rel_pages * BYPASS_THRESHOLD_PAGES;
        bypass = (vacrel->lpdead_item_pages < threshold &&
                  TidStoreMemoryUsage(vacrel->dead_items) < 32 * 1024 * 1024);
```

（vacuumlazy.c:2435-2437；`BYPASS_THRESHOLD_PAGES` = 0.02，vacuumlazy.c:187。）含 LP_DEAD 的页不足表的 2% 且 TID 集 < 32MB 时直接放弃本轮索引清理，LP_DEAD 原地留给下次。动机（2385-2401 注释）：一张 HOT 更新占 99% 的表，偶发几十个非 HOT 死元组，不值得为它们全量扫几百 GB 的索引；阈值用"页数占比"而不是"元组数"，因为真正的机会成本是**这些页的 VM 位设不上**。注意该优化只在单轮场景启用（内存触顶的中途轮转会置 `consider_bypass_optimization = false`，vacuumlazy.c:1371）。

不 bypass 则 `lazy_vacuum_all_indexes`（2493）：failsafe 预检（2515-2519，触发则整轮不做）→ 串行逐个索引 `lazy_vacuum_one_index` → `vac_bulkdel_one_index` → `index_bulk_delete(ivinfo, istat, vac_tid_reaped, ...)`（vacuum.c:2667），B-tree 落到 `btbulkdelete`：全量扫索引，命中 TidStore 的项被删；每删完一个索引复查一次 failsafe（2544-2549，触发则中断剩余索引）。并行 vacuum 时整段外包给 parallel worker（2552-2565），并行度按索引数分配（一个索引最多一个 worker）。

### 2.8 阶段三：lazy_vacuum_heap_rel / lazy_vacuum_heap_page（vacuumlazy.c:2639-2875）

按 TidStore 迭代块号回访堆页，**只需普通排他锁**（2711-2712 注释：非 cleanup 锁即可——因为只动行指针状态不搬元组）。每页核心（vacuumlazy.c:2809-2862）：

```c
    START_CRIT_SECTION();

    for (int i = 0; i < num_offsets; i++)
    {
        ItemId      itemid;
        OffsetNumber toff = deadoffsets[i];

        itemid = PageGetItemId(page, toff);

        Assert(ItemIdIsDead(itemid) && !ItemIdHasStorage(itemid));
        ItemIdSetUnused(itemid);
        unused[nunused++] = toff;
    }

    Assert(nunused > 0);

    /* Attempt to truncate line pointer array now */
    PageTruncateLinePointerArray(page);

    if ((vmflags & VISIBILITYMAP_VALID_BITS) != 0)
    {
        ...
        PageSetAllVisible(page);
        PageClearPrunable(page);
        visibilitymap_set(blkno,
                          vmbuffer, vmflags,
                          vacrel->rel->rd_locator);
        conflict_xid = newest_live_xid;
    }
    ...
    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (RelationNeedsWAL(vacrel->rel))
    {
        log_heap_prune_and_freeze(vacrel->rel, buffer,
                                  ...
                                  unused, nunused);
    }

    END_CRIT_SECTION();
```

master 上的新优化（2780-2807 注释）：进临界区前先用 `heap_page_would_be_all_visible` 预判"清完这些 LP_DEAD 后页是否 all-visible"，是则把**释放槽位与设置 VM 合并在同一临界区、同一条 WAL** 里——省一条 WAL、少一次页锁往返。`PageTruncateLinePointerArray` 把页尾连续的 LP_UNUSED 槽位整体砍掉（行指针数组本身缩短）。

收尾语义再强调一次：`lazy_truncate_heap` 只归还**尾部**连续空页给 OS；表中部的空闲空间只进 FSM 复用。这就是"VACUUM 不缩表"的准确含义；要整体缩表需重写全表（`VACUUM FULL`，`commands/cluster.c`）。

### 小结

lazy vacuum = ①扫堆（VM 跳页 + 剪枝/冻结共用引擎 + 死 TID 进 TidStore）→ ②每索引 ambulkdelete（TidStoreIsMember 判死）→ ③回堆 LP_DEAD→LP_UNUSED 并顺手设 VM。顺序不可交换（否则 TID 复用后索引指向无关新行）；崩溃安全靠"每一步幂等 + 只在整轮索引完成后才动堆"；内存不足多轮、死 TID 太少 bypass、表龄危险 failsafe，三个开关都作用在阶段二上——因为索引全量扫描是最贵的一段。

---

## 3. HOT pruning 与页内清理引擎

### 3.1 heap_page_prune_opt：读路径上的四道闸（pruneheap.c:271-410）

等 autovacuum 来太慢——高频 UPDATE 的热页几秒内就能堆满死版本。PG 的答案：**每个读到该页的查询都可以顺手做单页清理**。入口 `heap_page_prune_opt`（pruneheap.c:271），由 heapam 的页访问路径调用，四道闸层层过滤保证读路径开销趋近于零：

```c
    prune_xid = PageGetPruneXid(page);
    if (!TransactionIdIsValid(prune_xid))
        return;
    ...
    vistest = GlobalVisTestFor(relation);

    if (!GlobalVisTestIsRemovableXid(vistest, prune_xid, true))
        return;
    ...
    minfree = RelationGetTargetPageFreeSpace(relation,
                                             HEAP_DEFAULT_FILLFACTOR);
    minfree = Max(minfree, BLCKSZ / 10);

    if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
    {
        ...
        if (!ConditionalLockBufferForCleanup(buffer))
            return;
```

（pruneheap.c:294-330。）闸门 ①：页头 `pd_prune_xid` 是上次 DELETE/UPDATE 留下的"本页最早可剪时刻"提示，无效即从未删改过，白板页直接走人；闸门 ②：用 GlobalVisState 两次比较判断这个时刻是否已过视界（§1.5）；闸门 ③：空间启发式——页快满（`PageIsFull` 或空闲 < max(fillfactor 目标, 页的 10%)）才值得动手，否则把机会留给未来（注释 312-317 坦承这里不拿锁读 pd_lower/pd_upper 可能读到瞬时错值，但"启发式估计错一次无所谓，不拿锁更重要"）；闸门 ④：cleanup 锁只试不等。拿到锁后**重查一遍**空间条件（337，锁内的准确值）再进引擎。还有一个第 0 道闸：恢复模式直接返回（285-286，standby 不能写 WAL，主库的清理记录很快会重放过来）。

HOT pruning 解决**页内空间复用**，不碰索引——死元组最多缩成 LP_DEAD 存根 + LP_REDIRECT 跳板，索引项的删除仍归 VACUUM。on-access 路径也不敢用 `MARK_UNUSED_NOW`（354-358 注释：无法在该路径上安全判定表没有索引）。

### 3.2 heap_page_prune_and_freeze 主线走读（pruneheap.c:1111-1416）

统一引擎，VACUUM 与 on-access 共用。主线八步：

1. **setup**（1128-1130，`prune_freeze_setup`）：把 params 摊平进 `PruneState`，读取旧 VM 位；
2. **VM 矛盾修复**（1137-1140）：VM 说 all-visible 但页头 `PD_ALL_VISIBLE` 没设 ⇒ 先修复这类损坏再干活；
3. **fast path**（1149-1156）：页已 all-frozen（或 all-visible 且本次不冻结）⇒ `prune_freeze_fast_path`（1028-1071）只数一遍活元组就返回——这是 `SKIP_PAGES_THRESHOLD` 强制扫进来的"顺路页"的廉价出口；
4. **计划**（1164，`prune_freeze_plan`，§3.3）：只读分析，把每个槽位的命运写进 prstate 的 `redirected[]/nowdead[]/nowunused[]/frozen[]` 数组——**先算后改**是整个引擎的骨架：所有可能失败的逻辑（可见性判断、CLOG 查询、multixact 展开）都发生在临界区外；
5. **决策**（1172-1227）：活人里最新的 xmin 若可能仍被某快照视为在跑 ⇒ 撤销 all-visible 资格（1172-1177）；`do_prune`/`do_hint_prune` 判定（1186-1196）；`heap_page_will_freeze`（1202，pruneheap.c:753）决定是否执行冻结计划——"必须冻"（页上有 XID < FreezeLimit，`freeze_required`）或"顺手冻"（页将变 all-frozen 且代价已付：反正要写 WAL/已产生 FPI）；LP_DEAD 存在 ⇒ 页不能设 all-visible（1220-1221，但注意 1207-1214 注释：LP_DEAD 被视为"准 LP_UNUSED"，不影响**冻结**决策，只影响本次设 VM）；
6. **冲突视界**（1239-1245）：`conflict_xid` 取"设 VM 的 newest_live_xid、冻结的 FreezePageConflictXid、剪枝的 latest_xid_removed"三者最新——standby 重放这条 WAL 时要取消比它老的查询（recovery conflict 的源头）；
7. **执行**（1252-1336）：临界区内按序：写 `pd_prune_xid` hint → `heap_page_prune_execute`（2087，把三个数组真正落到行指针上，含 `ItemIdSetRedirect`，2156）→ `heap_freeze_prepared_tuples`（heapam.c:7600）→ `PageSetAllVisible` + `visibilitymap_set` → `MarkBufferDirty` → 一条 `XLOG_HEAP2_PRUNE*` 记录（`log_heap_prune_and_freeze`，pruneheap.c:2583）覆盖剪枝+冻结+VM 三件事；
8. **回填**（1377-1415）：ndeleted/nnewlpdead/nfrozen/live_tuples/deadoffsets 等交还调用者；若冻了 ⇒ 用 `FreezePageRelfrozenXid` 更新调用者的 NewRelfrozenXid，没冻 ⇒ 用 `NoFreezePageRelfrozenXid`（"这页留着不冻的话 relfrozenxid 最多能推到哪"）。

### 3.3 prune_freeze_plan：两趟扫描（pruneheap.c:551-738）

第一趟（581-640）**逆序**遍历行指针：LP_UNUSED ⇒ 记"不变"；LP_DEAD ⇒ `mark_unused_now` 时直接转 LP_UNUSED，否则记"不变的 dead"（进 deadoffsets）；LP_REDIRECT ⇒ 入 `root_items[]`；LP_NORMAL ⇒ 调 `heap_prune_satisfies_vacuum` 算 HTSV 存进 `htsv[]`，非 heap-only 的入 `root_items[]`，heap-only 的入 `heaponly_items[]`。为什么逆序？562-580 的注释给了两个理由：**正确性**——HTSV 必须每元组只算一次（RECENTLY_DEAD 可能因为视界中途收紧而变 DEAD、INSERT_IN_PROGRESS 可能因插入者中止变 DEAD，算两次会前后矛盾）；**性能**——元组数据在页内从高地址向低地址排布，逆序遍历行指针 = 顺序访问元组内存，CPU 预取友好。

第二趟（653-666）对每个根调 `heap_prune_chain`（§3.4）。扫尾（672-722）处理没被任何链覆盖的 heap-only 元组：DEAD 且非 HotUpdated ⇒ 直接 LP_UNUSED——这是**中止的 HOT 更新**留下的孤儿（父版本可能又被更新过，链根本不经过它，688-693 注释）；DEAD 却 HotUpdated ⇒ `elog(ERROR, "dead heap-only tuple ... is not linked to from any HOT chain")`（716-717）——宁可报错也不能留下带存储的 DEAD 元组（不变式 §6.6）。

### 3.4 heap_prune_chain 全函数走读：版本链清理的指针舞蹈（pruneheap.c:1505-1704）

函数头注释（1478-1490）先立规矩：从链头开始的连续 DEAD 前缀（夹在 DEAD 前面的 RECENTLY_DEAD 一并算——"a RECENTLY_DEAD tuple preceding a DEAD tuple is really DEAD, our visibility test is just too coarse to detect it"）要剪掉；根指针重定向到最后一个 DEAD 之后的第一个幸存者；全链皆死则根置 LP_DEAD。

**第一段：沿链收集（1508-1644）。** 状态机变量：`chainitems[]` 按顺序记链上的槽位，`nchain` 计数，`ndeadchain` 记"最后一个 DEAD 的位置 + 1"，`priorXmax` 记上一跳的 update XID。循环体的每个 break/goto 都是一种链终止条件：

```c
    for (;;)
    {
        ...
        if (offnum < FirstOffsetNumber)
            break;
        if (offnum > maxoff)
            break;
        if (prstate->processed[offnum])
            break;

        lp = PageGetItemId(page, offnum);
        ...
        if (ItemIdIsRedirected(lp))
        {
            if (nchain > 0)
                break;          /* not at start of chain */
            chainitems[nchain++] = offnum;
            offnum = ItemIdGetRedirect(rootlp);
            continue;
        }
        ...
        if (TransactionIdIsValid(priorXmax) &&
            !TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
            break;

        chainitems[nchain++] = offnum;

        switch (htsv_get_valid_status(prstate->htsv[offnum]))
        {
            case HEAPTUPLE_DEAD:
                ndeadchain = nchain;
                HeapTupleHeaderAdvanceConflictHorizon(htup,
                                                      &prstate->latest_xid_removed);
                break;

            case HEAPTUPLE_RECENTLY_DEAD:
                ...
                break;

            case HEAPTUPLE_DELETE_IN_PROGRESS:
            case HEAPTUPLE_LIVE:
            case HEAPTUPLE_INSERT_IN_PROGRESS:
                goto process_chain;
            ...
        }

        if (!HeapTupleHeaderIsHotUpdated(htup))
            goto process_chain;
        ...
        offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
        priorXmax = HeapTupleHeaderGetUpdateXid(htup);
    }
```

细节拆解：

- **`xmin == priorXmax` 校验**（1578-1580）：链的每一跳必须满足"下一版本的创建者 = 上一版本的更新者"，否则说明 ctid 指向的槽位已被无关行复用（旧版本被剪、槽位转世），链到此为止。这是 ctid 链在"槽位可复用"世界里保持自洽的唯一防线；
- **DEAD ⇒ `ndeadchain = nchain` 并继续走**（1589-1596）：不是遇到 DEAD 就停，而是记住"至此全可剪"，继续看后面还有没有更多 DEAD。同时把被删元组的 xmax 汇入 `latest_xid_removed`（standby 冲突视界）；
- **RECENTLY_DEAD ⇒ 继续走但不推进 ndeadchain**（1598-1616）：它单独不可剪，但若后面又出现 DEAD，它作为前缀被一起剪（此时无需推进冲突视界，1600-1607 的注释论证：后面那个 DEAD 的插入者更新，视界已被它覆盖）。1610-1615 强调必须走过 RECENTLY_DEAD 去找后面的 DEAD——**剪枝绝不能留下"仍有存储的 DEAD 元组"**，那会让 VACUUM 崩溃；
- **LIVE / IN_PROGRESS ⇒ 停止收集**（1618-1621）：链的存活区开始了；
- **非 HotUpdated ⇒ 链尾**（1632-1633）；否则沿 `t_ctid` 跳下一节（HOT 链保证同页，1641 Assert）。

循环出口后还有一个边角修复（1646-1658）：redirect 根指向的目标已经不成链（目标先被当作孤儿处理掉了）⇒ 根直接置 LP_DEAD/LP_UNUSED。

**第二段：按 ndeadchain 三分归宿（1660-1703）**

```c
process_chain:

    if (ndeadchain == 0)
    {
        /* 链上无死者：全部记"不变"（顺便做冻结评估） */
        ...
    }
    else if (ndeadchain == nchain)
    {
        /*
         * The entire chain is dead.  Mark the root line pointer LP_DEAD, and
         * fully remove the other tuples in the chain.
         */
        heap_prune_record_dead_or_unused(prstate, rootoffnum, ItemIdIsNormal(rootlp));
        for (int i = 1; i < nchain; i++)
            heap_prune_record_unused(prstate, chainitems[i], true);
    }
    else
    {
        /*
         * We found a DEAD tuple in the chain.  Redirect the root line pointer
         * to the first non-DEAD tuple, and mark as unused each intermediate
         * item that we are able to remove from the chain.
         */
        heap_prune_record_redirect(prstate, rootoffnum, chainitems[ndeadchain],
                                   ItemIdIsNormal(rootlp));
        for (int i = 1; i < ndeadchain; i++)
            heap_prune_record_unused(prstate, chainitems[i], true);

        /* the rest of tuples in the chain are normal, unchanged tuples */
        for (int i = ndeadchain; i < nchain; i++)
            heap_prune_record_unchanged_lp_normal(prstate, chainitems[i]);
    }
```

配图（部分死亡的典型链，UPDATE 三次后 A、B 已死，C 存活）：

```
剪枝前:
  索引项(k) ──→ [槽1: A(死)] --ctid--> [槽2: B(死)] --ctid--> [槽3: C(活)]
  chainitems = {1, 2, 3},  nchain = 3,  ndeadchain = 2

剪枝后:
  索引项(k) ──→ [槽1: LP_REDIRECT → 3]   [槽2: LP_UNUSED]   [槽3: C(活)]
                 (root 变跳板)             (立即可复用)        (数据被碎片整理挪紧)

若 C 也死 (ndeadchain == nchain == 3):
  索引项(k) ──→ [槽1: LP_DEAD]   [槽2: LP_UNUSED]   [槽3: LP_UNUSED]
                 (存根等 VACUUM 删索引)
```

三个 record 函数的分工（决策/记账分离，1498-1502 注释）：`heap_prune_record_redirect`（1730）把 (from, to) 成对压入 `redirected[]`；`heap_prune_record_dead`（1761）压 `nowdead[]` 并把偏移记进 `deadoffsets[]`（就是将来进 TidStore 的那批）；`heap_prune_record_unused`（1822）压 `nowunused[]`。真正落页在临界区里的 `heap_page_prune_execute`（2087）。注意**中间节点（槽 2）直接 LP_UNUSED 而根只能 LP_DEAD**——因为中间节点是 heap-only 元组，全世界没有索引项指向它，槽位可当场转世；根被索引指着，必须留存根走三阶段流程。

### 3.5 LP_REDIRECT 的所以然（②）：让索引不知道 HOT 存在

HOT（2007，commit 282d2a03dd）的契约：更新未改任何索引列且新版本落在同页时，**不插新索引项**——索引里那条 `k → (blk, root_off)` 永远指向链头槽位。现在推演"没有 LP_REDIRECT 会怎样"，穷举剪掉链头死版本 A 之后根槽位的所有出路：

1. **置 LP_UNUSED**：索引项悬垂 → 槽位被新行复用 → §2.2 的幽灵读灾难；
2. **置 LP_DEAD**：不悬垂了，但**链断了**——索引扫描到 (blk, root_off) 见 LP_DEAD 只会当"行已删"，存活的 C 从索引视角凭空消失。丢行，比幽灵读更糟；
3. **保留 A 不剪**：链头永不能剪 ⇒ 高频更新的行每页永远压着一个死版本，还得为它跳一次链，HOT 的空间收益残废；
4. **更新索引项让它直指 C**：这等于放弃"不碰索引"的 HOT 初衷——找到并改写索引项的成本就是普通 UPDATE 的成本，而且改索引需要拿索引页锁、写索引 WAL，剪枝就不再是页内局部操作。

LP_REDIRECT 是第五条路：根槽位放弃存储元组，`lp_off` 字段改存目标槽号（`ItemIdSetRedirect`，pruneheap.c:2156），一个 4 字节行指针当跳板。由此获得的完整闭环：

- 索引项终生不变（TID 稳定）；读路径遇 LP_REDIRECT 多跳一步，成本一次页内数组寻址；
- 后续再剪时 redirect 目标**跟着链头前移**（旧目标变 LP_UNUSED，redirect 改指新链头）——链再长也只有一个跳板（pruneheap.c:2135-2137 Assert 注释："There can be at most one LP_REDIRECT item per HOT chain"）；
- 整链死光时 LP_REDIRECT 退化为 LP_DEAD（§3.4 第二分支对 redirect 根同样适用），此后与普通死行一样走三阶段——pruneheap.c:2139-2144 的注释点明存在意义："We need to keep around an LP_REDIRECT item ... so that it's always possible for VACUUM to easily figure out what TID to delete from indexes when an entire HOT chain becomes dead."
- 索引 AM 的代码里**没有一行**与 HOT 相关的逻辑——"索引不知道 HOT 存在"，这层无知正是 HOT 能以纯堆内机制落地的原因。代价是 `fillfactor < 100` 才能给同页新版本留空间，以及 index-only scan 必须经 VM 确认（redirect 意味着索引指的槽位上没有元组数据）。

### 小结

HOT pruning 把版本链清理摊到每次页访问：pd_prune_xid + GlobalVisState + 空间启发式 + 条件锁四道闸控成本。清理引擎"先算后改"：两趟计划（逆序 HTSV + 逐链决策）+ 一个临界区 + 一条 WAL。`heap_prune_chain` 用 `xmin==priorXmax` 自证链的连续性，按"死亡前缀长度"三分归宿：全活不动、全死根变 LP_DEAD、半死根变 LP_REDIRECT。LP_REDIRECT 用页内一级间接换来"索引项终生不变"，是 PG 对抗 UPDATE 写放大最重要的机制。

---

## 4. freeze 与回卷保护

### 4.1 wraparound 的数学：为什么逼近 2^31 就必须停写（所以然 ③b）

XID 是 uint32，用完从 3 绕回（0/1/2 是保留值：Invalid/Bootstrap/FrozenTransactionId）。环上没有绝对大小，只有相对新旧：`TransactionIdPrecedes(a, b)` 的实现是 `(int32)(a - b) < 0`——**a 落在 b 逆时针半圈（2^31 = 约 21.4 亿）以内算"更老"**。这个定义能工作的前提是：**任何时刻，数据库里同时存在的未冻结 XID 的跨度 < 半圈**。一旦某个元组的 xmin 落后当前 XID 超过 2^31，减法翻转符号，它会被所有比较判成"在未来"——插入它的事务"还没发生"，于是整行对所有人瞬间不可见。不是报错，是**静默消失**，这比崩溃恶劣得多。

因此老 XID 必须**冻结**：置 `HEAP_XMIN_FROZEN`（htup_details.h:206，committed|invalid 两位同置；9.4 前是改写 xmin=2，现在保留原始 xmin 供取证），宣布"此行对所有人永久可见，退出比较体系"。`pg_class.relfrozenxid` 承诺"本表没有比它更老的未冻结 XID"；`pg_database.datfrozenxid = min(全库 relfrozenxid)`；CLOG 只保留 datfrozenxid 之后的段（更早的提交状态不再需要——都冻结了）。

安全边界的代码在 `GetNewTransactionId` 的检查里（`src/backend/access/transam/varsup.c`）：

- `xidWrapLimit = oldest_datfrozenxid + 2^31`（varsup.c:384，`(MaxTransactionId >> 1)`）：数学上的悬崖；
- `xidStopLimit = xidWrapLimit - 3000000`（varsup.c:400）：留 300 万 XID 余量的**停写线**——越线后拒绝分配新 XID：`ERROR: database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database "%s"`（varsup.c:147）。只读查询不受影响（不分配 XID）；解法是完成表龄最老的那些表的 vacuum（现代版本 single-user mode 已非必须）；
- `xidWarnLimit`：停写线前 4000 万开始每次分配都告警（varsup.c:159-179）。

**为什么必须留 300 万余量而不是用到最后一个？** 停写之后还需要 XID 去执行拯救性的 VACUUM 相关操作与可能的人工处置；且多个 backend 并发分配，检查与分配之间有窗口。300 万是"够管理员救场"的工程冗余。

### 4.2 heap_prepare_freeze_tuple：冻结的资格线与强制线（heapam.c:7267）

VACUUM 的剪枝阶段对每个存活元组调用它，生成"冻结计划"（先算后改：计划在临界区外，执行由 `heap_freeze_prepared_tuples`，heapam.c:7600，统一在临界区内落页记 WAL）。xmin 处理段（heapam.c:7291-7314，逐字节选）：

```c
    xid = HeapTupleHeaderGetXmin(tuple);
    if (!TransactionIdIsNormal(xid))
        xmin_already_frozen = true;
    else
    {
        if (TransactionIdPrecedes(xid, cutoffs->relfrozenxid))
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg_internal("found xmin %u from before relfrozenxid %u",
                                     xid, cutoffs->relfrozenxid)));

        /* Will set freeze_xmin flags in freeze plan below */
        freeze_xmin = TransactionIdPrecedes(xid, cutoffs->OldestXmin);
```

注意两条线的分工（PG 16 起的页级冻结模型）：

- **资格线 = OldestXmin**：xmin < OldestXmin 的元组**可以**冻（对所有人已可见，冻了不影响任何快照）；
- **强制线 = FreezeLimit**：页上存在 XID < FreezeLimit 时置 `pagefrz.freeze_required`（判定在 `heap_tuple_should_freeze`，heapam.c:8086），该页**必须**执行冻结计划；
- **页级决策**：没到强制线时由 `heap_page_will_freeze`（pruneheap.c:753）做机会主义选择——若整页冻完能设 all-frozen 位、且本次反正要写 WAL/FPI，就顺手冻透（"代价已付"原则）。冻结因此是**全页原子**的：要么按计划全冻要么全不冻，避免半冻页。

开头那个 `ERRCODE_DATA_CORRUPTED` 检查是不变式 §6.3 的运行时哨兵：出现比 relfrozenxid 还老的 xmin，说明上一轮的承诺已被破坏（磁盘损坏/bug），继续冻结只会掩埋证据，立即报错。

### 4.3 freeze 为什么不能太早、也不能太晚（所以然 ③a）

`vacuum_freeze_min_age`（默认 5000 万，guc_parameters.dat:3388-3391）决定 FreezeLimit = nextXID - min_age。把它推向两个极端：

**极端 A：min_age = 0（见谁冻谁）。** 每次 vacuum 把所有可见元组全部冻结：

1. **WAL 放大**：冻结改元组头 ⇒ 页脏 ⇒ 冻结 WAL；若该页在本 checkpoint 周期内首次被改，还要带整页镜像（FPW，约 8KB）。一张刚 COPY 完 100GB 的表，立即全量冻结 ≈ 再写一遍 100GB 级别的 WAL——而这些行如果一周内会被更新/删除，冻结完全白做（xmax 一设，冻结的 xmin 就没意义了）；
2. **写放大转嫁给复制与备份**：这些 WAL 要走流复制、归档、PITR 全链路；
3. 唯一的好处（relfrozenxid 激进推进）对健康库并无收益——回卷时钟本来就远。

所以 min_age 的意义是"给元组一个自证短命的机会"：活过 5000 万个事务还没被改，大概率是冷数据，冻结不亏。

**极端 B：min_age 极大 / vacuum 长期只做例行清理（永不冻）。** 未冻结 XID 跨度逼近安全线：

1. `age(relfrozenxid)` 越过 `vacuum_freeze_table_age`（1.5 亿）⇒ 下次 vacuum 升格 aggressive：不许跳 all-visible 页——多年没碰过的冷页全部要重读一遍并冻结。**冻结债不会消失，只会集中到期**；
2. 越过 `autovacuum_freeze_max_age`（2 亿）⇒ 强制 anti-wraparound autovacuum，无视一切开关（§5.2）；
3. 若它也跑不完（被长事务钉住 / 被 cost delay 拖住 / 单表太大），越过 `vacuum_failsafe_age`（16 亿）⇒ failsafe：跳过索引清理和截断、卸掉限速全速冻结（§4.5）；
4. 最终 `xidStopLimit`：停写事故。

两个极端之间，参数体系给出的折中就是三条年龄线：min_age 管"冻多老的"（别太早），table_age 管"何时必须全面巡逻"（别太晚），max_age 管"没人巡逻时的强制出警"。

### 4.4 vacuum_get_cutoffs：切点计算与 aggressive 判定（vacuum.c:1105-1264）

全函数六步：

1. 读表的 relfrozenxid/relminmxid（1128-1129）；
2. `OldestXmin = GetOldestNonRemovableTransactionId(rel)`（1142，§1.4）、OldestMxact（1147）；
3. **视界滞后预警**（1165-1180）：OldestXmin 落后 nextXID 超过 autovacuum_freeze_max_age ⇒ `WARNING: cutoff for removing and freezing tuples is far in the past`，hint 直接点名三大嫌疑人：长事务、prepared 事务、复制槽；
4. **FreezeLimit**（1188-1199）：`freeze_min_age` 封顶为 max_age 的一半（防 anti-wraparound 过于频繁），`FreezeLimit = nextXID - freeze_min_age`，且强制 ≤ OldestXmin（不能冻还有人要看的 XID）；
5. MultiXact 对偶的一套（1207-1219），另有 `MultiXactMemberFreezeThreshold`（1159）：member 空间膨胀时动态调低 mxid 的有效 max_age；
6. **aggressive 判定**（1230-1239，逐字）：

```c
    if (freeze_table_age < 0)
        freeze_table_age = vacuum_freeze_table_age;
    freeze_table_age = Min(freeze_table_age, autovacuum_freeze_max_age * 0.95);
    Assert(freeze_table_age >= 0);
    aggressiveXIDCutoff = nextXID - freeze_table_age;
    if (!TransactionIdIsNormal(aggressiveXIDCutoff))
        aggressiveXIDCutoff = FirstNormalTransactionId;
    if (TransactionIdPrecedesOrEquals(cutoffs->relfrozenxid,
                                      aggressiveXIDCutoff))
        return true;
```

运维语言：`age(relfrozenxid) > vacuum_freeze_table_age`（默认 1.5 亿）时，**任何**落到该表的 vacuum 自动升格 aggressive。95% 封顶（1232）的用意在 1224-1228 的注释里写明：如果你有夜间例行 VACUUM，让它先于 anti-wraparound autovacuum 得到 aggressive 机会——温和的计划内冻结，替代侵入性的强制出警。

### 4.5 多级防线总表

| 层级 | 触发条件 | 行为 | 源码 |
|---|---|---|---|
| 例行冻结 | 每次 vacuum | 冻 XID < FreezeLimit 的页（强制）+ 机会主义整页冻结 | heapam.c:7267、pruneheap.c:753 |
| eager scan（PG 18） | 普通 vacuum，按区域抽样 | 顺路冻 all-visible 页，摊平 aggressive 的债 | vacuumlazy.c:250、1823-1827；commit 052026c9b9 |
| aggressive | `age(relfrozenxid) > vacuum_freeze_table_age`（1.5 亿） | 不跳 all-visible 页，保证推进 relfrozenxid ≥ FreezeLimit | vacuum.c:1230-1239 |
| anti-wraparound autovacuum | `age > autovacuum_freeze_max_age`（2 亿） | 强制启动，无视表级/全局 autovacuum 开关 | autovacuum.c:3184-3198 |
| failsafe | `age > max(vacuum_failsafe_age, max_age×1.05)`（默认 16 亿） | 跳过索引清理/堆二阶段/截断，卸掉 cost delay 全速冻结 | vacuumlazy.c:2889-2938、vacuum.c:1273-1289；commit 1e55e7d175 |
| 告警线 | 距停写 4000 万 | 每次分配 XID 打 WARNING | varsup.c:159 |
| 停写线 | `xidStopLimit = wrapLimit - 300 万` | 拒绝分配新 XID，只读自保 | varsup.c:139-147、400 |

failsafe 的动作值得看一眼（vacuumlazy.c:2905-2932）：`VacuumFailsafeActive = true` → 放弃 ring buffer 策略（可用全部 shared buffers）→ 关闭 `do_index_vacuuming/do_index_cleanup/do_rel_truncate` → `WARNING: bypassing nonessential maintenance` → `VacuumCostActive = false`。逻辑：此刻唯一致命的是回卷，索引清理"只是"膨胀问题，可以欠着。触发点有三处：开工前（heap_vacuum_rel:863）、每 4GB 扫描（lazy_scan_heap:1343-1345）、每个索引之间（lazy_vacuum_all_indexes:2515/2544）。

数据库级联动：CLOG/`pg_xact` 只能截断到全库 datfrozenxid；**一张卡住的表拖住全库的 CLOG 回收与回卷时钟**。监控视角：`SELECT max(age(relfrozenxid)) FROM pg_class WHERE relkind IN ('r','m','t')` 是必备告警项。

### 小结

回卷是模 2^31 比较规则的必然推论：未冻结 XID 跨度必须 < 半圈。冻结的资格线是 OldestXmin、强制线是 FreezeLimit、执行以页为单位。三条年龄线（min_age / table_age / max_age）分别防"冻太早浪费 WAL"与"冻太晚集中爆债"；failsafe 与 xidStopLimit 是最后两道保险丝，代价递增：先牺牲膨胀治理，再牺牲写入可用性。

---

## 5. autovacuum：让账单自动支付

### 5.1 launcher / worker 两级架构

`src/backend/postmaster/autovacuum.c` 文件头注释即架构说明。分工：

- **launcher**（常驻，`AutoVacLauncherMain`，autovacuum.c:411）：纯调度器。按 `launcher_determine_sleep`（autovacuum.c:847）安排节奏：数据库列表按"上次视察时间"排序，间隔 = `autovacuum_naptime`（默认 60s，声明 autovacuum.c:127）/ 库数——目标是**每个库每 naptime 被视察一次**。到点后挑最久未视察的库，向 postmaster 发信号请求 fork worker（launcher 不能自己 fork——worker 必须是 postmaster 直系子进程才能挂进共享内存体系）；
- **worker**（临时，`AutoVacWorkerMain`，autovacuum.c:1418）：连上目标库执行 `do_autovacuum`（autovacuum.c:1928）：扫全表 `pg_class`，对每个表（含 TOAST）调 `relation_needs_vacanalyze` 判断 + 打分，把需要处理的表**按分数降序**排成清单逐个执行，干完退出。并发上限 `autovacuum_max_workers`（默认 3）+ 槽位 `autovacuum_worker_slots`；同一张表同一时间只会有一个 worker（重查机制防重复）。

master（v19+）的新演进：`relation_needs_vacanalyze` 输出 `AutoVacuumScores`（autovacuum.c:329-341：max/xid/mxid/vac/vac_ins/anl 六个分量）——最接近回卷、最超阈值的表优先处理，替代旧的物理顺序遍历。

### 5.2 relation_needs_vacanalyze 全函数走读（autovacuum.c:3073-3341）

函数头注释（3002-3007）给出总公式：

```
 * threshold = vac_base_thresh + vac_scale_factor * reltuples
 * if (threshold > vac_max_thresh)
 *     threshold = vac_max_thresh;
```

**① 参数三级取值（3140-3179）**：每个参数都是"表级 reloption 优先，否则 GUC"：

```c
    vac_scale_factor = (relopts && relopts->vacuum_scale_factor >= 0)
        ? relopts->vacuum_scale_factor
        : autovacuum_vac_scale;

    vac_base_thresh = (relopts && relopts->vacuum_threshold >= 0)
        ? relopts->vacuum_threshold
        : autovacuum_vac_thresh;
```

默认值（guc_parameters.dat 核实）：scale_factor 0.2 / threshold 50 / **max_threshold 1 亿（v19 起默认启用）** / ins_scale 0.2 / ins_threshold 1000 / anl_scale 0.1 / anl_threshold 50 / freeze_max_age 2 亿。`av_enabled = (relopts ? relopts->enabled : true) && AutoVacuumingActive()`（3178-3179）。

**② 回卷强制（3184-3198）**——凌驾于一切开关：

```c
    /* Force vacuum if table is at risk of wraparound */
    xidForceLimit = recentXid - freeze_max_age;
    if (xidForceLimit < FirstNormalTransactionId)
        xidForceLimit -= FirstNormalTransactionId;
    force_vacuum = (TransactionIdIsNormal(relfrozenxid) &&
                    TransactionIdPrecedes(relfrozenxid, xidForceLimit));
    if (!force_vacuum)
    {
        multiForceLimit = recentMulti - multixact_freeze_max_age;
        ...
    }
    *wraparound = force_vacuum;
```

`age(relfrozenxid) > autovacuum_freeze_max_age` ⇒ 必须 vacuum，**即使表级 autovacuum=off、即使全局 autovacuum=off**（3250-3254 注释：全局关闭时只处理被强制的表）。这就是日志里 `(to prevent wraparound)` 的来源。

**③ 打分（3200-3247）**：`scores->xid = xid_age / freeze_max_age`；表龄越过有效 failsafe 线（≥ max_age×1.05）后分数**指数放大**（3237-3240：`pow(score, xid_age/1e8)`），确保濒危表插队到清单最前。五个 `*_score_weight` GUC 可调权重，全设 0 退回 v19 前行为。

**④ 无统计即退出（3256-3259）**：`pgstat_fetch_stat_tabentry_ext` 拿不到 ⇒ return（从未被写过的表不必处理；force_vacuum 已在上面置好 dovacuum）。

**⑤ 三公式判定（3261-3322）**：

```c
    vactuples = tabentry->dead_tuples;
    instuples = tabentry->ins_since_vacuum;
    anltuples = tabentry->mod_since_analyze;
    ...
    if (relpages > 0 && relallfrozen > 0)
    {
        ...
        relallfrozen = Min(relallfrozen, relpages);
        pcnt_unfrozen = 1 - ((float4) relallfrozen / relpages);
    }

    vacthresh = (float4) vac_base_thresh + vac_scale_factor * reltuples;
    if (vac_max_thresh >= 0 && vacthresh > (float4) vac_max_thresh)
        vacthresh = (float4) vac_max_thresh;

    vacinsthresh = (float4) vac_ins_base_thresh +
        vac_ins_scale_factor * reltuples * pcnt_unfrozen;
    anlthresh = (float4) anl_base_thresh + anl_scale_factor * reltuples;
```

- **死元组公式**：`n_dead_tup > 50 + 0.2 × reltuples` ⇒ vacuum。管 UPDATE/DELETE 负载。20% 对大表极钝：5 亿行要攒 1 亿死行——这正是 `vacuum_max_thresh`（1 亿封顶）要修的问题，也是大表必调 `autovacuum_vacuum_scale_factor` 的原因；
- **插入公式**（PG 13+，commit b07642dbcd）：`ins_since_vacuum > 1000 + 0.2 × reltuples × pcnt_unfrozen` ⇒ vacuum。管 append-only 表（§8.4）。`pcnt_unfrozen` 因子是 master 新改进：已冻结的部分不再计入基数——一张 10 亿行但 95% 已冻结的历史表，实际按 5000 万行的"活跃部分"计算阈值；
- **analyze 公式**：`mod_since_analyze > 50 + 0.1 × reltuples`（TOAST 表和 pg_statistic 不做 analyze，3314-3315）。

### 5.3 vacuum_delay_point 全函数走读（vacuum.c:2437 起）

限流机制分两半。**记账**：缓冲池每次页访问给进程本地变量 `VacuumCostBalance`（globals.c:160）加分——命中 +`vacuum_cost_page_hit`(1)、未命中 +`vacuum_cost_page_miss`(2)、首次弄脏 +`vacuum_cost_page_dirty`(20，guc_parameters.dat:3356-3359)。**结算**：vacuum 各层循环遍布 `vacuum_delay_point`（一阶段每页一次 vacuumlazy.c:1332，三阶段 2690，索引 AM 内部也有）：

```c
void
vacuum_delay_point(bool is_analyze)
{
    double      msec = 0;

    /* Always check for interrupts */
    CHECK_FOR_INTERRUPTS();

    if (InterruptPending)
        return;

    if (IsParallelWorker())
    {
        ...
        parallel_vacuum_update_shared_delay_params();
    }

    if (!VacuumCostActive && !ConfigReloadPending)
        return;

    /*
     * Autovacuum workers should reload the configuration file if requested.
     * This allows changes to [autovacuum_]vacuum_cost_limit and
     * [autovacuum_]vacuum_cost_delay to take effect while a table is being
     * vacuumed or analyzed.
     */
    if (ConfigReloadPending && AmAutoVacuumWorkerProcess())
    {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);
        VacuumUpdateCosts();
        ...
    }

    /*
     * If we disabled cost-based delays after reloading the config file,
     * return.
     */
    if (!VacuumCostActive)
        return;

    /*
     * For parallel vacuum, the delay is computed based on the shared cost
     * balance.  See compute_parallel_delay.
     */
    if (VacuumSharedCostBalance != NULL)
        msec = compute_parallel_delay();
    else if (VacuumCostBalance >= vacuum_cost_limit)
        msec = vacuum_cost_delay * VacuumCostBalance / vacuum_cost_limit;

    /* Nap if appropriate */
    if (msec > 0)
    {
        ...
        if (msec > vacuum_cost_delay * 4)
            msec = vacuum_cost_delay * 4;
        ...
        pgstat_report_wait_start(WAIT_EVENT_VACUUM_DELAY);
        pg_usleep(msec * 1000);
        pgstat_report_wait_end();
        ...
```

（vacuum.c:2437-2509，省略 delay 计时统计段；睡后 `VacuumCostBalance = 0` 重新攒。）逐段：

- **`CHECK_FOR_INTERRUPTS()` 打头**（2443）：这是 vacuum 可以被随时安全取消的机制入口（§8.3）——每页一次的取消响应粒度；
- **免税出口**（2458-2459）：`VacuumCostActive` 为假（手动 VACUUM 默认 `vacuum_cost_delay=0` 不限速；failsafe 也会拔掉它）且无待处理的配置重载 ⇒ 直接返回，热路径两次分支的成本；
- **在线调参**（2467-2478）：autovacuum worker 在此响应 SIGHUP 现场 reload——**可以对着一个正在爬的 anti-wraparound vacuum 在线调大 cost_limit，立即生效**，不必等它重启；
- **令牌桶**（2493-2494）：攒满 `vacuum_cost_limit`（默认 200）就睡 `vacuum_cost_delay`（autovacuum 默认 2ms）的等比例时长，单次睡眠封顶 4 倍（2501-2502，防止参数突变导致长眠）；睡眠计入 `WAIT_EVENT_VACUUM_DELAY` 等待事件（`pg_stat_activity` 可见）；
- **并行分摊**（2491-2492）：并行 vacuum 的 balance 在共享内存汇总，谁把桶打满谁去睡。

据此可算默认 autovacuum 的理论脏页上限：`200/20 页 × (2ms 一睡 ⇒ 500 睡/秒) × 8KB = 39 MB/s`（纯脏页情形）；读命中路径则是 `200/1 × 500 × 8KB ≈ 780 MB/s`。**大表清不动时第一调优对象就是 cost_limit/cost_delay**。

配套机制——**多 worker 均摊**：`AutoVacuumUpdateCostLimit`（autovacuum.c:1753）：

```c
        nworkers_for_balance = pg_atomic_read_u32(&AutoVacuumShmem->av_nworkersForBalance);
        ...
        vacuum_cost_limit = Max(vacuum_cost_limit / nworkers_for_balance, 1);
```

（autovacuum.c:1786-1793；`av_nworkersForBalance` 声明 autovacuum.c:308。）cost 预算是**全体 autovacuum worker 均分**的——把 `autovacuum_max_workers` 从 3 调到 10 不增加总清理带宽，只是切得更碎，单表反而更慢。这是最常见的调优误区。表级设置了 cost 参数的 worker 退出均摊（`wi_dobalance`）。

### 5.4 为什么是 cost-based 限流，而不是 IO 带宽限流（所以然 ④）

直觉方案是"限制 vacuum 的 MB/s"。PG 选择"页访问记账 + 攒满即睡"，理由是一组工程权衡：

1. **vacuum 根本测不到自己的真实 IO**。PG 走 buffered IO：`read()` 可能命中 OS page cache（零物理读），脏页由 checkpointer/bgwriter 在未来某刻代写（vacuum 的"写"只是弄脏 buffer）。要按物理带宽限流，得先有全平台可靠的物理 IO 归因——2004 年（此机制诞生时）没有，今天跨 Linux/BSD/macOS/Windows 也依然没有统一答案。而**逻辑页访问**在缓冲池管理器里天然可数、零额外成本、全平台一致；
2. **代价权重本身就是资源模型**。hit=1 / miss=2 / dirty=20 表达的是"命中最便宜、物理读其次、产生未来写回义务最贵"——它限的不只是读带宽，还包括 vacuum 转嫁给 checkpointer 的写压力和 WAL 压力，这是单纯的 IO 带宽限速器覆盖不了的维度；
3. **自适应比精确更重要**。令牌桶的实际效果是"每做 X 单位逻辑工作睡 Y 毫秒"：存储快时 X 完成得快、睡眠占比自动升高；系统繁忙时 vacuum 的页访问本身变慢、睡眠自动稀疏——粗糙但方向正确的负反馈，无需测速；
4. **实现半径小**。全部状态 = 一个进程本地计数器 + 一次 `pg_usleep`，无内核依赖、无后台校准线程。代价是不精确（默认值曾长期过时：PG 12 把默认 delay 从 20ms 降到 2ms，PG 14 把 page_miss 从 10 降到 2，都是对硬件进化的追认），以及需要 DBA 理解"分数"这个抽象单位。

对照：InnoDB 的对应物是 `innodb_io_capacity`（按 IOPS 建模）——它敢这么做是因为 InnoDB 自己管理 direct IO，物理 IO 完全自见。PG 的选择与其 buffered-IO 架构一体两面。

### 小结

launcher 按 naptime/库数巡库派 worker；worker 全表打分（xid/mxid/vac/ins/anl 五分量取 max）排序后逐表执行；回卷强制凌驾一切开关。执行期由"页访问记账 + 攒满即睡"限流：单位是逻辑页访问的加权分而非字节——因为在 buffered IO 架构下逻辑访问可数而物理 IO 不可见；预算全 worker 均摊，加 worker 不加带宽。

---

## 6. 不变式清单

每条给出"是什么—代码锚点—违反后果"。

1. **回收界限**：只有 `dead_after < OldestXmin`（或过了 GlobalVis 水位）的死元组才可移除。锚点：heapam_visibility.c:1125、pruneheap.c:1439-1453。违反 ⇒ 持有老快照的查询沿 ctid 走到已被回收/复用的槽位，返回错行或 `could not access status of transaction` 类错误；standby 上表现为本应被 conflict 取消的查询读到脏数据。
2. **索引先于堆**：索引项删除完成之前，堆槽位不得从 LP_DEAD 转 LP_UNUSED；任何索引项都不得指向 LP_UNUSED 槽位。锚点：vacuumlazy.c:1256-1263（注释）、lazy_vacuum 的控制流 2454-2460。违反 ⇒ §2.2 推演的悬垂 TID/幽灵读。派生规则：无索引表可一趟完成（`MARK_UNUSED_NOW`）。
3. **relfrozenxid 承诺**：表中不存在 xmin/xmax < relfrozenxid 的未冻结 XID；datfrozenxid 之前的 CLOG 可截断。锚点：heapam.c:7297-7301 的 `ERRCODE_DATA_CORRUPTED` 哨兵、vacuumlazy.c:928-931 的 Assert、936-946 的 skippedallvis 弃权。违反 ⇒ CLOG 查询落在已截断的段上直接报错，或回卷后行"来自未来"静默消失。
4. **aggressive 义务**：aggressive vacuum 必须把 NewRelfrozenXid 推进到 ≥ FreezeLimit——所以它不许跳 all-visible 页（find_next_unskippable_block:1815-1816）、必要时硬等 cleanup 锁（lazy_scan_noprune:2222-2236 + lazy_scan_heap:1449-1452）。违反 ⇒ anti-wraparound vacuum 白跑，表龄继续增长直至 failsafe/停写。
5. **HOT 链根可达**：索引只指链根；heap-only 元组永不被索引直接引用，也永不单独变 LP_DEAD；每链至多一个 LP_REDIRECT。锚点：pruneheap.c:2133-2144 注释与 Assert、heap_prune_chain 三分支。违反（断根）⇒ 活行从索引视角消失（丢行）。
6. **剪枝不留带存储的 DEAD**：pruning 之后页上不允许存在仍占存储的 DEAD 元组。锚点：pruneheap.c:1484-1485 注释、1610-1615（RECENTLY_DEAD 前缀必须走透）、716-717 的 elog(ERROR) 兜底。违反 ⇒ VACUUM 对"该死却还有肉身"的元组既不能冻（xmax 要被移除）又不能留，陷入逻辑矛盾（历史上真实的 bug 类别）。
7. **all-visible 纯洁性**：设了 all-visible 的页不得含 prunable/dead 项；VM 位与页头 `PD_ALL_VISIBLE` 同向。锚点：pruneheap.c:1720-1726、1811-1818（发现矛盾按 VM 损坏修复）、1137-1140。违反 ⇒ index-only scan 跳过堆检查，直接把死行返回给用户。
8. **切点单调链**：`FreezeLimit ≤ OldestXmin`（vacuum.c:1197-1199）、`NewRelfrozenXid` 初始于 OldestXmin 且只向老收（vacuumlazy.c:807）、`freeze_table_age ≤ 0.95 × freeze_max_age`（vacuum.c:1232）。违反第一条 ⇒ 冻结"还有人可能看不见其插入"的行，老快照看到本不该看到的数据。
9. **一表一 vacuum**：同一表同一时刻至多一个 vacuum（`ShareUpdateExclusiveLock` 自斥）。违反的假想世界 ⇒ 两个进程对同页并发剪枝/冻结，dead_items 相互踩踏。

---

## 7. 设计权衡与历史（git 考古）

按时间轴，七个改变了本章形态的 commit（均在本仓库核实）：

1. **282d2a03dd（2007-09-20，Tom Lane）"HOT updates."** 原始提交说明即本章 §3.5 的纲领："When we update a tuple without changing any of its indexed columns, and the new version can be stored on the same heap page, we no longer generate extra index entries for the new version. Instead, index searches follow the HOT-chain links to ensure they find the correct tuple version." 同时引入 `pruneheap.c`、LP_REDIRECT 与查询路径剪枝——把"清理"从 vacuum 独占变为全民义务。权衡：读路径多了跳链与剪枝成本，换 UPDATE 写放大的数量级下降。
2. **b07642dbcd（2020-03-28，David Rowley）"Trigger autovacuum based on number of INSERTs"**（PG 13）。在此之前 insert-only 表永远够不到死元组阈值（§8.4）。权衡：多了一类周期性 vacuum 的开销，换 anti-wraparound 巨债与 index-only scan 退化两大顽疾的缓解。
3. **dc7420c2c9（2020-08-12，Andres Freund）"snapshot scalability: Don't compute global horizons while building snapshots."**（PG 14）。GetSnapshotData 不再顺手算全局 xmin，视界改为按需计算 + GlobalVisState 两水位缓存 + 按 shared/catalog/data/temp 四档分化（§1.5）。这是本章"两层验尸 + dead_after 回传"代码结构的直接来源。权衡：视界从"每快照精确"变"惰性模糊但保守"，换取多核下拿快照的可伸缩性。
4. **5100010ee4（2021-04-07，Peter Geoghegan）"Teach VACUUM to bypass unnecessary index vacuuming."**（PG 14）。§2.7 的 2% + 32MB bypass。设计说明里的关键判断：衡量标准不是死元组数量，而是"这些 LP_DEAD 拖住了多少页设不了 VM 位"。
5. **1e55e7d175（2021-04-07，Peter Geoghegan）"Add wraparound failsafe to VACUUM."**（PG 14）。§4.5 的 failsafe。权衡哲学：回卷停写是一级事故，膨胀是二级问题，紧急时刻用后者换前者。
6. **30e144287a + 667e65aac3（2024-03-21 / 04-02，Masahiko Sawada）"Add TIDStore..." / "Use TidStore for dead tuple TIDs storage during lazy vacuum."**（PG 17）。§2.6 的 radix tree 替换平面数组，1GB 上限与 O(log n) 查找同时消失。配套的 6dbb490261（2024-04-03，Heikki Linnakangas）"Combine freezing and pruning steps in VACUUM" 把剪枝/冻结/VM 合并为单 WAL 引擎（§3.2）。
7. **052026c9b9（2025-02-11，Melanie Plageman）"Eagerly scan all-visible pages to amortize aggressive vacuum"**（PG 18）。§2.4 的 eager scan：普通 vacuum 按 4096 页区域抽样冻结 all-visible 页，用平时的小额分期偿还 aggressive vacuum 的集中债务——freeze"太早 vs 太晚"权衡（§4.3）的最新一次再平衡。

纵览 18 年演进的主线：**清理工作不断"去集中化"**——从 vacuum 独扛，到查询顺手剪枝（2007），到插入也触发维护（2020），到能跳则跳/能免则免（2021），到内存结构升级支撑单轮完成（2024），到冻结债分期（2025）。每一步都在把"一次性大账单"拆成"随用随付的小额税"。

---

## 8. what-if 推演

### 8.1 一个被遗忘的复制槽：从膨胀到停写的完整时间线

设置：某下游 CDC 用逻辑槽 `slot_cdc`；Day 0 下游服务下线，无人删槽。库上写负载约 500 万事务/天。

- **Day 0+**：槽的 `xmin`/`catalog_xmin` 停止前进。`ComputeXidHorizons` 每次都被 procarray.c:1838-1857 的取小拉回 Day 0。**症状**：`pg_stat_user_tables.n_dead_tup` 稳定爬升；autovacuum 照常运行但 vacuum 日志里 "removable cutoff: ..., which was N XIDs old"（vacuumlazy.c:1084-1086）的 N 一天天变大，"tuples: 0 removed, ... dead but not yet removable" 的第二个数字疯长——**vacuum 在跑，只是扫了白扫**。同时 `restart_lsn` 钉住 WAL 回收，`pg_wal` 开始变大。
- **Day 1-7**：热表膨胀显形：页内塞满 RECENTLY_DEAD，HOT 剪枝同样无能为力（视界过不去），非 HOT 更新比例上升（页内没空间），索引跟着膨胀。目录表（catalog_xmin 档）在 DDL 频繁时最先痛。
- **Day 30（1.5 亿事务）**：表龄过 `vacuum_freeze_table_age`，vacuum 纷纷升格 aggressive——但 `NewRelfrozenXid` 从 OldestXmin 起步（vacuumlazy.c:807），而 OldestXmin 钉在 Day 0，**relfrozenxid 几乎推不动**。`vacuum_get_cutoffs` 开始打 `WARNING: cutoff for removing and freezing tuples is far in the past`（vacuum.c:1171-1175，hint 点名 replication slots）。
- **Day 40（2 亿）**：anti-wraparound autovacuum 全面接管（autovacuum.c:3184-3198），日志充满 `(to prevent wraparound)`；打分机制把这些表排最前，worker 循环空转——每轮扫全表、回收 0 行、推进 0。
- **Day 320（16 亿）**：failsafe 触发（vacuumlazy.c:2889），`WARNING: bypassing nonessential maintenance`，卸限速全速跑——依然徒劳，钉子不在速度。
- **Day 419（约 21 亿）**：过 `xidWarnLimit`，每次分配 XID 打 WARNING；数百万条告警刷屏。
- **Day 420**：`xidStopLimit`（varsup.c:139-147）：`ERROR: database is not accepting commands that assign new transaction IDs...` **全库停写**（只读仍可用）。此时 `pg_wal` 可能也已写满磁盘（另一条死法，谁先到看磁盘大小）。
- **修复**：`pg_drop_replication_slot('slot_cdc')` ⇒ 下一次 `ComputeXidHorizons` 立即解放 ⇒ 已在跑的 anti-wraparound vacuum（cost 参数可在线 reload，vacuum.c:2467-2478）一轮回收全部积压死元组并推进 relfrozenxid ⇒ `vac_update_relstats` → datfrozenxid 前进 → CLOG 截断、停写解除。**预防**：监控 `pg_replication_slots` 的 `xmin`/`catalog_xmin` 年龄与 `restart_lsn` 滞后；物理槽配 `max_slot_wal_keep_size`（它保护 WAL，不保护 xmin——xmin 防线仍靠监控）。

### 8.2 autovacuum 永远追不上写入：死亡螺旋及参数解法

设置：单表 2 亿行，UPDATE 密集 2 万行/秒，默认参数。

螺旋的每一圈：死元组阈值 = 50 + 0.2×2 亿 = 4000 万 ⇒ 触发时已积压 4000 万死行（约 33 分钟的量）；worker 开工，默认 cost 预算 39MB/s 脏页 ⇒ 扫 30GB 表 + 5 个索引要数小时；期间又新增上亿死行；`maintenance_work_mem` 偏小 ⇒ TidStore 多次触顶 ⇒ 索引被全量扫 3-4 遍，单轮时间进一步拉长；下一轮起点更烂——页更多（表在长胖）、索引更胖。且 reltuples 随膨胀虚高，阈值水涨船高，触发反而更迟钝。若中途还有夜间报表长事务钉视界，每轮的"dead but not yet removable"再打个折。**终态**：表稳定在 3-5 倍膨胀"动态平衡"，直到某天表龄追上 anti-wraparound。

拆解方向与对应机制（全部指向本章讲过的代码位）：

1. **让单轮更快**：调大 `vacuum_cost_limit` / 调小 `autovacuum_vacuum_cost_delay`（对在跑的 vacuum 可 SIGHUP 生效，vacuum.c:2467-2478）——这是唯一直接加带宽的旋钮；`autovacuum_max_workers` 无效（均摊，autovacuum.c:1786-1793）；
2. **让索引只扫一遍**：加大 `maintenance_work_mem`/`autovacuum_work_mem`（vacuumlazy.c:3419-3421），看日志 `index scans: N` 是否归 1（PG 17 的 TidStore 已大幅缓解）；
3. **让触发更敏感**：表级 `autovacuum_vacuum_scale_factor = 0.01` 或靠 `autovacuum_vacuum_max_threshold`（默认 1 亿，对 5 亿行以上的表生效）封顶；
4. **让垃圾少产生**：`fillfactor=70` + 避免更新索引列 ⇒ 提高 HOT 率（`n_tup_hot_upd` 占比），让清理在页内闭环、根本不进 dead_items；
5. **结构性手段**：分区——把一张追不上的大表变成 N 张追得上的小表（每张独立触发、独立 worker、独立 relfrozenxid）。

### 8.3 vacuum 中途取消：留下什么、为什么安全

`pg_cancel_backend` 一个跑了 6 小时的 VACUUM（响应点就是 `vacuum_delay_point` 开头的 `CHECK_FOR_INTERRUPTS()`，vacuum.c:2443，粒度约每页一次）。留下什么：

- **已剪枝的页**：保持已剪。每页的剪枝/冻结是独立临界区 + 独立 WAL（pruneheap.c:1252-1336），崩溃恢复也会重放——这些是**已落袋的进展**，下次 vacuum 直接受益（页上已是 LP_DEAD 存根，`lazy_scan_noprune` 都能收集）；
- **dead_items（TidStore）**：进程内存，随进程消失。已收集未处理的 TID 白收——但堆上的 LP_DEAD 还在，无非下次重扫；
- **索引**：若取消发生在阶段二中间，某些索引已删完项、某些删了一半。**安全**：被删的索引项指向的堆槽位仍是 LP_DEAD（阶段三没跑），没有任何 TID 悬垂；没删的项下次 `ambulkdelete` 再删（B-tree 删除幂等）。不变式 §6.2 的方向性（先索引后堆）保证任意切断点都无害；
- **relfrozenxid / 统计**：不推进。`vac_update_relstats`（vacuum.c:1432）和 `pgstat_report_vacuum`（vacuumlazy.c:988）都在最末尾——所以 `n_dead_tup` 不清零，autovacuum 很快会重新盯上该表（这是特性：工作没干完就不该销账）；
- **锁与事务**：vacuum 在自己的事务里，取消即回滚释放锁。但注意 vacuum 对 pg_class 的统计更新用的是**非事务性原地更新**（heap_inplace_update 路径），不受回滚牵连——不过既然没执行到，也就无所谓。

一句话：vacuum 的持久副作用全部由"每页原子 + 每步幂等 + 顺序不变式"保护，**任何时刻取消都不损坏数据，只损失未销账的进度**。真正要小心的反而是频繁取消 anti-wraparound autovacuum（例如被每晚的 DDL 踢掉）：进度反复清零，表龄持续上升，直到 failsafe 出场。

### 8.4 insert-only 表：PG 13 前后的两个世界

设置：审计日志表，只 INSERT 不 UPDATE/DELETE，日增 1000 万行。

**PG 12 及以前**：`n_dead_tup` 恒为 0，死元组公式永不触发；表上唯一会发生的 vacuum 是 `age(relfrozenxid) > 2 亿` 时的 anti-wraparound。后果三连：

1. **冻结巨债**：两亿事务攒下的全部页（可能数 TB）在一次 aggressive vacuum 里集中读取 + 冻结 + 写 WAL——"the dreaded anti-wraparound vacuum"，生产事故的常客（IO 打满数小时，还常与业务高峰撞车，DBA 被迫手动 `VACUUM FREEZE` 错峰还债）；
2. **VM 长期空白**：没人设 all-visible 位 ⇒ **index-only scan 全程回堆**（`heap_fetches` 居高），这张"最适合 IOS 的表"恰恰用不上 IOS；
3. **统计尚可**（analyze 公式看 `mod_since_analyze`，INSERT 计入，autoanalyze 会跑）——所以这是"vacuum 专属"的盲区。

**PG 13+**（commit b07642dbcd）：插入公式 `ins_since_vacuum > 1000 + 0.2 × reltuples` 让它每增长约 20% 就得到一次 vacuum：设 VM 位（IOS 生效）、顺手冻结部分页、把冻结债切成分期。**master 上的再进化**：阈值基数乘 `pcnt_unfrozen`（autovacuum.c:3275-3291）——表越冻越多，"活跃基数"越小，触发越勤但每次越便宜；叠加 PG 18 eager scan（052026c9b9）后，理想状态下 aggressive vacuum 到来时发现绝大多数页已 all-frozen，直接按 §2.4 规则 4 跳过，"巨债"被彻底摊薄。运维残余注意点：insert 型 vacuum 默认不清理索引（没有死元组时 `ambulkdelete` 本就不会被调用，只有 `amvacuumcleanup`），开销主要是堆扫描 + 冻结 WAL——把它和"分区 + 老分区一次性 FREEZE 后永久静默"结合是日志表的标准解。

---

## 9. 运维视角总结

### 9.1 表膨胀成因矩阵

膨胀的统一方程：**垃圾产生速率 > 垃圾回收速率**。回收被卡只有两类根因——"视界推不动"（§1.4）与"清理跑不动"（§5.3/§8.2）：

| 成因 | 机制（源码锚点） | 诊断入口 | 处置 |
|---|---|---|---|
| 长事务 / idle in transaction | backend 的 (xid,xmin) 参与 ComputeXidHorizons 取小（procarray.c:1731-1804） | `pg_stat_activity` 按 `xact_start`/`backend_xmin` 排序；vacuum 日志 removable cutoff 滞后 | `idle_in_transaction_session_timeout`、`transaction_timeout`、杀会话 |
| 遗忘的 prepared 事务 | dummy PGPROC 常驻 ProcArray，重启不消失 | `pg_prepared_xacts` | COMMIT/ROLLBACK PREPARED |
| 物理槽（备库失联） | `replication_slot_xmin` 参与取小（procarray.c:1838-1841）；另钉 WAL | `pg_replication_slots.xmin/restart_lsn` | 删废槽；`max_slot_wal_keep_size` |
| 逻辑槽（下游停摆） | `slot_catalog_xmin` 先毒目录表（procarray.c:1850-1857） | 同上看 `catalog_xmin` | 修复/删除订阅；槽延迟监控 |
| 备库反馈 | `hot_standby_feedback` → walsender xmin + PROC_AFFECTS_ALL_HORIZONS 跨库生效（procarray.c:1788-1799） | `pg_stat_replication.backend_xmin` | 权衡：关掉则备库查询可能被冲突取消 |
| 大表阈值迟钝 | `50 + 0.2 × reltuples`（autovacuum.c:3286-3288） | `n_dead_tup` 长期高位、vacuum 间隔过长 | 表级 scale_factor、`autovacuum_vacuum_max_threshold` |
| vacuum 太慢 | cost 令牌桶（vacuum.c:2493-2502）+ 均摊（autovacuum.c:1786-1793） | `pg_stat_progress_vacuum`、日志耗时/`index scans` | cost_limit/delay 在线调；maintenance_work_mem |
| 索引过多 | 阶段二逐索引全量扫（vacuumlazy.c:2531-2550） | 日志 `index scans: N` × 索引数 | 删冗余索引；REINDEX 治已膨胀索引 |
| 非 HOT 更新 | 改索引列/页无空间 ⇒ 完整 UPDATE + 新索引项 | `n_tup_hot_upd / n_tup_upd` | fillfactor 调低、不更新索引列 |

已发生的重度膨胀，普通 VACUUM 治不了（只回收尾部空页）：`pg_repack` 或停机 `VACUUM FULL`。

### 9.2 与 InnoDB purge 的对照

| 维度 | InnoDB purge | PostgreSQL VACUUM |
|---|---|---|
| 垃圾在哪 | undo 段（集中，独立于数据页） | 堆页内（分散，与活数据混居） |
| 回收边界 | 最老 read view | OldestXmin（ProcArray + prepared + walsender + 槽，四路取小） |
| 长事务后果 | history list 增长、undo 表空间膨胀 | 全库死元组判不了死，**表和索引本体膨胀** |
| 清理索引 | purge 按 undo 记录定位删单条 delete-marked 项 | ambulkdelete 全索引扫描 + TidStore 批量判死 |
| 页内轻量清理 | 无对应物（垃圾不在数据页） | HOT pruning：每个读者都是清理工 |
| 空间归还 | undo 独立表空间可 truncate | 堆内空间进 FSM 复用；仅尾部空页归还 OS |
| 事务 ID 回卷 | DB_TRX_ID 48 位，实践中不回卷，无 freeze | 32 位，freeze/relfrozenxid/anti-wraparound 一整套 |
| 限流模型 | innodb_io_capacity（IOPS，direct IO 自见） | cost-based 令牌桶（buffered IO 下按逻辑页访问建模） |
| 失控终局 | undo 膨胀、查询变慢 | XID 回卷 ⇒ 拒绝写入（xidStopLimit） |

关键差异是**垃圾的位置**：undo 集中存放，膨胀被隔离在回滚段里，主数据始终紧凑；PG 的垃圾与活数据同页混居，膨胀直接摊薄每次 IO 的有效载荷。作为交换，PG 读旧版本零成本（版本就在堆里，不必回 undo 链重建）、回滚瞬时完成、且无 undo 表空间这个单点。还有一点 InnoDB 用户最容易低估：**PG 的 VACUUM 不只是垃圾回收**——它同时是回卷保护（freeze）、index-only scan 的使能者（VM）、优化器统计的维护者（autoanalyze）。"这表 append-only 不需要 vacuum"在 PG 里是错误结论（§8.4）。

---

## 自测问题（9 题，附详解）

**1. 一个开了 8 小时的只读 REPEATABLE READ 事务，为什么会导致所有表膨胀？给出完整源码链条。**

该 backend 拿快照时把最老可能可见 XID 记入 `proc->xmin`；RR 隔离级整个事务持同一快照，xmin 8 小时不动。`ComputeXidHorizons`（procarray.c:1673）遍历 ProcArray 对每个 backend 取 `TransactionIdOlder(xmin, xid)`（1750）再全局取小（1762-1803），data 档视界被钉在 8 小时前；`GetOldestNonRemovableTransactionId`（1943）把它作为 OldestXmin 交给 vacuum。此后任何死亡时刻晚于它的元组，在 `HeapTupleSatisfiesVacuum`（heapam_visibility.c:1121-1126）只能判 `RECENTLY_DEAD`；HOT pruning 走 GlobalVisState 同样过不去（水位由同一计算刷新，procarray.c:1902）。只读无关紧要——**快照本身就是钉子**。注意范围限定：它只钉自己所在数据库的 data 档（procarray.c:1796-1803），别的库和临时表不受影响。

**2. 为什么必须"先删索引项、再把 LP_DEAD 置 LP_UNUSED"？颠倒后第一个坏结果何时出现？**

LP_DEAD 槽位仍被索引项引用（TID = 块号+槽位号）。若先置 LP_UNUSED，坏结果不需要等崩溃或 vacuum 结束——**下一条 INSERT 复用该槽位的瞬间**，老索引项就指向了键值无关的新行，任何走该索引的查询立即返回错行（§2.2 四步推演）。正确顺序下任意点失败都安全：索引删一半 + 堆全是 LP_DEAD = 无悬垂（LP_DEAD 不可复用），下次重来即可。代码保证：`lazy_vacuum`（vacuumlazy.c:2454-2460）仅在 `lazy_vacuum_all_indexes` 返回 true（全部索引完成）后才调 `lazy_vacuum_heap_rel`；failsafe 中断索引轮时连带跳过堆轮。

**3. `heap_prune_chain` 为什么遇到 DEAD 不立即停、还要继续沿链走？RECENTLY_DEAD 夹在两个 DEAD 之间时命运如何？**

它维护 `ndeadchain` = "至此全可剪"的前缀长度（pruneheap.c:1592），继续走是为了找**更后面的 DEAD**：夹在 DEAD 前面的 RECENTLY_DEAD 实际上已死（后继版本的删除者都过视界了，前驱的删除者必然更老——可见性测试只是太粗看不出来，1481-1483 注释），必须一并剪掉；否则会留下"仍有存储的 DEAD 元组"，违反 pruneheap.c:1484-1485 的硬性规则（VACUUM 无法处理）。所以夹在 DEAD 之间的 RECENTLY_DEAD 会作为死亡前缀的一部分被剪除，且无需为它推进 standby 冲突视界（1600-1607：后面那个 DEAD 已覆盖）。

**4. LP_REDIRECT 解决什么问题？为什么链中间的死版本可以直接 LP_UNUSED，而链头只能 LP_DEAD 或 LP_REDIRECT？**

HOT 更新不插新索引项，索引永远指链头槽位。剪掉链头死版本后：置 UNUSED ⇒ 悬垂（幽灵读）；置 DEAD ⇒ 断链（活版本从索引消失，丢行）；于是需要第三态——LP_REDIRECT 把根槽位变成指向现存活版本的页内跳板（`ItemIdSetRedirect`，pruneheap.c:2156），索引项终生不变，"索引不知道 HOT 存在"（§3.5 四路穷举）。链中间版本是 heap-only 元组（`HEAP_ONLY_TUPLE`），不变式保证无任何索引项直接指它（pruneheap.c:2133-2144），槽位可当场转世 LP_UNUSED；链头被索引引用，必须走"LP_DEAD 存根 → 阶段二删索引项 → 阶段三 LP_UNUSED"的全流程。

**5. 冻结的"资格线"和"强制线"分别是什么？为什么 `FreezeLimit` 必须 ≤ `OldestXmin`？**

资格线 = OldestXmin：`heap_prepare_freeze_tuple` 里 `freeze_xmin = TransactionIdPrecedes(xid, cutoffs->OldestXmin)`（heapam.c:7304）——只有对所有活跃快照都已可见的 xmin 才**可以**冻。强制线 = FreezeLimit（nextXID - vacuum_freeze_min_age）：页上有更老的 XID 时 `freeze_required` 置位（`heap_tuple_should_freeze`，heapam.c:8086），该页**必须**冻，这是 relfrozenxid 能推进到 FreezeLimit 的保证。若 FreezeLimit > OldestXmin，会强制冻结"某些快照尚看不见其插入"的元组——冻结 = 宣布永久全员可见，老快照会突然看到不该看的行。因此 vacuum.c:1197-1199 强制 clamp：`if (TransactionIdPrecedes(cutoffs->OldestXmin, cutoffs->FreezeLimit)) cutoffs->FreezeLimit = cutoffs->OldestXmin;`——这也解释了长事务如何间接冻结不了：OldestXmin 被钉住，FreezeLimit 跟着被压低。

**6. `age(relfrozenxid)` 逼近 2^31 时为什么必须拒绝写入？写出关键数值链。**

`TransactionIdPrecedes` 按 `(int32)(a-b) < 0` 比较，正确性前提是所有未冻结 XID 与当前 XID 的距离 < 2^31（约 21.4 亿）：一旦某 xmin 落后超过半圈，减法符号翻转，该行被判"来自未来"而静默消失。数值链（varsup.c）：`xidWrapLimit = oldest_datfrozenxid + 2^31`（384 行）是数学悬崖；`xidStopLimit = xidWrapLimit - 3,000,000`（400 行）是停写线，越线后 `GetNewTransactionId` 直接 ERROR（139-147 行）；`xidWarnLimit` 提前 4000 万告警。留 300 万余量是为了停写后仍有 XID 供拯救性操作使用。只读不受影响（不分配 XID）。解除条件：完成最老表的 vacuum 使 datfrozenxid 前进，三条线整体右移。

**7. 把 `autovacuum_max_workers` 从 3 调到 10，单表的 anti-wraparound vacuum 会变快吗？正确的三个提速手段？**

不会。一表一 worker（锁自斥）；且 cost 预算全体均摊：`AutoVacuumUpdateCostLimit`（autovacuum.c:1753）执行 `vacuum_cost_limit = Max(vacuum_cost_limit / nworkers_for_balance, 1)`（1793）——worker 越多单个越慢。正确手段：① 调大 `vacuum_cost_limit`/调小 `autovacuum_vacuum_cost_delay`，SIGHUP 后由 `vacuum_delay_point` 内的 reload 逻辑对**在跑的** vacuum 即时生效（vacuum.c:2467-2478）；② 加大 `maintenance_work_mem`/`autovacuum_work_mem` 减少 TidStore 触顶次数，把 `index scans` 压到 1（vacuumlazy.c:3419-3421）；③ 若已逼近 failsafe 线，可预期 vacuum 自动卸载限速与索引清理（vacuumlazy.c:2930-2932）——或者人为干预：`VACUUM (INDEX_CLEANUP off)` 手动模拟 failsafe 行为，先保回卷再治膨胀。

**8. VACUUM 日志显示 `index scans: 0` 但表上确有少量死元组被发现，是什么机制？什么条件下发生？有什么隐性代价？**

PG 14 的 index bypass（commit 5100010ee4）：`lazy_vacuum`（vacuumlazy.c:2435-2437）在"含 LP_DEAD 的页 < 表的 2%（`BYPASS_THRESHOLD_PAGES`）且 TidStore < 32MB"时跳过整个阶段二/三，LP_DEAD 原地留给未来。条件细节：仅单轮场景（中途因内存触顶轮转过就强制执行，vacuumlazy.c:1371）、`INDEX_CLEANUP` 未强制 on。隐性代价：这些页的 VM all-visible 位设不上（LP_DEAD 在场，pruneheap.c:1220-1221），index-only scan 对它们仍要回堆；LP_DEAD 槽位不能复用，占行指针数组。设计判断正是"这点代价 < 全量扫一遍所有索引"。

**9. 备库开着 `hot_standby_feedback` 时，主库哪张表会因为备库的长查询而膨胀？与备库不反馈相比，各自的故障模式是什么？**

反馈的 xmin 落在主库 walsender 的 `proc->xmin` 上并带 `PROC_AFFECTS_ALL_HORIZONS`（procarray.c:1788-1790），因此**跨库钉住 shared/catalog/data 全部视界**——主库所有数据库的所有普通表都受影响（对比：普通长事务只钉本库）。故障模式对照：开反馈 ⇒ 主库膨胀（备库查询变成主库的"远程长事务"），若 walsender 断连保护即消失，除非叠加复制槽让它持久（procarray.c:1655-1664 注释描述了无槽时的这个窗口）；关反馈 ⇒ 主库正常回收，但备库重放清理记录（`log_heap_prune_and_freeze` 携带的 conflict_xid，pruneheap.c:1239-1245）时会取消仍需要那些行版本的备库查询（recovery conflict）。这是"主库膨胀"与"备库查询被杀"之间的强制二选一，第三选项是备库查询走独立的逻辑副本。
