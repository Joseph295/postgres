# 第 10 篇 · VACUUM 与 autovacuum：为 MVCC 付账

> 对应《PostgreSQL 源码学习指南》第 11 章。源码版本：本仓库 master 分支（20devel）。
> 文中所有 `文件:行号` 均已在本仓库核实；引用的代码片段为真实源码摘录（可能省略无关行）。

## 本章导读

第 09 篇讲过：PG 没有 undo，回滚免费，代价是垃圾（死元组）留在堆里。VACUUM 就是那张迟早要付的账单。这是 PG 与 MySQL 差异最大的运维领域——InnoDB 的 purge 只是遥远的近似，你必须把它当全新知识学。

本章路线：

1. 先回答"为什么需要 VACUUM"：死元组如何判定（`HeapTupleSatisfiesVacuum`）、回收边界（OldestXmin / GlobalVisState）由谁决定、长事务为什么会阻碍回收；
2. 逐段走读 lazy vacuum 主流程 `heap_vacuum_rel`：三阶段（扫堆收集死 TID → 索引批量删除 → 回扫堆把 LP_DEAD 置 LP_UNUSED）、VM 跳页、PG 17 的 TidStore；
3. HOT pruning：查询路径上的"顺手清理"与 LP_REDIRECT 机制；
4. freeze 与回卷保护的完整链条：`heap_prepare_freeze_tuple`、aggressive vacuum、relfrozenxid 推进；
5. autovacuum 的 launcher/worker 架构、触发公式逐行、cost-based delay 限流；
6. 运维视角总结：表膨胀成因矩阵，与 InnoDB purge 对照。

## 前置知识

- **元组头**（`src/include/access/htup_details.h`）：每行有 `xmin`（创建者 XID）、`xmax`（删除者 XID）、`ctid`（版本链指针）与 infomask 标志位。UPDATE = 插新版本 + 旧版本设 xmax。
- **行指针（ItemId / line pointer）**：页内的槽位数组，有四种状态——`LP_NORMAL`（指向元组）、`LP_UNUSED`（可复用）、`LP_DEAD`（元组已删，但槽位还被索引指着）、`LP_REDIRECT`（重定向到同页另一槽位）。
- **CLOG**：每事务 2 bit 的提交状态位图；hint bit 把查询结果缓存在元组头。
- **VM（visibility map）**：每个堆页 2 个 bit：all-visible（页上全部元组对所有事务可见）、all-frozen（全部已冻结）。
- **XID 回卷**：XID 是 32 位循环计数器，比较按模 2^31 的"半圈"规则；老 XID 必须被冻结，否则 40 亿事务后新旧关系颠倒。
- **对 MySQL 用户**：InnoDB 的旧版本在 undo 段里，由 purge 线程沿 history list 清理；PG 的旧版本就在堆页里，由 VACUUM 清理。`history list length` 的 PG 对应物大致是 `pg_stat_user_tables.n_dead_tup` + 最老快照年龄。

---

## 1. 为什么需要 VACUUM：死元组的判定与回收边界

### 1.1 MVCC 债务

每条 UPDATE/DELETE 都会留下旧版本元组。它们不能立即删除——可能还有老快照要读它们。于是产生三个必须回答的问题：

1. 一个元组什么时候从"可能还有人要读"变成"确定没人要读"（死元组）？
2. 这个边界（horizon）由谁、如何计算？
3. 谁负责把死元组占的空间收回来？

第 3 问的答案就是 VACUUM（以及查询路径上的 HOT pruning）。前两问的答案在 `heapam_visibility.c` 和 `procarray.c`。

### 1.2 HeapTupleSatisfiesVacuum：给元组验尸

`src/backend/access/heap/heapam_visibility.c:1113`：

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
            res = HEAPTUPLE_DEAD;          /* 死亡时间早于最老视界 ⇒ 真死 */
    }
    ...
    return res;
}
```

返回值五种（`src/include/access/heapam.h:138`）：

| 结果 | 含义 | VACUUM 的处置 |
|---|---|---|
| `HEAPTUPLE_DEAD` | 死了，且对任何人都不可见 | **可回收** |
| `HEAPTUPLE_RECENTLY_DEAD` | 死了，但可能还有老快照能看见 | 不能动，留给下次 |
| `HEAPTUPLE_LIVE` | 活着 | 保留（考虑冻结） |
| `HEAPTUPLE_INSERT_IN_PROGRESS` | 插入事务还在跑 | 保留 |
| `HEAPTUPLE_DELETE_IN_PROGRESS` | 删除事务还在跑 | 保留 |

结构是两层：`HeapTupleSatisfiesVacuumHorizon`（heapam_visibility.c:1147）做与视界无关的判断，把"死亡时刻"（删除者 XID）通过 `dead_after` 交给上层；上层只做一次 `dead_after < OldestXmin` 的比较。拆两层是为了让不同调用者能用**不同的视界**做比较——VACUUM 用固定的 OldestXmin，HOT pruning 用可动态收紧的 GlobalVisState（§3）。

`HeapTupleSatisfiesVacuumHorizon` 的判断顺序（heapam_visibility.c:1157-1213）：

```c
/* ① 插入者提交了吗？ */
if (!HeapTupleHeaderXminCommitted(tuple))       /* hint bit 没说已提交 */
{
    if (HeapTupleHeaderXminInvalid(tuple))
        return HEAPTUPLE_DEAD;                  /* 插入者已知中止 ⇒ 从没活过，直接死 */
    ...
    else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
        return HEAPTUPLE_INSERT_IN_PROGRESS;    /* 插入者还在跑 */
    else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
        SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, ...);  /* 查 CLOG 并缓存 */
    else
    {
        SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, ...);
        return HEAPTUPLE_DEAD;                  /* 中止或崩溃 ⇒ 死 */
    }
}

/* ② 插入者已提交，看删除者 */
if (tuple->t_infomask & HEAP_XMAX_INVALID)
    return HEAPTUPLE_LIVE;                      /* 没人删过 ⇒ 活 */

if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    return HEAPTUPLE_LIVE;                      /* xmax 只是行锁不是删除 ⇒ 活 */
...
/* ③ 删除者已提交 ⇒ RECENTLY_DEAD，死亡时刻 = xmax，交给上层比视界 */
```

注意两个 PG 特色：**插入者中止的元组直接判 DEAD**（无需等视界——它从未对任何人可见过，这正是"没有 undo"的清理路径）；判断过程顺手写 hint bit，所以 VACUUM 本身也是 hint bit 的主要生产者。

### 1.3 OldestXmin 与 GlobalVisState：回收边界怎么算

**OldestXmin** 由 `GetOldestNonRemovableTransactionId`（`src/backend/storage/ipc/procarray.c:1944`）→ `ComputeXidHorizons`（procarray.c:1674）计算，本质是一次对 ProcArray（运行中事务数组）的扫描：

```c
h->latest_completed = TransamVariables->latestCompletedXid;
/* 初值 = latestCompletedXid + 1，作为上界 */
...
h->slot_xmin = procArray->replication_slot_xmin;              /* ① 复制槽的 xmin */
h->slot_catalog_xmin = procArray->replication_slot_catalog_xmin;

for (int index = 0; index < arrayP->numProcs; index++)        /* ② 遍历所有 backend */
{
    ...
    xid = UINT32_ACCESS_ONCE(other_xids[index]);
    xmin = UINT32_ACCESS_ONCE(proc->xmin);                    /* 该 backend 快照的 xmin */

    xmin = TransactionIdOlder(xmin, xid);                     /* 取其 xid 与 xmin 中更老者 */
    if (!TransactionIdIsValid(xmin))
        continue;                                             /* 没快照没 XID 的闲连接不拖后腿 */

    h->oldest_considered_running =
        TransactionIdOlder(h->oldest_considered_running, xmin);   /* ③ 全局取最小 */

    if (statusFlags & (PROC_IN_VACUUM | PROC_IN_LOGICAL_DECODING))
        continue;                                             /* vacuum 自己不算 */
    ...
    h->data_oldest_nonremovable = TransactionIdOlder(...);    /* 普通表的视界 */
}
```

逐段解读：
- **③ 是长事务危害的机制根源**：视界 = 所有活跃 backend 的（xid, 快照 xmin）中的**最小值**。任何一个事务只要持有老快照（哪怕是只读的 `REPEATABLE READ` 事务，或一个开着事务不提交的空闲连接 `idle in transaction`），它的 `proc->xmin` 就把全库的 OldestXmin 钉死在过去。此后所有表上，死亡时刻晚于该 xmin 的元组统统只能判 `RECENTLY_DEAD`，VACUUM 扫了也白扫——**膨胀不是 vacuum 不勤快，而是视界推不动**；
- **① 复制槽同样拖后腿**：物理槽的 `xmin`、逻辑槽的 `catalog_xmin` 在拿 ProcArrayLock 时一并读入，参与取小。一个断连备库的物理槽或没人消费的逻辑槽，效果等同一个永不提交的事务；
- 视界按对象分四档（shared / catalog / data / temp，`ComputeXidHorizonsResult`，procarray.c:196），临时表只看自己，普通表不受其他库的 backend 影响（procarray.c:1778 起）。

**GlobalVisState**（procarray.c:184）是上述精确计算的**廉价近似缓存**：

```c
struct GlobalVisState
{
    /* XIDs >= are considered running by some backend */
    FullTransactionId definitely_needed;    /* ≥ 它的肯定还有人要 */
    /* XIDs < are not considered to be running by any backend */
    FullTransactionId maybe_needed;         /* < 它的肯定没人要 */
};
```

两条水位线夹出一个"不确定区间"：XID < maybe_needed ⇒ 一定可回收；XID ≥ definitely_needed ⇒ 一定不可回收；夹在中间 ⇒ 调 `GlobalVisTestIsRemovableXid`（procarray.c:4277）时才触发一次真正的 `ComputeXidHorizons` 重算并收紧水位。设计动机：HOT pruning 发生在每次查询的读路径上，不能每次都拿 ProcArrayLock 扫全表 backend；用"模糊但保守"的缓存把大多数判断变成两次 64 位比较。`GlobalVisTestFor(rel)`（procarray.c:4114）按表的类别返回四档缓存之一。

VACUUM 同时使用两者（vacuumlazy.c:786-804 注释）：`OldestXmin` 是一次性算好的硬边界（保证 relfrozenxid 推进的正确性），`vistest` 用于剪枝判断（可能比 OldestXmin 更新、剪得更多，但绝不更少）。

### 小结

死元组判定 = 两层验尸（先定性、再与视界比较死亡时刻）；视界 = min(所有 backend 的 xid/xmin, 复制槽 xmin)。长事务/空闲事务/滞留复制槽都会把视界钉在过去，让全库的死元组"死而不僵"。GlobalVisState 用两条水位线把高频的可回收判断降成两次比较。

---

## 2. lazy vacuum 主流程：heap_vacuum_rel 逐段走读

### 2.1 入口与准备

`VACUUM` 命令经 `commands/vacuum.c` 的调度，对每个堆表调用表访问方法的 `relation_vacuum` 回调，即 `heap_vacuum_rel`（`src/backend/access/heap/vacuumlazy.c:624`）。骨架（省略统计与错误上下文）：

```c
heap_vacuum_rel(Relation rel, const VacuumParams *params, ...)
{
    vacrel = palloc0_object(LVRelState);           /* 本次 vacuum 的全部状态 */
    vac_open_indexes(rel, RowExclusiveLock, &vacrel->nindexes, &vacrel->indrels);
    ...
    /* ① 算三组切点：OldestXmin（死活边界）、FreezeLimit（冻结边界）、
     *    并判定本次是否 aggressive（见 §4） */
    vacrel->aggressive = vacuum_get_cutoffs(rel, params, &vacrel->cutoffs);
    vacrel->rel_pages = orig_rel_pages = RelationGetNumberOfBlocks(rel);
    vacrel->vistest = GlobalVisTestFor(rel);       /* ② 剪枝用的近似视界 */

    /* ③ relfrozenxid 的候选新值，从 OldestXmin 开始，扫描中遇到更老的活 XID 会往回收 */
    vacrel->NewRelfrozenXid = vacrel->cutoffs.OldestXmin;
    ...
    lazy_check_wraparound_failsafe(vacrel);        /* ④ 回卷失险预检 */
    dead_items_alloc(vacrel, params->nworkers);    /* ⑤ 分配死 TID 存储（TidStore） */

    lazy_scan_heap(vacrel);                        /* ★ 第一阶段 + 内嵌二三阶段 */
    ...
    if (should_attempt_truncation(vacrel))
        lazy_truncate_heap(vacrel);                /* 尾部空页截断，归还给 OS */

    /* ⑥ 更新 pg_class：relpages/reltuples/relallvisible + relfrozenxid 推进 */
    vac_update_relstats(...);                      /* vacuum.c:1432 */
    pgstat_report_vacuum(...);                     /* 让 n_dead_tup 归零 */
}
```

重要认知（对照注释 vacuumlazy.c:786-801）：`OldestXmin` 决定"谁算死"，`vistest` 决定"剪枝现场怎么剪"，`NewRelfrozenXid` 从 `OldestXmin` 出发、被扫描中遇到的每个未冻结 XID 往回拉——最终它就是能安全写进 `pg_class.relfrozenxid` 的新值。

### 2.2 三阶段总览

`lazy_scan_heap`（vacuumlazy.c:1279）头部注释（vacuumlazy.c:1251-1256）明确了三阶段结构：

```
阶段一 lazy_scan_heap：       顺序扫堆页 → 剪枝/冻结 → 把残留的 LP_DEAD 的 TID 存入 dead_items
阶段二 lazy_vacuum_all_indexes：对每个索引做 ambulkdelete，删掉指向 dead_items 的索引项
阶段三 lazy_vacuum_heap_rel： 回到堆，把这些 LP_DEAD 槽位改成 LP_UNUSED（真正可复用）
```

为什么必须这个顺序？**LP_DEAD 槽位不能直接复用**：索引里还有指向它的 TID，若先复用槽位放新元组，老索引项会指到无关数据（幽灵读）。必须先删索引项、再释放槽位。这也解释了：索引越多，vacuum 越慢（阶段二对每个索引全量扫描）；以及 `dead_items` 内存装不下时要做多轮"阶段二+三"（`maintenance_work_mem` 直接决定索引被扫几遍）。

### 2.3 阶段一：扫堆与 VM 跳页

主循环（vacuumlazy.c:1321-1500）要点：

```c
while (true)
{
    vacuum_delay_point(false);                 /* ① cost-based 限流点（§5.3） */

    if (vacrel->scanned_pages % FAILSAFE_EVERY_PAGES == 0)
        lazy_check_wraparound_failsafe(vacrel);   /* ② 定期检查回卷失险 */

    /* ③ dead_items 满了 ⇒ 中途先做一轮索引清理+堆清理，清空后继续扫 */
    if (vacrel->dead_items_info->num_items > 0 &&
        TidStoreMemoryUsage(vacrel->dead_items) > vacrel->dead_items_info->max_bytes)
    {
        lazy_vacuum(vacrel);                   /* 内嵌执行阶段二+三 */
        ...
    }

    buf = read_stream_next_buffer(stream, &per_buffer_data);   /* ④ 流式预读取下一页 */
    if (!BufferIsValid(buf))
        break;
    ...
    got_cleanup_lock = ConditionalLockBufferForCleanup(buf);   /* ⑤ 尝试 cleanup 锁 */
    ...
    if (got_cleanup_lock)
        lazy_scan_prune(vacrel, buf, blkno, page, ...);        /* ⑥ 剪枝+冻结+收 TID */
}
```

- **VM 跳页**藏在 ④ 的读流回调 `heap_vac_scan_next_block`（vacuumlazy.c:1648）里：它查 visibility map 找"下一个不可跳过的块"。all-visible 的页没有死元组，普通 vacuum 直接跳过；all-frozen 的页连 aggressive vacuum 也可跳过（无需冻结）。但有一个反直觉的细节（vacuumlazy.c:1683-1699 注释）：**可跳范围小于 `SKIP_PAGES_THRESHOLD`（32 页，vacuumlazy.c:209）时不跳**——顺序读时 OS 预读已经把页读进来了，跳跃反而破坏预读；而且不跳意味着有机会顺路推进 relfrozenxid；
- ⑤ 的 **cleanup 锁**（排他 + 无其他 pin）是移动页内元组（碎片整理）的前提。拿不到时降级走 `lazy_scan_noprune`（只收集已有 LP_DEAD、不剪枝），除非本页有必须冻结的老 XID 才硬等（vacuumlazy.c:1442-1453）——vacuum 尽量不与前台查询死磕；
- ⑥ `lazy_scan_prune`（vacuumlazy.c:2021）调用与 HOT pruning 共用的 `heap_page_prune_and_freeze`（§3），把 HOT 链剪成 LP_DEAD/LP_REDIRECT 并执行冻结，随后把页上所有 LP_DEAD 的偏移量交给 `dead_items_add`（vacuumlazy.c:2106）。

### 2.4 dead_items 的存储：PG 17 的 TidStore（radix tree）

PG 16 及以前，死 TID 存在一个**平面有序数组**里（每个 TID 6 字节），索引清理时用二分查找，且受 1GB 上限约束。PG 17 引入 **TidStore**（`src/backend/access/common/tidstore.c`，注释见 tidstore.c:7："Internally it uses a radix tree as the storage for TIDs. The key is the BlockNumber and the value is a bitmap of offsets"）：

- `dead_items_alloc`（vacuumlazy.c:3416）调 `TidStoreCreateLocal(max_bytes, insert_only)`（vacuumlazy.c:3474；实现 tidstore.c:162）；
- 键 = 块号，值 = 该块内偏移量的位图，底层是基数树（`lib/radixtree.h`）；
- 三重收益：**查找 O(键长)** 而非 O(log n)（阶段二里每个索引项都要查一次"你指的 TID 死了吗"，这是最热的路径，回调 `vac_tid_reaped` 用 `TidStoreIsMember` 实现）；**内存省一个量级**（位图 + 前缀共享 vs 每 TID 6 字节）；**没有 1GB 上限**，超大表一轮扫完的概率大增，索引重复扫描轮数下降。并行 vacuum 时用共享内存版 TidStore，worker 直接共享。

### 2.5 阶段二：索引批量删除

`lazy_vacuum`（vacuumlazy.c:2369）先考虑**跳过优化**（vacuumlazy.c:2435-2437）：

```c
threshold = (double) vacrel->rel_pages * BYPASS_THRESHOLD_PAGES;   /* 2% */
bypass = (vacrel->lpdead_item_pages < threshold &&
          TidStoreMemoryUsage(vacrel->dead_items) < 32 * 1024 * 1024);
```

含 LP_DEAD 的页不足表的 2% 且 TID 集 < 32MB 时，干脆不扫索引（LP_DEAD 留给下次）——避免为几十个死 TID 全量扫几百 GB 的索引。否则 `lazy_vacuum_all_indexes`（vacuumlazy.c:2494）对每个索引调 `vac_bulkdel_one_index` → 索引 AM 的 `ambulkdelete`（B-tree 为 `btbulkdelete`）：**全量扫描索引**，对每个索引项回调查询 TidStore，命中则删。

### 2.6 阶段三：LP_DEAD → LP_UNUSED

`lazy_vacuum_heap_rel`（vacuumlazy.c:2640）遍历 TidStore 中的块号，对每块调 `lazy_vacuum_heap_page`（vacuumlazy.c:2758）：

```c
START_CRIT_SECTION();

for (int i = 0; i < num_offsets; i++)
{
    ItemId      itemid;
    OffsetNumber toff = deadoffsets[i];

    itemid = PageGetItemId(page, toff);
    Assert(ItemIdIsDead(itemid) && !ItemIdHasStorage(itemid));
    ItemIdSetUnused(itemid);               /* ★ LP_DEAD → LP_UNUSED */
    unused[nunused++] = toff;
}
...
PageTruncateLinePointerArray(page);        /* 页尾连续的 UNUSED 槽位整个砍掉 */
...
if ((vmflags & VISIBILITYMAP_VALID_BITS) != 0)
{
    PageSetAllVisible(page);
    visibilitymap_set(blkno, vmbuffer, vmflags, ...);  /* 顺手设 VM all-visible */
}
MarkBufferDirty(buffer);
if (RelationNeedsWAL(vacrel->rel))
    log_heap_prune_and_freeze(...);        /* 写一条 WAL，恢复/备库同步此清理 */

END_CRIT_SECTION();
```

此时索引项已删，槽位可以安全复用。注意 master 上的新优化：释放槽位与设置 VM 位合并在同一个临界区、同一条 WAL 记录里（vacuumlazy.c:2780-2807 注释）。

最后：`lazy_truncate_heap` 尝试截断**表尾部**的连续空页（唯一把空间还给操作系统的路径——表中部的空闲空间只进 FSM 复用，这就是"VACUUM 不缩表"的准确含义；要整体缩表得用重写全表的 `VACUUM FULL`，`commands/cluster.c`）；`vac_update_relstats`（vacuum.c:1432）把 relpages/reltuples/relallvisible 和新的 relfrozenxid 写回 `pg_class`。

### 小结

lazy vacuum = ①扫堆（VM 跳页 + 剪枝/冻结 + 死 TID 进 TidStore）→ ②每个索引 ambulkdelete（查 TidStore 删项）→ ③回堆把 LP_DEAD 置 LP_UNUSED 并更新 FSM/VM。顺序不可颠倒（索引安全）；内存不够就多轮；死 TID 太少可跳过索引阶段；只有尾部空页会归还 OS。TidStore（radix tree + 偏移位图）是 PG 17 对第①③阶段内存与第②阶段查找的双重升级。

---

## 3. HOT pruning：查询路径上的顺手清理

### 3.1 动机与入口

等 autovacuum 来清理太慢了——一个频繁 UPDATE 的热页可能几秒内就堆满死版本。PG 的答案：**每个读到该页的查询都可以顺手做页内清理**（单页、无需索引配合）。入口 `heap_page_prune_opt`（`src/backend/access/heap/pruneheap.c:272`），由 `heapam.c` 的页访问路径调用：

```c
heap_page_prune_opt(Relation relation, Buffer buffer, ...)
{
    ...
    /* ① 快速放弃：页上没有"可能有死元组"的提示 */
    prune_xid = PageGetPruneXid(page);      /* 页头 pd_prune_xid：上次 DELETE/UPDATE 留下的提示 */
    if (!TransactionIdIsValid(prune_xid))
        return;

    /* ② 用 GlobalVisState 廉价判断：最老的死亡时刻过视界了吗 */
    vistest = GlobalVisTestFor(relation);
    if (!GlobalVisTestIsRemovableXid(vistest, prune_xid, true))
        return;

    /* ③ 启发式：页快满了才值得清理 */
    minfree = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);
    minfree = Max(minfree, BLCKSZ / 10);
    if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
    {
        if (!ConditionalLockBufferForCleanup(buffer))   /* ④ 拿不到 cleanup 锁就算了 */
            return;
        ...
        heap_page_prune_and_freeze(&params, &presult, ...);   /* ⑤ 真正剪枝 */
    }
}
```

四道闸门层层过滤，保证读路径上的额外开销接近零：多数页在 ①（没删过东西）或 ③（还有空间）就返回；② 靠 §1.3 的 GlobalVisState 两次比较搞定；④ 绝不为清理而等锁。这是"人人有责的微型 vacuum"——它解决空间**复用**（页内），但不处理索引，所以只能把死元组缩成一个 LP_DEAD 存根，最终仍需 VACUUM 收尾。

### 3.2 LP_REDIRECT：重定向指针机制

剪枝的核心 `heap_page_prune_and_freeze`（pruneheap.c:1112）→ `heap_prune_chain`（pruneheap.c:1505）沿 HOT 链逐个验尸。设 HOT 链：

```
索引项 ──→ [槽1: 版本A(死)] → [槽2: 版本B(死)] → [槽3: 版本C(活)]
```

索引只指向链头（HOT 更新不插新索引项）。剪掉 A、B 后链头槽位不能直接 LP_UNUSED（索引还指着槽 1），也不该留成 LP_DEAD（那就断链找不到 C 了）。方案：**把槽 1 改成 LP_REDIRECT，重定向到槽 3**：

```
索引项 ──→ [槽1: LP_REDIRECT→3] [槽2: LP_UNUSED] [槽3: 版本C(活)]
```

- 槽 1 的 `ItemId` 不再指元组数据，`lp_off` 字段改存目标槽号（`ItemIdSetRedirect`，执行处 pruneheap.c:2156）；A、B 的元组数据被整页碎片整理回收，**中间节点槽 2 直接 LP_UNUSED**（heap-only 元组没有索引指它，可立即复用）；
- 决策记录在 `heap_prune_record_redirect`（pruneheap.c:1731）：`redirected[]` 数组成对存 (from, to)，最后统一执行并写 WAL；
- 读路径遇到 LP_REDIRECT 就多跳一步；后续再剪枝时 redirect 指针也会**跟着链头前移**；若整条链都死光，LP_REDIRECT 退化成 LP_DEAD，等 VACUUM 连索引一起清。

一个页内间接层，换来了"索引不变、堆内自由收缩版本链"的能力——这是 PG 对抗 UPDATE 写放大最重要的机制（配合 `fillfactor < 100` 给 HOT 留页内空间）。

### 小结

HOT pruning 把版本链清理摊到每次页访问上：pd_prune_xid + GlobalVisState + 空间启发式三道闸控制成本；LP_REDIRECT 保住索引到链头的引用，链中间的 heap-only 死版本立即变 LP_UNUSED 复用。它解决页内空间复用，索引清理仍归 VACUUM。

---

## 4. freeze 与回卷保护的完整链条

### 4.1 为什么要 freeze

XID 是 32 位循环计数器，`TransactionIdPrecedes` 按模 2^31 比较——任何时刻，比当前 XID"老半圈"以上的 XID 会被判为"在未来"。若一个元组的 xmin 老到超过半圈，它会突然变成"尚未发生的插入"而集体消失。因此老元组必须**冻结**：宣布"此行对所有人可见，不再与任何 XID 比较"（现代 PG 置 `HEAP_XMIN_FROZEN` 标志位，即 infomask 的 committed|invalid 两位同置，保留原始 xmin 供取证）。

### 4.2 heap_prepare_freeze_tuple

`src/backend/access/heap/heapam.c:7267`。VACUUM 的剪枝阶段对每个存活元组调用它，判断 xmin/xmax 是否老于冻结切点并生成"冻结计划"（先算后改，改动由 `heap_freeze_prepared_tuples`（heapam.c:7600）在临界区统一执行并记 WAL）：

```c
xid = HeapTupleHeaderGetXmin(tuple);
if (!TransactionIdIsNormal(xid))
    xmin_already_frozen = true;                 /* 已冻结/特殊 XID，跳过 */
else
{
    if (TransactionIdPrecedes(xid, cutoffs->relfrozenxid))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg_internal("found xmin %u from before relfrozenxid %u", ...)));
                                                /* 比表的 relfrozenxid 还老 ⇒ 数据损坏 */
    ...                                         /* < FreezeLimit 则加入冻结计划 */
}
```

三个层次的切点（由 `vacuum_get_cutoffs` 统一计算，见下）：
- `OldestXmin`：死活边界；
- `FreezeLimit = nextXID - vacuum_freeze_min_age`（且 ≤ OldestXmin）：**必须**冻结的下限——比它老的 XID 本轮必须处理；
- 页级优化：若整页所有 XID 都能冻结，则一次冻透并设 VM 的 all-frozen 位（`pagefrz.freeze_required` 与页级决策，见函数头注释 heapam.c:7242-7248）。`vacuum_freeze_min_age`（默认 5000 万）的意义是"别急着冻新元组"——它们可能马上又被更新，冻了白冻。

### 4.3 aggressive vacuum 的触发：vacuum_get_cutoffs

普通 vacuum 会用 VM 跳过 all-visible 页，**这些页上的老 XID 就没机会被冻结**，因此普通 vacuum 不能把 relfrozenxid 推得太远。定期需要一次"不许跳 all-visible 页"（只可跳 all-frozen 页）的 **aggressive vacuum**。判定在 `vacuum_get_cutoffs`（`src/backend/commands/vacuum.c:1106`）尾部（vacuum.c:1230-1239）：

```c
if (freeze_table_age < 0)
    freeze_table_age = vacuum_freeze_table_age;               /* 默认 1.5 亿 */
freeze_table_age = Min(freeze_table_age, autovacuum_freeze_max_age * 0.95);
                                          /* 封顶在强制线的 95%，给例行 vacuum 留机会 */
aggressiveXIDCutoff = nextXID - freeze_table_age;
...
if (TransactionIdPrecedesOrEquals(cutoffs->relfrozenxid, aggressiveXIDCutoff))
    return true;                          /* 表龄超线 ⇒ 本次升格为 aggressive */
```

翻译成运维语言：`age(relfrozenxid) > vacuum_freeze_table_age`（默认 1.5 亿）时，**任何**落到这张表上的 vacuum 自动升格为 aggressive。95% 封顶是精心设计：确保夜间例行 VACUUM 会在 anti-wraparound autovacuum（更具侵入性）触发之前先获得一次 aggressive 机会。

### 4.4 relfrozenxid 的推进与回卷保护全链条

`heap_vacuum_rel` 里 `NewRelfrozenXid` 从 `OldestXmin` 出发（vacuumlazy.c:807），扫描中每遇到一个保留下来的未冻结 XID 就往回收，结束后写入 `pg_class.relfrozenxid`（`vac_update_relstats`，vacuum.c:1432）。它是"本表所有页中可能存在的最老未冻结 XID"的下界——**只有 aggressive vacuum（扫过全部非 all-frozen 页）才能大幅推进它**。

完整的多级防线（触发阈值从低到高）：

| 层级 | 触发条件 | 行为 | 源码 |
|---|---|---|---|
| 例行冻结 | 每次 vacuum | 冻结 < FreezeLimit 的 XID | heapam.c:7267 |
| aggressive | `age(relfrozenxid) > vacuum_freeze_table_age`(1.5 亿) | 不跳 all-visible 页，保证能推进 relfrozenxid | vacuum.c:1230-1239 |
| anti-wraparound autovacuum | `age(relfrozenxid) > autovacuum_freeze_max_age`(2 亿) | 即使表没死元组、甚至 autovacuum=off 也强制启动 | autovacuum.c:3184-3198 |
| failsafe | `age > vacuum_failsafe_age`(16 亿) | vacuum 进入紧急模式：跳过索引清理/截断、无视 cost delay，全速冻结 | vacuumlazy.c:863、vacuum.c:1273 |
| 最后防线 | 距回卷仅剩约 300 万 XID | 拒绝分配新 XID（报错），只读自保（varsup.c 的 xidStopLimit） | transam/varsup.c |

数据库级视图：`datfrozenxid = min(所有表的 relfrozenxid)`，CLOG 只能截断到全库 datfrozenxid 之前——所以**一张被长事务/坏索引卡住 vacuum 的表，会阻止整个库的 CLOG 回收和回卷时钟复位**。

### 小结

freeze 把老 XID 移出比较体系以对抗 32 位回卷。三条年龄线各司其职：`vacuum_freeze_min_age` 决定冻多老的元组、`vacuum_freeze_table_age` 决定何时升格 aggressive（不跳 all-visible 页以推进 relfrozenxid）、`autovacuum_freeze_max_age` 决定何时强制启动。failsafe 和 xidStopLimit 是最后两道保险丝。

---

## 5. autovacuum：让账单自动支付

### 5.1 launcher / worker 两级架构

`src/backend/postmaster/autovacuum.c` 文件头注释即架构说明。两级分工：

- **launcher**（常驻，`AutoVacLauncherMain`，autovacuum.c:411）：不做任何清理，只当调度器。主循环 `launcher_determine_sleep`（autovacuum.c:626）按 `autovacuum_naptime`（默认 60s）除以数据库数量安排节奏，目标是"每个库每 naptime 被视察一次"；到点后挑一个最久没被视察的库，通过向 postmaster 发信号请求 fork 一个 worker（launcher 自己不能 fork——worker 需要 postmaster 的直系子进程身份）；
- **worker**（临时，`AutoVacWorkerMain`，autovacuum.c:1418）：连上目标库执行 `do_autovacuum`（autovacuum.c:1928）——扫一遍 `pg_class`，对每个表调 `relation_needs_vacanalyze` 判断是否需要处理，把需要的表排成清单逐个执行 vacuum/analyze，干完退出。同库并发上限由 `autovacuum_max_workers`（autovacuum.c:125）与槽位 `autovacuum_worker_slots` 控制。

master 分支的新演进：`relation_needs_vacanalyze` 现在还输出一组 **score**（`AutoVacuumScores`，autovacuum.c:329），worker 按分数高低决定处理顺序——最接近回卷/最超阈值的表优先，不再是简单的 OID 顺序。

### 5.2 relation_needs_vacanalyze 的触发公式逐行

`src/backend/postgres/autovacuum.c` 的 `relation_needs_vacanalyze`（autovacuum.c:3074）。函数头注释（autovacuum.c:3005-3007）给出公式，代码逐行印证：

**第一步：参数三级取值**（autovacuum.c:3140-3146）——表级 reloption 优先，否则用 GUC：

```c
vac_scale_factor = (relopts && relopts->vacuum_scale_factor >= 0)
    ? relopts->vacuum_scale_factor
    : autovacuum_vac_scale;               /* GUC autovacuum_vacuum_scale_factor，默认 0.2 */

vac_base_thresh = (relopts && relopts->vacuum_threshold >= 0)
    ? relopts->vacuum_threshold
    : autovacuum_vac_thresh;              /* GUC autovacuum_vacuum_threshold，默认 50 */
```

**第二步：回卷强制**（autovacuum.c:3184-3198）——凌驾于一切开关之上：

```c
xidForceLimit = recentXid - freeze_max_age;
force_vacuum = (TransactionIdIsNormal(relfrozenxid) &&
                TransactionIdPrecedes(relfrozenxid, xidForceLimit));
/* MultiXact 同理 */
*wraparound = force_vacuum;
```

即 `age(relfrozenxid) > autovacuum_freeze_max_age` ⇒ 必须 vacuum，**即使该表 autovacuum=off、即使全局 autovacuum=off**（注释 autovacuum.c:3250-3254：此时只做被强制的表）。这就是日志里 `(to prevent wraparound)` 的来源。

**第三步：死元组阈值**（autovacuum.c:3286-3299）：

```c
vacthresh = (float4) vac_base_thresh + vac_scale_factor * reltuples;
if (vac_max_thresh >= 0 && vacthresh > (float4) vac_max_thresh)
    vacthresh = (float4) vac_max_thresh;        /* PG 18+：给大表的阈值封顶 */

vacinsthresh = (float4) vac_ins_base_thresh +
    vac_ins_scale_factor * reltuples * pcnt_unfrozen;   /* 插入型阈值（PG 13+） */
anlthresh = (float4) anl_base_thresh + anl_scale_factor * reltuples;

if (av_enabled && vactuples > vacthresh)
    *dovacuum = true;
```

翻译：**`n_dead_tup > 50 + 0.2 × reltuples` 时触发 vacuum**（`vactuples` 即统计系统里的 `tabentry->dead_tuples`，autovacuum.c:3261）。三个公式并列：
- 死元组公式：管 UPDATE/DELETE 型负载；默认 20% 意味着 5 亿行的表要攒 1 亿死行才触发——大表几乎必须调小 scale_factor 或用 `autovacuum_vacuum_max_threshold`（新 GUC，autovacuum.c:129）封顶；
- 插入公式（`ins_since_vacuum` 超过 `1000 + 0.2 × reltuples × 未冻结比例`）：管 append-only 表，让它们也定期获得 VM/冻结维护（否则第一次 anti-wraparound vacuum 会是灾难性的全表冻结）。`pcnt_unfrozen` 因子（autovacuum.c:3275-3291）是 master 的新改进：已冻结部分不计入基数；
- analyze 公式：`mod_since_analyze > 50 + 0.1 × reltuples`。

没有统计数据（`!tabentry`，表从未被写过）则直接返回不处理（autovacuum.c:3258）。

### 5.3 cost-based delay：给清理装上限速器

vacuum 是纯后台工作，不能与前台争 I/O。机制分两半：

**记账**：缓冲池的每次页访问给全局变量 `VacuumCostBalance` 加分——命中 +`vacuum_cost_page_hit`(1)，未命中 +`vacuum_cost_page_miss`(2)，弄脏 +`vacuum_cost_page_dirty`(20)（bufmgr.c 各处累加，变量定义 globals.c:160）。

**结算**：vacuum 的各层循环里遍布 `vacuum_delay_point`（`src/backend/commands/vacuum.c:2438`，如 lazy_scan_heap 每页调一次，vacuumlazy.c:1332）：

```c
if (!VacuumCostActive && !ConfigReloadPending)
    return;                                   /* 没开限速（手动 VACUUM 默认 delay=0） */
...
else if (VacuumCostBalance >= vacuum_cost_limit)
    msec = vacuum_cost_delay * VacuumCostBalance / vacuum_cost_limit;

if (msec > 0)
{
    if (msec > vacuum_cost_delay * 4)
        msec = vacuum_cost_delay * 4;         /* 单次最多睡 4 倍，防长眠 */
    pg_usleep(msec * 1000);
    ...
    VacuumCostBalance = 0;                    /* 睡完清零重新攒 */
}
```

就是一个令牌桶：攒满 `vacuum_cost_limit`（默认 200）分就睡 `vacuum_cost_delay` 毫秒。autovacuum 用自己的参数（`autovacuum_vacuum_cost_delay` 默认 2ms、cost_limit 默认继承 200），据此可算出 autovacuum 的默认脏页写速率上限约为 `200/20 页 × (1000/2) 次/秒 × 8KB ≈ 39MB/s`——大表清不动时，第一调优对象就是这两个参数。

两个配套机制：
- **多 worker 均摊**：`VacuumUpdateCosts` / `AutoVacuumUpdateCostLimit`（autovacuum.c:1684 / 1753）把 cost_limit 按当前活跃 worker 数（`av_nworkersForBalance`，autovacuum.c:308）均分——**加大 `autovacuum_max_workers` 并不增加总清理带宽**，只是分得更细，这是最常见的调优误区；
- **动态生效**：`vacuum_delay_point` 里检测到 SIGHUP 会现场 reload 配置（vacuum.c:2467-2478），所以可以对着一个正在爬的 anti-wraparound vacuum 在线调大 cost_limit，立即提速；failsafe 触发时则直接关掉限速（`VacuumFailsafeActive`）。

### 小结

launcher 按 naptime 巡库派 worker；worker 对每表算三个阈值公式（死元组 `50+0.2N`、插入、analyze），回卷强制凌驾于一切开关；执行期由"页访问记账 + 攒满即睡"的令牌桶限流，多 worker 均摊同一份预算。

---

## 6. 运维视角总结

### 6.1 表膨胀成因矩阵

膨胀的统一方程：**垃圾产生速率 > 垃圾回收速率**。回收被卡只有两类根因——"视界推不动"（§1.3：算出来的 OldestXmin 太老，死元组判不了死）和"清理跑不动"（vacuum 没跑/跑不快）：

| 成因 | 机制（源码依据） | 诊断入口 | 处置 |
|---|---|---|---|
| 长事务 / idle in transaction | backend 的 xmin 钉住 ComputeXidHorizons 的取小结果（procarray.c:1731-1775） | `pg_stat_activity` 按 `xact_start` 排序；vacuum 日志的 "removable cutoff" 滞后 | `idle_in_transaction_session_timeout`、杀会话 |
| 物理复制槽（备库失联） | `replication_slot_xmin` 参与视界取小（procarray.c:1728）；还会阻止 WAL 回收 | `pg_replication_slots` 的 `xmin`、`restart_lsn` | 删除废槽；`max_slot_wal_keep_size` |
| 未消费的逻辑槽 | `replication_slot_catalog_xmin` 钉住目录表视界（procarray.c:1729）；CDC 下游停摆的典型后果 | 同上，看 `catalog_xmin` | 修复/删除订阅；监控槽延迟 |
| 备库反馈 | `hot_standby_feedback=on` 把备库最老查询的 xmin 传回主库 | `pg_stat_replication.backend_xmin` | 权衡：关掉则备库查询可能被冲突取消 |
| 频繁更新 + 阈值不敏感 | 大表 `0.2×reltuples` 阈值太钝（autovacuum.c:3286） | `n_dead_tup` 长期高位 | 表级调小 `autovacuum_vacuum_scale_factor`、用 max_threshold |
| vacuum 太慢 | cost delay 令牌桶（vacuum.c:2493-2502）；多 worker 均摊不加速 | vacuum 进度视图 `pg_stat_progress_vacuum`、日志耗时分解 | 调 cost_limit/delay；`maintenance_work_mem` 减少索引扫描轮数 |
| 索引过多 | 阶段二对每个索引全量扫描（vacuumlazy.c:2494） | vacuum 日志 index scans 次数 | 删冗余索引 |
| 非 HOT 更新 | 改了索引列/页内没空间 ⇒ 走完整 UPDATE 路径 | `pg_stat_user_tables.n_tup_hot_upd` 占比 | `fillfactor` 调低、避免更新索引列 |

已发生的重度膨胀，普通 VACUUM 治不了（只回收尾部空页）：用 `pg_repack` 或停机 `VACUUM FULL` 重写。

### 6.2 与 InnoDB purge 的对照

| 维度 | InnoDB purge | PostgreSQL VACUUM |
|---|---|---|
| 垃圾在哪 | undo 段（集中，独立于数据页） | 堆页内（分散，与活数据混居） |
| 谁产生回收边界 | 最老 read view | OldestXmin（ProcArray + 复制槽取小） |
| 长事务的后果 | history list 增长，undo 表空间膨胀 | 全库死元组无法判死，**表和索引本体膨胀** |
| 清理索引 | 需要（purge 删除 delete-marked 索引项） | 需要（阶段二 ambulkdelete） |
| 空间归还 | undo 独立表空间可 truncate | 堆内空间只进 FSM 复用；仅尾部空页归还 OS |
| 额外职责 | 无 | 冻结（回卷保护）、VM 维护（index-only scan 依赖）、统计信息（analyze） |
| 失控的终局 | undo 膨胀、查询变慢 | XID 回卷 ⇒ 拒绝写入（xidStopLimit） |

关键差异在"垃圾的位置"：undo 集中存放，膨胀被隔离在回滚段里，主数据始终紧凑；PG 的垃圾与活数据同页混居，膨胀直接摊薄每次 I/O 的有效载荷（扫 100 页可能 60 页是死的）。作为交换，PG 读旧版本零成本（版本就在堆里，不用回 undo 链重建），且回滚瞬间完成。还有一点 InnoDB 用户容易忽略：**PG 的 VACUUM 不只是垃圾回收**，它同时是回卷保护（freeze）、index-only scan 的使能者（VM）和优化器统计的维护者（autoanalyze）——所以"这表是 append-only 的，不需要 vacuum"在 PG 里是错误结论。

---

## 自测问题

**1. 一个开了 8 小时的只读 REPEATABLE READ 事务，为什么会导致所有表膨胀？请给出源码链条。**

该 backend 的快照 xmin 记录在 `proc->xmin`；`ComputeXidHorizons`（procarray.c:1674）遍历 ProcArray 对所有 backend 的 (xid, xmin) 取最小值得出视界，8 小时前的 xmin 把 OldestXmin 钉在 8 小时前。此后任何表上死亡时刻晚于它的元组，在 `HeapTupleSatisfiesVacuum`（heapam_visibility.c:1125）里都只能判 `HEAPTUPLE_RECENTLY_DEAD` 而非 `DEAD`，VACUUM 和 HOT pruning 都无法回收。只读不重要——快照本身就是钉子。

**2. 为什么 lazy vacuum 必须"先删索引项、再把 LP_DEAD 改成 LP_UNUSED"？如果颠倒会怎样？**

LP_DEAD 槽位仍被索引项引用。若先置 LP_UNUSED，槽位可被新插入复用；老索引项的 TID 会指向毫不相干的新元组，索引扫描返回错误行。所以 `lazy_vacuum`（vacuumlazy.c:2369）严格保证 `lazy_vacuum_all_indexes` 成功完成后才调 `lazy_vacuum_heap_rel`；`dead_items` 内存不够时宁可分多轮，每轮都完整执行"索引→堆"顺序。

**3. LP_REDIRECT 解决什么问题？链中间的死元组为什么可以直接 LP_UNUSED 而链头不行？**

HOT 更新不给新版本插索引项，索引永远指向链头。剪掉链头的死版本后，须保留一个从原链头槽位到现存活版本的跳板，即 LP_REDIRECT（pruneheap.c:1731、2156）。链中间的版本是 heap-only 元组（`HEAP_ONLY_TUPLE` 标志），保证没有任何索引项直接指向它们，故槽位可立即 LP_UNUSED 复用；链头槽位有索引引用，只能在 VACUUM 删完索引项后（作为 LP_DEAD）才可释放。

**4. `vacuum_freeze_min_age`、`vacuum_freeze_table_age`、`autovacuum_freeze_max_age` 三个参数分别控制什么？为什么 freeze_table_age 被封顶在 freeze_max_age 的 95%？**

min_age（默认 5000 万）：元组 XID 老于它才冻结（FreezeLimit = nextXID - min_age），避免冻结马上又被更新的新元组。table_age（默认 1.5 亿）：表龄超过它则本次 vacuum 升格 aggressive——不跳 all-visible 页，从而能安全推进 relfrozenxid（vacuum.c:1230-1239）。max_age（默认 2 亿）：表龄超过它则无条件强制启动 anti-wraparound autovacuum（autovacuum.c:3184-3198）。95% 封顶（vacuum.c:1232）确保例行 vacuum（如夜间任务）总能在强制线之前先得到一次 aggressive 机会，用温和的方式完成冻结，避免走到更具侵入性的强制路径。

**5. 把 `autovacuum_max_workers` 从 3 调到 10，单表的 anti-wraparound vacuum 会变快吗？为什么？正确的提速手段是什么？**

不会。第一，一张表同一时间只有一个 worker 在处理；第二，cost-based delay 的预算是全体 worker 均摊的——`AutoVacuumUpdateCostLimit`（autovacuum.c:1753）把 cost_limit 按活跃 worker 数（av_nworkersForBalance）平分，worker 越多每个反而越慢。正确手段：调大 `vacuum_cost_limit`／调小 `autovacuum_vacuum_cost_delay`（可对运行中的 vacuum 在线 reload 生效，vacuum.c:2467-2478）、加大 `maintenance_work_mem`（减少索引扫描轮数）；若已接近 failsafe（`vacuum_failsafe_age`），vacuum 会自动脱掉限速全速冲刺。
