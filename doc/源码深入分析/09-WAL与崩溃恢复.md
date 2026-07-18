# 第 09 篇 · WAL 与崩溃恢复：数据库可靠性的全部秘密

> 对应《PostgreSQL 源码学习指南》第 10 章。源码版本：本仓库 master 分支（20devel）。
> 文中所有 `文件:行号` 均已在本仓库核实；引用的代码片段为真实源码摘录（可能省略无关行）。

## 本章导读

如果说 MVCC 是 PostgreSQL 的灵魂，WAL（Write-Ahead Log，预写式日志）就是它的心脏——写操作的持久性、崩溃恢复、物理复制、逻辑解码、PITR，全部构建在这一条只追加（append-only）的日志之上。用大数据的话说：**the log is the database**，WAL 之于 PostgreSQL，就像 commit log 之于 Kafka。本章按"格式 → 写入 → 并发 → 理论对照 → checkpoint → 恢复 → 归档"的顺序推进：

1. 先解剖 WAL 记录的物理格式（`xlogrecord.h` 逐字段）；
2. 跟着 `XLogBeginInsert → XLogRegisterBuffer → XLogInsert` 走一遍写入路径，重点逐行分析 `XLogRecordAssemble` 里 full-page-write 的判定；
3. 深入 `xlog.c` 的 WALInsertLock 多槽并发设计与组提交；
4. 与 ARIES、InnoDB 对照，理解 PG"有 redo 无 undo"的深层原因；
5. 逐段走读 `CreateCheckPoint`，看 REDO 点如何确定、脏页按什么顺序刷盘；
6. 逐段走读崩溃恢复主循环 `PerformWalRecovery`，用源码印证"页级幂等"；
7. 最后概述归档、PITR 与 restartpoint。

## 前置知识

- **页（Page）**：PG 的表文件由 8KB 的页组成，页头第一个字段就是 `pd_lsn`（page LSN）。读过第 07 篇（存储管理）最好，没读过只需记住这一点。
- **缓冲池**：数据页的修改都发生在共享缓冲池里，脏页由 checkpointer/bgwriter 异步写盘。
- **事务提交**：`COMMIT` 时唯一必须同步等待磁盘的是 WAL（`XLogFlush`），数据页可以慢慢刷。
- **对 MySQL 用户**：PG 的 WAL 大致对应 InnoDB 的 redo log（ib_logfile / #innodb_redo），但 PG 没有独立的 undo log 和 binlog——WAL 一份日志承担了 redo + 复制 + CDC 三份职责。

---

## 1. WAL 物理格式：一条记录长什么样

### 1.1 总体布局

`src/include/access/xlogrecord.h:20` 的注释给出了一条 WAL 记录的完整布局：

```
Fixed-size header (XLogRecord struct)     固定头
XLogRecordBlockHeader                     块引用头 × 0..N
...
XLogRecordDataHeader[Short|Long]          主数据头
block data                                每个块引用的数据（可能是整页镜像）
...
main data                                 rmgr 自定义的主数据
```

一条记录 = **固定头 + 若干"块引用" + 主数据**。"块引用"（block reference）是核心抽象：记录声明"我修改了哪些页"，恢复逻辑据此做通用处理（读页、比较 LSN、还原整页镜像），rmgr 只需处理页内的具体改动。

### 1.2 XLogRecord 固定头逐字段

`src/include/access/xlogrecord.h:41`：

```c
typedef struct XLogRecord
{
    uint32      xl_tot_len;   /* total len of entire record */
    TransactionId xl_xid;     /* xact id */
    XLogRecPtr  xl_prev;      /* ptr to previous record in log */
    uint8       xl_info;      /* flag bits, see below */
    RmgrId      xl_rmid;      /* resource manager for this record */
    /* 2 bytes of padding here, initialize to zero */
    pg_crc32c   xl_crc;       /* CRC for this record */
} XLogRecord;
```

逐字段解读：

| 字段 | 含义 | 关键点 |
|---|---|---|
| `xl_tot_len` | 整条记录总长度（含头） | 读取器靠它跳到下一条记录；上限 `XLogRecordMaxSize`（约 1020MB，xlogrecord.h:74） |
| `xl_xid` | 产生这条记录的事务 XID | 逻辑解码按它把记录归到事务；无 XID 的操作（如 checkpoint）为 0 |
| `xl_prev` | **前一条记录的起始 LSN** | 反向链指针。这是 WAL 防"半截旧日志被误当新日志"的关键校验之一：如果读到的记录 `xl_prev` 对不上，说明日志到此为止 |
| `xl_info` | 8 个标志位 | 高 4 位归 rmgr 自由使用（`XLR_RMGR_INFO_MASK 0xF0`，如 heap 用它区分 INSERT/UPDATE/DELETE），低 4 位归 XLogInsert 内部使用（xlogrecord.h:62） |
| `xl_rmid` | 资源管理器 ID | 恢复时按它分发给 `heap_redo`/`btree_redo` 等回调（见 §6.3） |
| `xl_crc` | CRC32C 校验和 | 覆盖除头外的全部内容 + 头的前半部分。恢复时靠 CRC 判断"日志写到哪里就断了"——**WAL 的结尾不是靠文件长度，而是靠第一条校验失败的记录** |

### 1.3 块引用：XLogRecordBlockHeader

`src/include/access/xlogrecord.h:103`：

```c
typedef struct XLogRecordBlockHeader
{
    uint8       id;             /* block reference ID */
    uint8       fork_flags;     /* fork within the relation, and flags */
    uint16      data_length;    /* number of payload bytes (not including page image) */
    /* If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows */
    /* If BKPBLOCK_SAME_REL is not set, a RelFileLocator follows */
    /* BlockNumber follows */
} XLogRecordBlockHeader;
```

- `id`：块引用编号（0 ~ `XLR_MAX_BLOCK_ID`=32，xlogrecord.h:241）。特殊 id 255/254 表示主数据（短/长格式），253 是复制源（origin），252 是顶层事务 XID；
- `fork_flags`：低 4 位是 fork 号（main/FSM/VM），高 4 位是标志（xlogrecord.h:196）：
  - `BKPBLOCK_HAS_IMAGE 0x10`——本块带整页镜像（full-page image，FPI）；
  - `BKPBLOCK_HAS_DATA 0x20`——本块带 rmgr 自定义数据；
  - `BKPBLOCK_WILL_INIT 0x40`——重放时会重建整页（比如新页插入），因此**不需要**先读旧页；
  - `BKPBLOCK_SAME_REL 0x80`——与上一个块引用同表，省掉 RelFileLocator 的存储（空间优化）。
- 带镜像时后跟 `XLogRecordBlockImageHeader`（xlogrecord.h:141）：记录镜像长度、"空洞"位置（`hole_offset`）与压缩方式。PG 知道标准页中部（`pd_lower` 到 `pd_upper` 之间）是全零空洞，会把它从镜像里抠掉再存（可再叠加 pglz/lz4/zstd 压缩，`wal_compression`）。

### 1.4 LSN 的本质：WAL 流中的字节偏移

`src/include/access/xlogdefs.h:21`：

```c
typedef uint64 XLogRecPtr;
```

LSN（Log Sequence Number，类型名 `XLogRecPtr`）就是一个 **64 位无符号整数，表示该位置在整条 WAL 逻辑字节流中的偏移量**。没有任何神秘结构：LSN 相减就是两点之间的 WAL 字节数（监控里 `pg_wal_lsn_diff()` 干的就是这个）。习惯上显示为 `高32位/低32位` 的十六进制，如 `0/1699A28`。

三个身份合一：
1. **地址**：给定 LSN，可以唯一定位到某个 WAL 段文件的某个偏移；
2. **版本号**：每个数据页头的 `pd_lsn` 记录"最后修改此页的 WAL 记录结束位置"，恢复时用它做幂等判断（§6.4）；
3. **时钟**：LSN 单调递增，是整个集群的逻辑时间轴——复制延迟、恢复进度都用 LSN 差衡量。

### 1.5 WAL 段文件命名规则

WAL 逻辑流被切成默认 16MB 的段文件存放在 `pg_wal/`。命名逻辑在 `src/include/access/xlog_internal.h:165`：

```c
static inline void
XLogFileName(char *fname, TimeLineID tli, XLogSegNo logSegNo, int wal_segsz_bytes)
{
    snprintf(fname, XLOG_FNAME_LEN + 1, "%08X%08X%08X", tli,
             (uint32) (logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes)),
             (uint32) (logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)));
}
```

文件名是 24 个十六进制字符，分三段各 8 位：`0000000A 00000003 000000C5` = 时间线 + "log id" + 段号。

- **时间线（TimeLine ID）**：每次 PITR 恢复或备库提升都会开出一条新时间线，防止"平行宇宙"的 WAL 互相覆盖；
- 后两段合起来就是段序号 `XLogSegNo`（= LSN / 段大小）。历史上 WAL 曾按 4GB 一个 "log file" 组织，所以拆成两段显示——第二段是 `LSN 高 32 位`，第三段是低 32 位落在哪个段。**注意第三段不会数到 FF**：16MB 段时每个 "log id" 只有 0x00~0xFF 共 256 个段（`XLogSegmentsPerXLogId`，xlog_internal.h:99）。
- `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)`（xlog_internal.h:116）完成 LSN → 段号换算，就是一个整除。

### 小结

WAL 记录 = 固定头（含 CRC、反向链指针、rmgr 分发标签）+ 自描述的块引用列表 + rmgr 私有数据。LSN 是 WAL 字节流的 64 位偏移量，同时充当页版本号和集群逻辑时钟。段文件名 = 时间线 + LSN 高位 + 段号，全部是十六进制的算术，没有魔法。

---

## 2. WAL 写入路径：从 heap_insert 到日志缓冲区

### 2.1 三步 API

任何模块写 WAL 都是固定三步（以 `heap_insert` 为例，heapam.c 中可见同样模式）：

```c
XLogBeginInsert();                          /* ① 开始组装 */
XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);  /* ② 登记修改的页 */
XLogRegisterData(&xlrec, SizeOfHeapInsert);      /* ②' 登记主数据 */
recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT);  /* ③ 组装并插入 */
PageSetLSN(page, recptr);                   /* ④ 把返回的 LSN 写进页头 */
```

**`XLogBeginInsert`**（`src/backend/access/transam/xloginsert.c:153`）只做状态检查：断言上一条记录已复位、当前不在恢复模式（恢复期间只读 WAL 不写 WAL）、没有重复调用。真正的登记数据放在进程本地的静态数组 `registered_buffers[]` / `rdatas[]` 里（xloginsert.c:101-131）——**组装阶段完全无锁**，这是设计要点：贵的内存拷贝和判断都在拿锁之前做完。

**`XLogRegisterBuffer`**（xloginsert.c:246）把一个被修改的缓冲区登记为块引用：记下页的物理坐标（`BufferGetTag`）、页指针（供做整页镜像）和 flags，填进 `registered_buffers[block_id]`。函数开头的断言（xloginsert.c:263-270）值得注意：

```c
Assert(BufferIsDirty(buffer));      /* 页必须已标脏 */
Assert(BufferIsLockedByMeInMode(buffer, BUFFER_LOCK_EXCLUSIVE) || ...);
```

**登记时页必须已经脏、已经被本进程排他锁住**——这是 `access/transam/README` 里"先改页、再写 WAL、最后设页 LSN，全程持有页锁 + critical section"规则的机器检查。flags 里最常用的：
- `REGBUF_STANDARD`：标准页布局，做镜像时可抠掉空洞；
- `REGBUF_WILL_INIT`：重放会重建页，无需镜像；
- `REGBUF_FORCE_IMAGE` / `REGBUF_NO_IMAGE`：强制/禁止整页镜像。

### 2.2 XLogInsert 主体

`src/backend/access/transam/xloginsert.c:482`：

```c
XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
    ...
    do
    {
        /* 拿到"当前 REDO 点"和"是否需要整页写"的本地快照（无锁，可能过期） */
        GetFullPageWriteInfo(&RedoRecPtr, &doPageWrites);

        rdt = XLogRecordAssemble(rmid, info, RedoRecPtr, doPageWrites,
                                 &fpw_lsn, &num_fpi, &fpi_bytes,
                                 &topxid_included);

        EndPos = XLogInsertRecord(rdt, fpw_lsn, curinsert_flags, num_fpi,
                                  fpi_bytes, topxid_included);
    } while (!XLogRecPtrIsValid(EndPos));   /* 返回 Invalid ⇒ 重新组装重试 */

    XLogResetInsertion();
    return EndPos;
}
```

逐段解说：
- **组装/插入分离 + 乐观重试**：`XLogRecordAssemble` 在无锁状态下把记录组装成 `XLogRecData` 链，组装依赖的 `RedoRecPtr`（最近一次 checkpoint 的 REDO 点）可能在组装期间被新 checkpoint 推进。`XLogInsertRecord`（xlog.c:784）拿到插入锁后会复核，发现过期就返回 `InvalidXLogRecPtr`，外层 `do...while` 重新组装——典型的乐观并发控制，因为 checkpoint 很少发生，绝大多数插入一次成功；
- 返回值 `EndPos` 是**记录末尾的 LSN**，调用者将它写入页头（`PageSetLSN`）。刷脏页前必须保证 WAL 已刷到页 LSN——这一行注释就写在函数头上（xloginsert.c:477："This implements the basic WAL rule 'write the log before the data'"）。

### 2.3 XLogRecordAssemble 的 full-page-write 判定（逐行分析）

这是本节的核心。`src/backend/access/transam/xloginsert.c:621` 的块引用循环里（xloginsert.c:678-699）：

```c
/* Determine if this block needs to be backed up */
if (regbuf->flags & REGBUF_FORCE_IMAGE)
    needs_backup = true;                       /* ① 调用者强制要镜像 */
else if (regbuf->flags & REGBUF_NO_IMAGE)
    needs_backup = false;                      /* ② 调用者声明不需要 */
else if (!doPageWrites)
    needs_backup = false;                      /* ③ full_page_writes=off 且无在线备份 */
else
{
    /*
     * We assume page LSN is first data on *every* page that can be
     * passed to XLogInsert, whether it has the standard page layout
     * or not.
     */
    XLogRecPtr  page_lsn = PageGetLSN(regbuf->page);

    needs_backup = (page_lsn <= RedoRecPtr);   /* ④ 核心判定！ */
    if (!needs_backup)
    {
        if (!XLogRecPtrIsValid(*fpw_lsn) || page_lsn < *fpw_lsn)
            *fpw_lsn = page_lsn;               /* ⑤ 记录未做镜像页的最小 LSN */
    }
}
```

第 ④ 行是 PostgreSQL 防"页撕裂"（torn page）的全部逻辑，值得展开：

- **`page_lsn <= RedoRecPtr` 意味着什么？** 页头 LSN 是上一次修改本页的 WAL 位置。若它 ≤ 当前 REDO 点，说明**自最近一次 checkpoint 以来本页尚未被修改过**——即本次是"checkpoint 后第一次改这个页"。此时必须把整页写进 WAL；反之（页 LSN > REDO 点），说明 checkpoint 之后已经有某条 WAL 记录给它做过镜像了，本次只记增量即可。
- **为什么 checkpoint 后第一次修改要写整页？** 崩溃恢复从 REDO 点开始重放。如果崩溃发生在操作系统把 8KB 页写了一半时（磁盘扇区 512B/4KB < 页 8KB），磁盘上是半新半旧的"撕裂页"，其内容无法作为增量重放的基础。有了整页镜像，恢复时直接用镜像**覆盖**该页，不依赖磁盘上页的任何内容，撕裂自然无害。而 checkpoint 保证 REDO 点之前的修改都已完好落盘，所以每个 checkpoint 周期只需第一次修改付出整页代价。
- **第 ⑤ 行 `fpw_lsn` 的用途**：对没做镜像的页，记住其中最小的页 LSN。`XLogInsertRecord`（xlog.c:885-896）拿到插入锁后复核：

```c
if (RedoRecPtr != Insert->RedoRecPtr)          /* REDO 点变了（刚发生 checkpoint） */
    RedoRecPtr = Insert->RedoRecPtr;           /* 更新本地副本 */
doPageWrites = (Insert->fullPageWrites || Insert->runningBackups > 0);

if (doPageWrites &&
    (!prevDoPageWrites ||
     (XLogRecPtrIsValid(fpw_lsn) && fpw_lsn <= RedoRecPtr)))
{
    /* 某个没做镜像的页按新 REDO 点其实需要镜像 —— 放弃，重来 */
    WALInsertLockRelease();
    END_CRIT_SECTION();
    return InvalidXLogRecPtr;
}
```

`fpw_lsn <= RedoRecPtr` 表示"存在一个未做镜像的页，其页 LSN 落到了新 REDO 点之前"——组装时的判定已失效，必须回到 `XLogInsert` 的循环重新组装。这一对"无锁乐观判定 + 持锁复核"配合，让 99.9% 的插入不用为 checkpoint 并发付出任何代价。

组装的其余部分（xloginsert.c:723-757）：需要镜像时计算空洞（标准页取 `pd_lower`~`pd_upper`）、尝试压缩、设置 `BKPIMAGE_APPLY`（只有真正因 FPW 而做的镜像在重放时才覆盖页；因一致性校验 `wal_consistency_checking` 而附带的镜像不覆盖）。最后计算 CRC、填好 `XLogRecord` 头（`xl_prev` 留空，等预留空间时才知道）。

### 小结

写 WAL 是"无锁组装 + 短临界区插入"的两段式。full-page write 的判定就一行：`page_lsn <= RedoRecPtr`——checkpoint 后第一次改页则记整页，否则记增量；配合插入时的 REDO 点复核，用乐观重试解决与 checkpoint 的竞争。

---

## 3. WAL 缓冲与并发：多槽插入锁与组提交

### 3.1 两步插入协议

`XLogInsertRecord`（xlog.c:784）头顶的大注释（xlog.c:824-855）说得非常清楚，插入 = 两步：

1. **预留空间**：全局唯一的插入头 `Insert->CurrBytePos` 由自旋锁 `insertpos_lck` 保护，预留就是 `CurrBytePos += size`；
2. **拷贝数据**：把记录拷进 WAL 缓冲区中预留的位置——**这一步可以完全并行**。

预留的实现 `ReserveXLogInsertLocation`（xlog.c:1149）：

```c
SpinLockAcquire(&Insert->insertpos_lck);
startbytepos = Insert->CurrBytePos;
endbytepos = startbytepos + size;          /* 预留 = 一次加法 */
prevbytepos = Insert->PrevBytePos;
Insert->CurrBytePos = endbytepos;
Insert->PrevBytePos = startbytepos;        /* 顺手记录前一条位置 → xl_prev */
SpinLockRelease(&Insert->insertpos_lck);

*StartPos = XLogBytePosToRecPtr(startbytepos);   /* 换算成 LSN 在锁外做 */
*EndPos = XLogBytePosToEndRecPtr(endbytepos);
*PrevPtr = XLogBytePosToRecPtr(prevbytepos);
```

两个精妙设计：
- `CurrBytePos` 存的是**扣除页头的"可用字节位置"**而非 LSN，这样预留就是纯加法，与页边界换算（要考虑每个 WAL 页的页头）被挪到锁外（xlog.c:1162-1170 注释）。自旋锁临界区只有五次赋值——这是全库竞争最激烈的锁之一，每纳秒都要省；
- `xl_prev` 反向链在这里顺手生成，无需额外同步。

### 3.2 WALInsertLock 的多槽设计

拷贝阶段的并发控制靠 8 个插入锁（`NUM_XLOGINSERT_LOCKS`，xlog.c:157）。结构定义在 xlog.c:374：

```c
typedef struct
{
    LWLock      lock;
    pg_atomic_uint64 insertingAt;   /* 本槽位当前插入进行到的 LSN */
    XLogRecPtr  lastImportantAt;
} WALInsertLock;
/* 外层 union WALInsertLockPadded 把每个槽填充到整缓存行，防伪共享（xlog.c:388） */
```

工作机制（xlog.c:339-363 注释 + 代码印证）：

- **插入者**：拿**任意一个**插入锁即可插入（8 个槽 ⇒ 最多 8 个并发拷贝互不阻塞）。`WALInsertLockAcquire`（xlog.c:1412）优先复用上次用过的槽（缓存亲和），拿不到就换下一个——竞争者会自动散布到空闲槽上；
- **刷盘者**：要把缓冲区某段刷到磁盘，必须确认"这段范围内的插入都拷贝完了"。它并不等待所有插入者，而是检查每个被持有的插入锁上的 `insertingAt`：**该值表示此插入者的进度，比刷盘目标靠后的插入者可以直接忽略**（`WaitXLogInsertionsToFinish` 的逻辑）；
- 小记录拷完就放锁，从不更新 `insertingAt`（一次原子写都省了）；只有跨页、需要在持锁期间睡眠（等缓冲区腾空）时才更新——注释（xlog.c:353-358）特别指出这不只是优化：若插入者 A 等待插入者 B 刷盘、B 又在等 A，会死锁，`insertingAt` 打破了这种循环等待。

与 InnoDB 对照：MySQL 8.0 重构后的 redo log 用"预留 LSN（原子加）+ 并行拷贝 + link_buf 收敛写入点"，思路与 PG 的"自旋锁预留 + 多槽并行拷贝 + insertingAt 收敛"殊途同归——都是把"定序"压缩到极小的原子操作，把"搬数据"完全并行化。

### 3.3 XLogWrite 与 XLogFlush

WAL 缓冲区是环形的若干 8KB 页（`wal_buffers`）。落盘分工：

- `XLogWrite`（xlog.c:2325）：真正调 `write()`/fsync 的函数，必须持有 `WALWriteLock`。它尽量把连续的缓冲页合并成一次大 `write()`（xlog.c:2343-2350 注释），写到段文件边界或请求点为止；
- `XLogFlush`（xlog.c:2800）：保证"LSN ≤ record 的 WAL 已落盘"，提交路径调用它；
- `XLogBackgroundFlush`（xlog.c:3003）：walwriter 后台进程周期性调用，异步推进落盘，替 `synchronous_commit=off` 的事务擦屁股。

### 3.4 组提交的实现细节

组提交（group commit）没有独立的"组长选举"代码，而是由 `XLogFlush` 的锁协议自然涌现（xlog.c:2848-2930）：

```c
for (;;)
{
    RefreshXLogWriteResult(LogwrtResult);
    if (record <= LogwrtResult.Flush)
        break;                                    /* ① 已有人替我刷过 ⇒ 白捡 */
    ...
    insertpos = WaitXLogInsertionsToFinish(WriteRqstPtr);

    /* ② 拿锁失败则等待——但醒来后不立刻抢锁，先回到循环头复查 */
    if (!LWLockAcquireOrWait(WALWriteLock, LW_EXCLUSIVE))
        continue;

    RefreshXLogWriteResult(LogwrtResult);
    if (record <= LogwrtResult.Flush)
    {   LWLockRelease(WALWriteLock); break;  }    /* ③ 持锁复查 */

    /* ④ 可选：故意睡一会儿，攒更多"跟班" */
    if (CommitDelay > 0 && enableFsync &&
        MinimumActiveBackends(CommitSiblings))
    {
        pg_usleep(CommitDelay);
        insertpos = WaitXLogInsertionsToFinish(insertpos);  /* 睡完把水位再推远 */
    }

    /* ⑤ 刷到当前最远的完成插入点，而不只是自己的 record */
    WriteRqst.Write = insertpos;
    WriteRqst.Flush = insertpos;
    XLogWrite(WriteRqst, insertTLI, false);
    LWLockRelease(WALWriteLock);
    break;
}
```

三层机制叠加成组提交：
1. **搭便车**（①③）：`LWLockAcquireOrWait` 是专为这个场景发明的原语——等锁的人被唤醒时**不持有锁**，先检查"锁的前任是否顺手把我的 LSN 也刷了"（前任按 ⑤ 刷到了全局最远插入点，大概率覆盖了我），是则一次系统调用都不用做就返回。注释（xlog.c:2868-2872）明说这是为了维持 fsync 瓶颈下的组提交速率；
2. **顺手多刷**（⑤）：拿到锁的"组长"不只刷自己需要的位置，而是刷 `WaitXLogInsertionsToFinish` 返回的当前最远已完成插入点——把排在后面的兄弟的记录一并带走；
3. **主动等客**（④）：`commit_delay`/`commit_siblings` 让组长睡一小觉攒批，默认关闭（0），因为前两层在高并发下已经足够。

对照 InnoDB：这相当于 `binlog_group_commit_sync_delay` + redo 的组提交合并成了一层——PG 没有 binlog，无需两阶段的组提交编排，路径短得多。

### 小结

WAL 并发写入 = 自旋锁保护的字节位置预留（几纳秒）+ 8 槽插入锁下的并行拷贝 + `insertingAt` 进度水位让刷盘者只等必要的人。组提交靠 `LWLockAcquireOrWait` 的"醒来先复查"与"组长刷到最远点"自然形成，`commit_delay` 只是可选的增强。

---

## 4. ARIES 对照：为什么 PG 有 redo 没有 undo

### 4.1 ARIES 三要素在 PG 的对应

| ARIES（Mohan 1992） | PostgreSQL | 说明 |
|---|---|---|
| WAL 先行（脏页落盘前日志必须落盘） | ✔ 完全一致 | bufmgr 刷页前调 `XLogFlush(page_lsn)` |
| Redo 重演历史（repeating history） | ✔ 物理页级重放 | §6 的恢复主循环；比 ARIES 的逻辑 redo 更"物理" |
| Undo 回滚未提交事务（loser transactions） | ✘ **没有 undo 阶段** | 恢复只有 redo，重放到 WAL 末尾即完成 |
| CLR（补偿日志记录） | ✘ 不需要 | 没有 undo 自然没有 CLR |

### 4.2 没有 undo 的深层原因：no-overwrite storage + CLOG

InnoDB 是**原地更新**存储：UPDATE 直接改行，旧值搬进 undo log。崩溃时磁盘上可能存在"未提交事务已经写进数据页"的状态，必须靠 undo 把它们**物理擦除**，否则别人会读到脏数据。

PG 是 **no-overwrite（不覆盖写）** 存储（源自 Stonebraker《The Design of POSTGRES》1986）：
- UPDATE = 插入新版本元组 + 旧版本打 xmax 标记，**旧版本原封不动留在堆里**；
- 未提交事务的新元组即使随脏页落了盘也无妨——它的 `xmin` 在 CLOG（`src/backend/access/transam/clog.c`，每事务 2 bit 的提交状态位图）里永远不会是 COMMITTED。崩溃后该事务在 CLOG 中呈"未提交"状态，MVCC 可见性检查（`HeapTupleSatisfiesMVCC`）会把这些元组当作不可见的垃圾过滤掉；
- 因此"回滚"= 在 CLOG 把 XID 标成 ABORTED（甚至什么都不标，缺省即不可见），**一个字节都不用碰数据页**。

代价守恒：InnoDB 把清理成本放在 purge 线程（消化 undo 历史链），PG 把它推迟给 VACUUM（清理堆里的死元组，见第 10 篇）。**undo 没有消失，只是改名叫"死元组"并搬进了堆本身。**

### 4.3 full-page write vs InnoDB doublewrite buffer

两者解决同一个问题——页大小 > 磁盘原子写单位导致的撕裂页——但策略相反：

| | PG full-page writes | InnoDB doublewrite |
|---|---|---|
| 机制 | checkpoint 后首次修改的页，整页写入 **WAL** | 每次刷脏页，先把整页写入共享表空间的 doublewrite 区，再写数据文件 |
| 写放大位置 | WAL 流膨胀（checkpoint 越频繁越严重） | 每次刷页都双写（顺序写 doublewrite 区 + 随机写数据文件） |
| 恢复方式 | 重放 WAL 时用镜像覆盖页（`RestoreBlockImage`，xlogutils.c:403） | 崩溃后扫描 doublewrite 区，用完好副本修复撕裂页，再正常跑 redo |
| 依赖假设 | WAL 自身 8KB 页有 CRC，半条记录会被 CRC 截断，天然抗撕裂 | redo 是增量的，必须以"页完好"为前提，故需 doublewrite 兜底 |
| 可关闭条件 | 文件系统保证原子写（如 ZFS 的 CoW）时可 `full_page_writes=off` | 同理可关 doublewrite |

一句话：**InnoDB 为每次刷脏页买保险，PG 为每个 checkpoint 周期的首次修改买保险**。PG 的方案让 WAL 变大（可用 `wal_compression` 缓解），但数据文件路径上零额外 I/O；调大 `checkpoint_timeout`/`max_wal_size` 摊薄 FPI 是 PG 调优的常识来源。

### 小结

PG 只有 redo 没有 undo，根源是 no-overwrite 存储：旧版本留在堆里 + CLOG 集中记录提交状态，使"未提交数据落盘"无害、回滚零成本，代价转移给 VACUUM。抗撕裂页上，PG 用"WAL 里的整页镜像"替代 InnoDB 的 doublewrite。

---

## 5. Checkpoint：给恢复画一条起跑线

### 5.1 CreateCheckPoint 逐段走读

checkpoint 的目的：把某时刻之前的所有脏页刷盘，从而允许恢复从该时刻（REDO 点）开始，并回收更早的 WAL。主函数 `CreateCheckPoint`（`src/backend/access/transam/xlog.c:7400`），由 checkpointer 进程调用。按执行顺序：

1. **判定类型**（xlog.c:7418）：`CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY` 视为 shutdown checkpoint（做完后不允许再写 WAL）；
2. **空闲跳过**（xlog.c:7487-7497）：非强制 checkpoint 时，若上次 checkpoint 以来没有"重要"WAL 活动（`GetLastImportantRecPtr()` 仍等于上次 checkpoint 位置），直接返回——系统空闲时不空转（`lastImportantAt` 就是 §3.2 里插入锁上顺手维护的那个值）；
3. **确定 REDO 点**（核心，xlog.c:7529-7602）：
   - **shutdown checkpoint**：持有全部插入锁（`WALInsertLockAcquireExclusive`）时读出当前插入位置 `CurrBytePos`，REDO 点 = 下一条记录的位置（xlog.c:7547）。因为停库时不会再有并发插入，checkpoint 记录本身就是 REDO 点，同时把共享的 `Insert->RedoRecPtr` 推进（xlog.c:7561）；
   - **在线 checkpoint**：插入一条特殊的 `XLOG_CHECKPOINT_REDO` 记录（xlog.c:7591-7593），**这条记录的起始 LSN 就是新的 REDO 点**。它的插入路径（`XLogInsertRecord` 的 `WALINSERT_SPECIAL_CHECKPOINT` 分支，xlog.c:925-942）持有全部插入锁并原子地更新 `Insert->RedoRecPtr = StartPos`——从这一刻起，所有后续插入的 FPW 判定都以新 REDO 点为准（§2.3），保证"REDO 点之后每页的首次修改必有镜像"这一恢复正确性的根基。PG 17 之前 REDO 点是一个"虚拟位置"，引入显式记录是为了 WAL summarizer/增量备份能在流里看到边界；
4. **等待悬空的提交**（xlog.c:7695-7712）：commit 分"写 commit 记录"和"更新 CLOG"两步（不同锁保护），若有事务恰好夹在中间（`delayChkptFlags` 标志），checkpoint 睡 10ms 轮询等它出临界区——否则可能出现"commit 记录在 REDO 点前、CLOG 更新却没被本次 checkpoint 刷盘"的窗口；
5. **刷盘**：`CheckPointGuts(checkPoint.redo, flags)`（xlog.c:7715），见 §5.2；
6. **写 checkpoint 记录并落盘**（xlog.c:7748-7754）：把 `CheckPoint` 结构体（含 redo 点、nextXid、oldestXid、NextOid、NextMulti 等事务系统水位）作为一条 `XLOG_CHECKPOINT_ONLINE/SHUTDOWN` 记录插入，然后 `XLogFlush(recptr)`；
7. **更新控制文件**（xlog.c:7788-7805）：`ControlFile->checkPoint = ProcLastRecPtr`（checkpoint 记录的位置）、`checkPointCopy = checkPoint`（整个结构体的副本），`UpdateControlFile()` 持久化。**只有这一步完成，checkpoint 才算生效**——之前崩溃的话，恢复仍从旧 checkpoint 开始，一切照常；
8. **回收旧 WAL**（xlog.c:7850-7854）：按新 REDO 点算出可删除的段号，`KeepLogSeg` 再考虑 `wal_keep_size`/复制槽的保留需求，然后删除或复用旧段文件；必要时使超限的复制槽失效（`InvalidateObsoleteReplicationSlots`）。

注意顺序的必然性：**先刷脏页 → 再写 checkpoint 记录 → 最后改 pg_control**。任何一步倒置都会造出"控制文件宣称可以从 REDO 点恢复，但 REDO 点之前的某个脏页其实没落盘"的数据丢失窗口。

### 5.2 CheckPointGuts 的刷盘顺序

`src/backend/access/transam/xlog.c:8049`：

```c
static void
CheckPointGuts(XLogRecPtr checkPointRedo, int flags)
{
    CheckPointRelationMap();            /* 关系映射文件（pg_filenode.map） */
    CheckPointReplicationSlots(flags & CHECKPOINT_IS_SHUTDOWN);
    CheckPointSnapBuild();              /* 逻辑解码的快照状态 */
    CheckPointLogicalRewriteHeap();
    CheckPointReplicationOrigin();
    /* Write out all dirty data in SLRUs and the main buffer pool */
    CheckPointCLOG();                   /* 事务提交状态位图 */
    CheckPointCommitTs();
    CheckPointSUBTRANS();
    CheckPointMultiXact();
    CheckPointPredicate();              /* SSI 谓词锁 */
    CheckPointBuffers(flags);           /* ★ 主缓冲池脏页——大头在这 */
    /* Perform all queued up fsyncs */
    ProcessSyncRequests();              /* ★ 集中执行所有攒下的 fsync */
    /* We deliberately delay 2PC checkpointing as long as possible */
    CheckPointTwoPhase(checkPointRedo);
}
```

顺序解读：先刷各种元状态（复制槽、逻辑解码），再刷 SLRU（CLOG 必须先于/伴随数据页持久化——不然重放的提交状态没有落点），然后 `CheckPointBuffers` 遍历整个缓冲池把 `BM_CHECKPOINT_NEEDED` 的脏页 `write()` 出去，最后 `ProcessSyncRequests` 统一 fsync 每个被写过的文件。write 与 fsync 分离，让操作系统有机会合并回写。

### 5.3 checkpointer 的节流：checkpoint_completion_target

一口气刷几十 GB 脏页会打爆磁盘。`CheckPointBuffers` 每写一页调一次 `CheckpointWriteDelay`（`src/backend/postmaster/checkpointer.c:795`），传入当前进度 `progress`（已写页数 / 总页数）：

```c
if (!(flags & CHECKPOINT_FAST) && ... && IsCheckpointOnSchedule(progress))
{
    ...
    AbsorbSyncRequests();       /* 顺手消化别的进程转来的 fsync 请求 */
    WaitLatch(MyLatch, ..., 100, ...);   /* 进度超前 ⇒ 睡 100ms */
}
```

判定"是否超前"的 `IsCheckpointOnSchedule`（checkpointer.c:865）：

```c
progress *= CheckPointCompletionTarget;        /* 目标：在周期的 target 比例内完成 */

/* ① 按 WAL 量比较 */
elapsed_xlogs = (((double) (recptr - ckpt_start_recptr)) /
                 wal_segment_size) / CheckPointSegments;
if (progress < elapsed_xlogs)
    return false;                              /* 落后于 WAL 增长速度 ⇒ 别睡了 */

/* ② 按时间比较 */
elapsed_time = (now - ckpt_start_time) / CheckPointTimeout;
if (progress < elapsed_time)
    return false;                              /* 落后于时间进度 ⇒ 别睡了 */

return true;                                   /* 超前 ⇒ 可以睡 */
```

两把尺子（WAL 生成量相对 `max_wal_size`、墙钟时间相对 `checkpoint_timeout`）取更紧的那把。`checkpoint_completion_target`（默认 0.9）把刷盘摊平到周期的 90%——这就是你在监控里看到 checkpoint 写入曲线平缓的原因。对照 InnoDB：等价物是 adaptive flushing（按 redo 生成速率调节刷脏页速率），思想一致。

### 小结

checkpoint = 定 REDO 点（在线时插一条 `XLOG_CHECKPOINT_REDO` 并原子推进共享 RedoRecPtr）→ 按序刷元状态/SLRU/缓冲池 → 写 checkpoint 记录并 fsync → 更新 pg_control → 回收旧 WAL。刷盘速率被 `IsCheckpointOnSchedule` 用 WAL 量和时间两把尺子节流到 `checkpoint_completion_target`。

---

## 6. 崩溃恢复：把历史重演一遍

### 6.1 总览与起点确定

启动时 postmaster 先派生 startup 进程执行 `StartupXLOG`（`src/backend/access/transam/xlog.c:5845`），主线：

```
StartupXLOG
 ├─ InitWalRecovery()            xlogrecovery.c:457   决定从哪开始、要不要归档恢复
 ├─ ... 初始化事务系统水位（nextXid 等来自 checkpoint 记录）
 ├─ PerformWalRecovery()         xlogrecovery.c:1612  ★ redo 主循环
 ├─ FinishWalRecovery()          xlogrecovery.c:1417  收尾：确定新 WAL 插入点
 ├─ PerformRecoveryXLogAction()  xlog.c:6785          end-of-recovery checkpoint 或记录
 └─ 打开只读/读写服务
```

**恢复起点**在 `InitWalRecovery` 中确定（无 backup_label 的普通崩溃场景，xlogrecovery.c:719-723）：

```c
CheckPointLoc = ControlFile->checkPoint;                  /* checkpoint 记录的 LSN */
CheckPointTLI = ControlFile->checkPointCopy.ThisTimeLineID;
RedoStartLSN = ControlFile->checkPointCopy.redo;          /* ★ redo 起点 */
record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc, ...);
```

`pg_control`（`global/pg_control`，可用 `pg_controldata` 查看）保存着最近一次成功 checkpoint 的记录位置及其内容副本 `checkPointCopy`——这就是 §5.1 第 7 步写入的东西。恢复从 `checkPointCopy.redo`（REDO 点）开始重放，而不是从 checkpoint 记录本身开始：在线 checkpoint 的 REDO 点在记录之前（刷脏页期间产生的 WAL 也要重放）。若存在 `backup_label`（基础备份恢复），起点改从 label 里的 checkpoint 取，防止用了备份过程中被推进的 pg_control。

### 6.2 PerformWalRecovery 主循环逐段走读

`src/backend/access/transam/xlogrecovery.c:1612`，删减后的骨架：

```c
/* ① 定位第一条要重放的记录 */
if (RedoStartLSN < CheckPointLoc)
{
    /* 在线 checkpoint：REDO 点在 checkpoint 记录之前，从 REDO 点读起 */
    XLogPrefetcherBeginRead(xlogprefetcher, RedoStartLSN);
    record = ReadRecord(xlogprefetcher, PANIC, false, replayTLI);
    /* 该位置必须是一条 XLOG_CHECKPOINT_REDO 记录，否则 FATAL */
    if (record->xl_rmid != RM_XLOG_ID ||
        (record->xl_info & ~XLR_INFO_MASK) != XLOG_CHECKPOINT_REDO)
        ereport(FATAL, ...);
}
else
    /* shutdown checkpoint：直接读 checkpoint 记录之后的下一条 */
    record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);

ereport(LOG, errmsg("redo starts at %X/%08X", ...));   /* 日志里熟悉的那行字 */

/* ② 主循环 */
do
{
    ProcessStartupProcInterrupts();               /* 处理信号 */
    if (recoveryPauseState != RECOVERY_NOT_PAUSED)
        recoveryPausesHere(false);                /* 热备暂停点 */
    if (recoveryStopsBefore(xlogreader))          /* PITR：目标在本条之前？ */
    {   reachedRecoveryTarget = true; break;  }

    ApplyWalRecord(xlogreader, record, &replayTLI);   /* ★ 重放一条 */

    if (recoveryStopsAfter(xlogreader))           /* PITR：目标在本条之后？ */
    {   reachedRecoveryTarget = true; break;  }
    record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);  /* 读下一条 */
} while (record != NULL);                         /* 读不到合法记录 = WAL 尽头 */
```

要点：
- 循环终止条件是 `record == NULL`——`ReadRecord` 在 CRC 失败、`xl_prev` 不匹配或文件缺失时返回 NULL，**WAL 的"末尾"就是第一条读不出的记录**（崩溃恢复模式下）；归档/流复制恢复模式下则会等待新 WAL 到来，变成"永不结束的恢复"（这正是备库的运行形态）；
- `recoveryStopsBefore/After` 检查 PITR 目标（时间戳/XID/LSN/命名恢复点），普通崩溃恢复恒为 false；
- 循环体外（xlogrecovery.c:1656）先调 `CheckRecoveryConsistency`，热备场景达到一致点后即可放开只读连接。

### 6.3 rm_redo 分发

`ApplyWalRecord`（xlogrecovery.c:1883）里真正的分发只有一行（xlogrecovery.c:1966）：

```c
GetRmgr(record->xl_rmid).rm_redo(xlogreader);
```

rmgr 表由 `src/include/access/rmgrlist.h` 生成，每行一个资源管理器：

```c
/* symbol name, textual name, redo, desc, identify, startup, cleanup, mask, decode */
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, ...)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, ...)     /* 提交/中止 → 更新 CLOG */
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, ...)            /* 堆的增删改 */
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, ...)         /* B-tree 页操作 */
```

`rmgrlist.h` 用 "X-macro" 技巧：同一份列表以不同的 `PG_RMGR` 宏定义多次展开，分别生成 rmgr 结构体数组、ID 枚举等。表中顺序决定 `xl_rmid` 数值，故只能尾部追加。`pg_waldump` 输出里的 "Heap/INSERT" 就是 textual name + desc 回调的产物。

### 6.4 页级幂等：LSN 比较的代码印证

redo 必须幂等——上次恢复可能中途崩溃，同一条记录会被重放两次。物理页级 redo 不能盲目重放（比如"把第 3 个槽位的元组删掉"重放两次就错了），PG 的方案是**页 LSN 比较**。每个 rmgr 的 redo 回调都通过统一入口 `XLogReadBufferForRedo`（`src/backend/access/transam/xlogutils.c:303` → `XLogReadBufferForRedoExtended`）取页：

```c
/* xlogutils.c:432（无整页镜像的分支） */
*buf = XLogReadBufferExtended(rlocator, forknum, blkno, mode, prefetch_buffer);
if (BufferIsValid(*buf))
{
    ...
    if (lsn <= PageGetLSN(BufferGetPage(*buf)))
        return BLK_DONE;          /* 页已 ≥ 本记录 ⇒ 之前重放过，跳过 */
    else
        return BLK_NEEDS_REDO;    /* 页比记录旧 ⇒ 需要重放 */
}
```

- `lsn` 是当前 WAL 记录的**结束位置**（与正常写路径 `PageSetLSN(page, recptr)` 用的值一致）。若页 LSN ≥ 它，说明页上已经包含了本记录（乃至更晚记录）的效果，返回 `BLK_DONE`，调用者跳过修改；
- 若块引用带 `BKPIMAGE_APPLY` 的整页镜像，则走另一分支（xlogutils.c:403）：`RestoreBlockImage` 直接用镜像**覆盖**页内容并 `PageSetLSN(page, lsn)`，返回 `BLK_RESTORED`——覆盖天然幂等，且不在乎页原来是否撕裂；
- rmgr 代码的标准形态因此是：

```c
if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
{
    /* 修改页内容 */
    PageSetLSN(page, lsn);       /* 重放后推进页 LSN，保证下次判 BLK_DONE */
    MarkBufferDirty(buffer);
}
```

这就是"LSN 是页的版本号"的完整闭环：写路径打版本，恢复路径比版本。

### 6.5 恢复结束后的处理

主循环退出后：
- `FinishWalRecovery`（xlogrecovery.c:1417）确定最后一条合法记录的位置，新的 WAL 插入将从那里继续（崩溃恢复总是恢复到 WAL 末尾）；
- `PerformRecoveryXLogAction`（xlog.c:6785）决定"如何在 WAL 里宣告恢复结束"：

```c
if (ArchiveRecoveryRequested && IsUnderPostmaster && PromoteIsTriggered())
{
    promoted = true;
    /* 备库提升：只写一条轻量的 XLOG_END_OF_RECOVERY 记录，不等 checkpoint */
    CreateEndOfRecoveryRecord();          /* xlog.c:7908 */
}
else
    /* 崩溃恢复：做一个真正的 end-of-recovery checkpoint，且等它完成 */
    RequestCheckpoint(CHECKPOINT_END_OF_RECOVERY |
                      CHECKPOINT_FAST | CHECKPOINT_WAIT);
```

两条路的取舍：崩溃恢复后必须先做一次 checkpoint（本质是 shutdown checkpoint，见 xlog.c:7415 注释）再开门——否则再次崩溃就要从头重放全部 WAL；而备库提升追求"秒级可写"，只写一条 `XLOG_END_OF_RECOVERY` 记录标记时间线切换，checkpoint 推迟到开门之后异步执行（xlog.c:6701-6702：`if (promoted) RequestCheckpoint(CHECKPOINT_FORCE)`）。

### 小结

恢复起点 = pg_control 里最近 checkpoint 的 REDO 点；主循环逐条读记录、按 `xl_rmid` 查 rmgrlist 分发给 rm_redo；幂等靠 `lsn <= PageGetLSN(page)` 则跳过（或整页镜像直接覆盖）；WAL 尽头由 CRC/xl_prev 校验失败界定；结束后崩溃恢复做同步 end-of-recovery checkpoint，提升场景用轻量 END_OF_RECOVERY 记录换取快速开门。

---

## 7. 归档、PITR 与 restartpoint 概述

### 7.1 归档

- 段文件写满（或 `XLOG_SWITCH`）后，`XLogArchiveNotify`（`src/backend/access/transam/xlogarchive.c:445`）在 `pg_wal/archive_status/` 下创建 `<segname>.ready` 标记文件；
- archiver 进程（`PgArchiverMain`，`src/backend/postmaster/pgarch.c:221`）的 `pgarch_ArchiverCopyLoop` 轮询 `.ready` 文件，逐个执行 `archive_command`（或 archive module 回调）拷走段文件，成功后改名 `.done`；
- 归档失败会无限重试，`.ready` 堆积 ⇒ `pg_wal` 膨胀——这是生产环境 `pg_wal` 撑爆磁盘的经典原因之一（另一个是废弃复制槽，见 §5.1 第 8 步）。

### 7.2 PITR（时间点恢复）

PITR = 基础备份 + 归档 WAL 重放到指定目标：
1. 恢复基础备份，配置 `restore_command`（从归档取段文件，实现在 `RestoreArchivedFile`，xlogarchive.c:55）；
2. 设置 `recovery_target_time/_xid/_lsn/_name` 之一；
3. 启动后走与崩溃恢复完全相同的 `PerformWalRecovery` 主循环，只是 WAL 来源换成归档，且 `recoveryStopsBefore/After`（§6.2）会在到达目标时停下；
4. 到达目标后按 `recovery_target_action` 暂停/提升，提升时开启**新时间线**（段文件名第一段 +1），并写 `.history` 文件记录分叉点——同一份归档可以反复恢复到不同时间点而互不污染。

崩溃恢复、归档恢复（PITR）、流复制备库三者共享同一套 xlogrecovery.c 代码，区别只是 WAL 从哪来、到哪停：这就是"备库 = 永不结束的崩溃恢复"的准确含义。

### 7.3 restartpoint

备库不能自己做 checkpoint（不能写 WAL），但同样需要"截断重放起点"防止每次重启都从头 redo。方案是 **restartpoint**：
- startup 进程每重放到一条主库的 checkpoint 记录，就把它存进共享内存（`RecoveryRestartPoint`，xlog.c:8089）；
- checkpointer 按自己的节奏（`checkpoint_timeout`，且受 `IsCheckpointOnSchedule` 同款节流）调用 `CreateRestartPoint`（xlog.c:8129）：刷脏页（复用 `CheckPointGuts`）、把 pg_control 的恢复起点推进到那条主库 checkpoint——**不写任何新 WAL 记录**；
- 效果：备库崩溃后从最近 restartpoint 重放，而不是从基础备份起点。

### 小结

归档 = `.ready`/`.done` 标记 + archiver 执行拷贝命令；PITR 复用恢复主循环，用 recovery target 提前停车并切新时间线；restartpoint 是备库版 checkpoint——只刷脏页和推进 pg_control，不产生 WAL。

---

## 自测问题

**1. 为什么 `XLogRecordAssemble` 用 `page_lsn <= RedoRecPtr` 判定是否写整页镜像？两个方向的错误各会导致什么？**

页 LSN ≤ REDO 点说明该页自最近 checkpoint 后未被改过，本次是首次修改；崩溃时它可能撕裂，而恢复恰好要从 REDO 点开始基于该页做增量重放，故必须记整页。若漏记（该记镜像时没记）：恢复可能基于撕裂页做增量重放，数据损坏。若多记（不该记时也记）：只损失 WAL 空间和写入带宽，正确性无损——所以 `XLogInsertRecord` 复核发现 REDO 点过期时宁可推倒重来，也绝不放过一个该记镜像的页（xloginsert.c:694、xlog.c:885-896）。

**2. 8 个 WALInsertLock 各自的 `insertingAt` 字段解决了什么问题？**

刷盘者要保证"目标范围内的插入都拷贝完毕"。若只能傻等所有持锁插入者结束，一是慢，二是可能死锁（插入者 A 为腾 WAL 缓冲区需要刷盘、从而等插入者 B，B 又在等 A，xlog.c:353-358）。`insertingAt` 公布每个插入者的进度水位，刷盘者可以忽略进度在目标之后的插入者，只等真正相关的人，同时打破循环等待。

**3. PostgreSQL 的恢复为什么没有 undo 阶段？"回滚免费"的代价由谁支付？**

no-overwrite 存储下，未提交事务的元组即使落盘也只是堆里一个 `xmin` 未提交的版本；CLOG 中该 XID 非 COMMITTED，MVCC 可见性会将其过滤，无需物理擦除。回滚只需（甚至不需要）把 CLOG 标成 ABORTED。代价转移给 VACUUM：这些"本该被 undo 清掉"的版本成为死元组，等待后台清理；长期不清理就是表膨胀。

**4. 在线 checkpoint 的 REDO 点为什么在 checkpoint 记录之前？恢复时如何找到并验证它？**

REDO 点在刷脏页**开始前**确定（master 上通过插入 `XLOG_CHECKPOINT_REDO` 记录并原子推进 `Insert->RedoRecPtr`，xlog.c:7579-7602）：刷盘期间系统继续产生 WAL，这些修改可能没赶上本轮刷盘，必须从刷盘前的位置开始重放才完整。checkpoint 记录在刷盘**结束后**才写入，自然位于 REDO 点之后。恢复时从 `pg_control->checkPointCopy.redo` 读起，`PerformWalRecovery` 校验该位置必须是一条 `XLOG_CHECKPOINT_REDO` 记录，否则 FATAL（xlogrecovery.c:1662-1678）。

**5. 同一段 WAL 被重放两次为什么不会破坏数据？分别说明有镜像和无镜像两种块引用。**

无镜像块：`XLogReadBufferForRedoExtended` 比较 `lsn <= PageGetLSN(page)`，页 LSN 已 ≥ 记录 LSN 则返回 `BLK_DONE` 跳过（xlogutils.c:444-447）；每次成功重放都会 `PageSetLSN` 推进页版本，保证第二次必然跳过。有镜像块（`BKPIMAGE_APPLY`）：`RestoreBlockImage` 直接用镜像覆盖整页再设 LSN（xlogutils.c:403-415），覆盖操作天然幂等。两者合起来实现了 ARIES 的 "repeating history" 而无需 per-page 的重放计数。
