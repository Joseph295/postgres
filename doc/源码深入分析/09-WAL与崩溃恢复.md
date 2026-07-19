# 第 09 篇 · WAL 与崩溃恢复：数据库可靠性的全部秘密

> 对应《PostgreSQL 源码学习指南》第 10 章。源码版本：本仓库 master 分支（20devel）。
> 文中所有 `文件:行号` 均已在本仓库核实；引用的代码片段为真实源码逐字摘录（省略处均注明原因）。
> v2 修订：新增 XLogInsertRecord / XLogFlush / CreateCheckPoint / PerformWalRecovery /
> XLogReadBufferForRedoExtended / heap_xlog_insert 的全函数走读，8 槽并行插入的正确性论证，
> 页撕裂场景推演，不变式清单，设计历史（git 考古），以及 5 个 what-if 故障推演。

## 本章导读

如果说 MVCC 是 PostgreSQL 的灵魂，WAL（Write-Ahead Log，预写式日志）就是它的心脏——写操作的持久性、崩溃恢复、物理复制、逻辑解码、PITR，全部构建在这一条只追加（append-only）的日志之上。用大数据的话说：**the log is the database**，WAL 之于 PostgreSQL，就像 commit log 之于 Kafka。

本章的推进顺序是"格式 → 写入 → 并发 → 落盘 → 抗撕裂 → 理论对照 → checkpoint → 恢复 → 归档"，最后以不变式清单、设计历史与故障推演收束：

1. §1 解剖 WAL 记录的物理格式（`xlogrecord.h` 逐字段）；
2. §2 跟着 `XLogBeginInsert → XLogRegisterBuffer → XLogInsert` 走完整条写入路径，重点是 `XLogRecordAssemble` 的 full-page-write 判定与 **`XLogInsertRecord` 全函数走读**（三类记录的分支、持锁复核重试、CRC 终算）；
3. §3 逐行分析 `ReserveXLogInsertLocation`——全库扩展性最敏感的几行代码——并**完整论证** 8 槽 WALInsertLock + 全局字节预留为什么能同时保证"LSN 全序"和"并行拷贝"（insertingAt 水位机制推演）；
4. §4 走读 `XLogFlush` 全函数（组提交的等待/接力协议）与 `XLogWrite` 主线；
5. §5 推演 8KB 页写到 4KB 扇区磁盘的撕裂场景，以及无 FPW 时 redo 如何产生静默损坏；对比 InnoDB doublewrite；
6. §6 与 ARIES 对照，从 no-overwrite 存储 + CLOG 推演 PG 为什么敢没有 undo；
7. §7 逐段走读 `CreateCheckPoint` 全函数，以及 `IsCheckpointOnSchedule` 摊平 I/O 的控制论逻辑；
8. §8 走读崩溃恢复主循环 `PerformWalRecovery`、通用取页入口 `XLogReadBufferForRedo`（四种返回值各给场景）和一个具体的 rmgr redo 函数 `heap_xlog_insert`；解释 end-of-recovery 为什么有两种收尾；
9. §9 归档、PITR 与 restartpoint；
10. §10 不变式清单（每条给违反后果）；§11 设计历史（git 考古）；§12 what-if 故障推演；最后自测 9 题。

## 前置知识

- **页（Page）**：PG 的表文件由 8KB 的页组成，页头第一个字段就是 `pd_lsn`（page LSN）。读过第 06 篇（存储管理与缓冲池）最好，没读过只需记住这一点。
- **缓冲池**：数据页的修改都发生在共享缓冲池里，脏页由 checkpointer/bgwriter 异步写盘。
- **事务提交**：`COMMIT` 时唯一必须同步等待磁盘的是 WAL（`RecordTransactionCommit` 里的 `XLogFlush(XactLastRecEnd)`，xact.c:1544），数据页可以慢慢刷。
- **critical section**：`START_CRIT_SECTION()` 内任何 ERROR 都会升级为 PANIC。"改页 + 写 WAL"必须在 critical section 内完成——改了页却没写成 WAL 的中间状态不允许存活。
- **对 MySQL 用户**：PG 的 WAL 大致对应 InnoDB 的 redo log（#innodb_redo），但 PG 没有独立的 undo log 和 binlog——WAL 一份日志承担了 redo + 物理复制 + 逻辑解码（CDC）三份职责。本章通篇穿插两者对照。

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

一条记录 = **固定头 + 若干"块引用" + 主数据**。"块引用"（block reference）是 9.5 重构后的核心抽象：记录声明"我修改了哪些页"，恢复逻辑据此做通用处理（读页、比较 LSN、还原整页镜像），rmgr 只需处理页内的具体改动。注释同时指出：`XLogRecord` 总是从 MAXALIGN 边界开始，但其后的字段不做对齐——WAL 是空间敏感的字节流。

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

| 字段 | 含义 | 关键点 |
|---|---|---|
| `xl_tot_len` | 整条记录总长度（含头） | 读取器靠它跳到下一条记录；上限 `XLogRecordMaxSize`（1020MB，xlogrecord.h:74，为 XLogReader 的单块分配留 4MB 余量） |
| `xl_xid` | 产生这条记录的事务 XID | 逻辑解码按它把记录归到事务；无 XID 的操作（如 checkpoint）为 0 |
| `xl_prev` | **前一条记录的起始 LSN** | 反向链指针。这是 WAL 防"半截旧日志被误当新日志"的关键校验之一：段文件是复用旧文件改名而来的（`RemoveOldXlogFiles` 回收），文件尾部可能残留上个周期的旧记录，其 CRC 完全合法——但它的 `xl_prev` 指向的是旧时间轴的位置，对不上就说明日志到此为止 |
| `xl_info` | 8 个标志位 | 高 4 位归 rmgr 自由使用（`XLR_RMGR_INFO_MASK 0xF0`，如 heap 用它区分 INSERT/UPDATE/DELETE），低 4 位归 XLogInsert 内部使用（`XLR_INFO_MASK 0x0F`，xlogrecord.h:62-63） |
| `xl_rmid` | 资源管理器 ID | 恢复时按它分发给 `heap_redo`/`btree_redo` 等回调（§8.3） |
| `xl_crc` | CRC32C 校验和 | 覆盖除头外的全部内容 + 头的前半部分（`xl_crc` 之前的字段，见 §2.4 第 ⑥ 步）。恢复时靠 CRC 判断"日志写到哪里就断了"——**WAL 的结尾不是靠文件长度，而是靠第一条校验失败的记录** |

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

- `id`：块引用编号（0 ~ `XLR_MAX_BLOCK_ID`=32，xlogrecord.h:241）。特殊 id 255/254 表示主数据（短/长格式头，`XLogRecordDataHeaderShort/Long`，xlogrecord.h:213/221），253 是复制源（origin），252 是顶层事务 XID；
- `fork_flags`：低 4 位是 fork 号（main/FSM/VM/init），高 4 位是标志（xlogrecord.h:198-201）：
  - `BKPBLOCK_HAS_IMAGE 0x10`——本块带整页镜像（full-page image，FPI）；
  - `BKPBLOCK_HAS_DATA 0x20`——本块带 rmgr 自定义数据；
  - `BKPBLOCK_WILL_INIT 0x40`——重放时会重建整页（比如新页插入），因此**不需要**先读旧页；
  - `BKPBLOCK_SAME_REL 0x80`——与上一个块引用同表，省掉 RelFileLocator 的存储（空间优化）。
- 带镜像时后跟 `XLogRecordBlockImageHeader`（xlogrecord.h:141）：记录镜像长度、"空洞"位置（`hole_offset`）与标志位（xlogrecord.h:157-163）：`BKPIMAGE_HAS_HOLE`（页中部 `pd_lower`~`pd_upper` 的全零空洞被抠掉）、`BKPIMAGE_APPLY`（重放时是否用镜像覆盖页，§2.3）、`BKPIMAGE_COMPRESS_PGLZ/LZ4/ZSTD`（`wal_compression` 的三种算法，§11）。

### 1.4 LSN 的本质：WAL 流中的字节偏移

`src/include/access/xlogdefs.h:21`：

```c
typedef uint64 XLogRecPtr;
```

LSN（Log Sequence Number，类型名 `XLogRecPtr`）就是一个 **64 位无符号整数，表示该位置在整条 WAL 逻辑字节流中的偏移量**。没有任何神秘结构：LSN 相减就是两点之间的 WAL 字节数（监控里 `pg_wal_lsn_diff()` 干的就是这个）。习惯上显示为 `高32位/低32位` 的十六进制，如 `0/1699A28`。

三个身份合一：
1. **地址**：给定 LSN，可以唯一定位到某个 WAL 段文件的某个偏移；
2. **版本号**：每个数据页头的 `pd_lsn` 记录"最后修改此页的 WAL 记录结束位置"，恢复时用它做幂等判断（§8.4）；
3. **时钟**：LSN 单调递增，是整个集群的逻辑时间轴——复制延迟、恢复进度都用 LSN 差衡量。

与 InnoDB 对照：InnoDB 的 LSN 同样是 redo 字节流偏移（8.0 起去掉了 512B block 头的"物理偏移≠逻辑偏移"折算），两家在这一点上完全趋同。

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

- **时间线（TimeLine ID）**：每次 PITR 恢复或备库提升都会开出一条新时间线，防止"平行宇宙"的 WAL 互相覆盖（§9.2）；
- 后两段合起来就是段序号 `XLogSegNo`（= LSN / 段大小）。历史上 WAL 曾按 4GB 一个 "log file" 组织，所以拆成两段显示。**注意第三段不会数到 FF**：16MB 段时每个 "log id" 只有 0x00~0xFF 共 256 个段（`XLogSegmentsPerXLogId`，xlog_internal.h:99）。
- `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)`（xlog_internal.h:116）完成 LSN → 段号换算，就是一个整除。

### 1.6 WAL 页头与 contrecord

WAL 流内部还按 8KB（`XLOG_BLCKSZ`）分页，每页开头有页头（段首页是 long 头 `SizeOfXLogLongPHD`，含段大小、系统标识；其余是 short 头）。一条记录跨页时，后续页头带 `XLP_FIRST_IS_CONTRECORD` 标志和 `xlp_rem_len`（还剩多少字节属于上一条记录）——§3.4 会看到写入侧怎么设置它，§12.1 会看到它在"半条记录"故障里的角色。

### 小结

WAL 记录 = 固定头（含 CRC、反向链指针、rmgr 分发标签）+ 自描述的块引用列表 + rmgr 私有数据。LSN 是 WAL 字节流的 64 位偏移量，同时充当页版本号和集群逻辑时钟。段文件名 = 时间线 + LSN 高位 + 段号，全部是十六进制算术，没有魔法。WAL 自身也分页，跨页记录靠 contrecord 标志续接。

---

## 2. WAL 写入路径：从 heap_insert 到日志缓冲区

### 2.1 三步 API 与"先改页后写日志"的机器检查

任何模块写 WAL 都是固定三步。看真实的 `heap_insert`（`src/backend/access/heap/heapam.c:2151-2183`，节选）：

```c
XLogBeginInsert();
XLogRegisterData(&xlrec, SizeOfHeapInsert);
...
XLogRegisterBuffer(HEAP_INSERT_BLKREF_HEAP, buffer,
                   REGBUF_STANDARD | bufflags);
XLogRegisterBufData(HEAP_INSERT_BLKREF_HEAP, &xlhdr, SizeOfHeapHeader);
XLogRegisterBufData(HEAP_INSERT_BLKREF_HEAP,
                    (char *) heaptup->t_data + SizeofHeapTupleHeader,
                    heaptup->t_len - SizeofHeapTupleHeader);
...
recptr = XLogInsert(RM_HEAP_ID, info);
PageSetLSN(page, recptr);
```

这段代码严格遵循 `src/backend/access/transam/README:437` 的"WAL-logged action 通用模式"：先钉住并排他锁页 → `START_CRIT_SECTION()` → 改页 → `MarkBufferDirty()` → 组装并 `XLogInsert()` → `PageSetLSN()` → `END_CRIT_SECTION()` → 放锁。

**`XLogBeginInsert`**（`src/backend/access/transam/xloginsert.c:153`）只做状态断言：上一条记录已复位、当前不在恢复模式（恢复期间只读 WAL 不写 WAL）、没有重复调用。真正的登记数据放在进程本地的静态数组 `registered_buffers[]` / `rdatas[]` 里——**组装阶段完全无锁**，这是设计要点：贵的内存拷贝和判断都在拿锁之前做完。

**`XLogRegisterBuffer`**（xloginsert.c:246）把一个被修改的缓冲区登记为块引用：记下页的物理坐标（`BufferGetTag`）、页指针（供做整页镜像）和 flags。函数开头的断言（xloginsert.c:263-270）是 README 规则的机器检查：

```c
#ifdef USE_ASSERT_CHECKING
	if (!(flags & REGBUF_NO_CHANGE))
	{
		Assert(BufferIsDirty(buffer));
		Assert(BufferIsLockedByMeInMode(buffer, BUFFER_LOCK_EXCLUSIVE) ||
			   BufferIsLockedByMeInMode(buffer, BUFFER_LOCK_SHARE_EXCLUSIVE));
	}
#endif
```

**登记时页必须已经脏、已经被本进程锁住**。flags 里最常用的：
- `REGBUF_STANDARD`：标准页布局，做镜像时可抠掉空洞；
- `REGBUF_WILL_INIT`：重放会重建页，无需镜像（`heap_insert` 在"页上第一条且唯一一条元组"时设置它，heapam.c:2121-2126）；
- `REGBUF_FORCE_IMAGE` / `REGBUF_NO_IMAGE`：强制/禁止整页镜像；
- `REGBUF_KEEP_DATA`：即使做了整页镜像也保留块数据（逻辑解码需要原始元组，heapam.c:2141-2149）。

### 2.2 XLogInsert：乐观重试的外层循环

`src/backend/access/transam/xloginsert.c:482`，主体是一个 do-while（xloginsert.c:512-535，逐字摘录）：

```c
	do
	{
		XLogRecPtr	RedoRecPtr;
		bool		doPageWrites;
		bool		topxid_included = false;
		XLogRecPtr	fpw_lsn;
		XLogRecData *rdt;
		int			num_fpi = 0;
		uint64		fpi_bytes = 0;

		/*
		 * Get values needed to decide whether to do full-page writes. Since
		 * we don't yet have an insertion lock, these could change under us,
		 * but XLogInsertRecord will recheck them once it has a lock.
		 */
		GetFullPageWriteInfo(&RedoRecPtr, &doPageWrites);

		rdt = XLogRecordAssemble(rmid, info, RedoRecPtr, doPageWrites,
								 &fpw_lsn, &num_fpi, &fpi_bytes,
								 &topxid_included);

		EndPos = XLogInsertRecord(rdt, fpw_lsn, curinsert_flags, num_fpi,
								  fpi_bytes, topxid_included);
	} while (!XLogRecPtrIsValid(EndPos));
```

- **组装/插入分离 + 乐观重试**：`XLogRecordAssemble` 在无锁状态下把记录组装成 `XLogRecData` 链，组装依赖的 `RedoRecPtr`（最近一次 checkpoint 的 REDO 点）可能在组装期间被新 checkpoint 推进。`XLogInsertRecord` 拿到插入锁后复核，发现过期就返回 `InvalidXLogRecPtr`，外层循环重新组装。因为 checkpoint 很少发生，绝大多数插入一次成功——典型的乐观并发控制；
- 返回值 `EndPos` 是**记录末尾的 LSN**，调用者将它写入页头（`PageSetLSN`）。函数头注释（xloginsert.c:477-479）："LSN is the XLOG point up to which the XLOG must be flushed to disk before the data page can be written out. This implements the basic WAL rule 'write the log before the data'."

### 2.3 XLogRecordAssemble 的 full-page-write 判定（逐行分析）

`src/backend/access/transam/xloginsert.c:620` 的块引用循环里（xloginsert.c:678-700，逐字摘录）：

```c
		/* Determine if this block needs to be backed up */
		if (regbuf->flags & REGBUF_FORCE_IMAGE)
			needs_backup = true;
		else if (regbuf->flags & REGBUF_NO_IMAGE)
			needs_backup = false;
		else if (!doPageWrites)
			needs_backup = false;
		else
		{
			/*
			 * We assume page LSN is first data on *every* page that can be
			 * passed to XLogInsert, whether it has the standard page layout
			 * or not.
			 */
			XLogRecPtr	page_lsn = PageGetLSN(regbuf->page);

			needs_backup = (page_lsn <= RedoRecPtr);
			if (!needs_backup)
			{
				if (!XLogRecPtrIsValid(*fpw_lsn) || page_lsn < *fpw_lsn)
					*fpw_lsn = page_lsn;
			}
		}
```

`needs_backup = (page_lsn <= RedoRecPtr)` 这一行是 PostgreSQL 防"页撕裂"（torn page）的全部逻辑：

- **`page_lsn <= RedoRecPtr` 意味着什么？** 页头 LSN 是上一次修改本页的 WAL 位置。若它 ≤ 当前 REDO 点，说明**自最近一次 checkpoint 以来本页尚未被修改过**——本次是"checkpoint 后第一次改这个页"。此时必须把整页写进 WAL；反之（页 LSN > REDO 点），checkpoint 之后已经有某条 WAL 记录给它做过镜像了，本次只记增量即可。
- **为什么是"首改必镜像"？** 崩溃恢复从 REDO 点开始重放，重放的前提是"页的当前内容是某个一致状态"。checkpoint 保证 REDO 点之前的修改都已完好落盘，所以 REDO 点后第一次修改时页在磁盘上是完好的旧版本或（若崩溃时写了一半）撕裂版本——镜像让恢复不依赖磁盘上页的任何内容（§5 详细推演）。每个 checkpoint 周期只需为首次修改付出整页代价。
- **`fpw_lsn` 的用途**：对没做镜像的页，记住其中最小的页 LSN，交给 `XLogInsertRecord` 复核（§2.4 第 ② 步）。

组装的其余部分（xloginsert.c:702-800）：`needs_data = !needs_backup`（有镜像时增量数据可省，除非 `REGBUF_KEEP_DATA`）；需要镜像时按 `REGBUF_STANDARD` 计算空洞（取 `pd_lower`~`pd_upper`，xloginsert.c:732-751）、`wal_compression` 时尝试压缩（xloginsert.c:762-769）；关键的一处（xloginsert.c:721）：

```c
		include_image = needs_backup || (info & XLR_CHECK_CONSISTENCY) != 0;
```

以及（xloginsert.c:794-795）：

```c
			if (needs_backup)
				bimg.bimg_info |= BKPIMAGE_APPLY;
```

**只有真正因 FPW 而做的镜像才带 `BKPIMAGE_APPLY`（重放时覆盖页）**；因 `wal_consistency_checking` 而附带的镜像只用于恢复后对比校验，不覆盖。最后计算数据部分的 CRC、填好 `XLogRecord` 头——但 `xl_prev` 留空（组装时不知道前一条记录在哪，要等预留空间时才知道），头部 CRC 也留到插入时终算。

### 2.4 XLogInsertRecord 全函数走读

`src/backend/access/transam/xlog.c:784`。这是 WAL 子系统的正门，全函数按序走读（`WAL_DEBUG` 调试段 xlog.c:1043-1107 省略——仅在编译期开启 `WAL_DEBUG` 时把每条记录 decode 后打日志，与主线逻辑无关）：

**① 记录分类（xlog.c:802-809）**：

```c
	/* Does this record type require special handling? */
	if (unlikely(rechdr->xl_rmid == RM_XLOG_ID))
	{
		if (info == XLOG_SWITCH)
			class = WALINSERT_SPECIAL_SWITCH;
		else if (info == XLOG_CHECKPOINT_REDO)
			class = WALINSERT_SPECIAL_CHECKPOINT;
	}
```

三类记录三种协议：普通记录（拿 1 个插入锁）、段切换记录（拿全部 8 个锁，独占到段尾）、checkpoint REDO 记录（拿全部 8 个锁，原子推进 RedoRecPtr）。接着断言记录头完整、`XLogInsertAllowed()`（恢复期间禁止写 WAL，违者 ERROR）。

**② 普通记录：持锁复核 FPW 判定（xlog.c:856-896）**。`START_CRIT_SECTION()` 后进入正题：

```c
	if (likely(class == WALINSERT_NORMAL))
	{
		WALInsertLockAcquire();
		...
		if (RedoRecPtr != Insert->RedoRecPtr)
		{
			Assert(RedoRecPtr < Insert->RedoRecPtr);
			RedoRecPtr = Insert->RedoRecPtr;
		}
		doPageWrites = (Insert->fullPageWrites || Insert->runningBackups > 0);

		if (doPageWrites &&
			(!prevDoPageWrites ||
			 (XLogRecPtrIsValid(fpw_lsn) && fpw_lsn <= RedoRecPtr)))
		{
			/*
			 * Oops, some buffer now needs to be backed up that the caller
			 * didn't back up.  Start over.
			 */
			WALInsertLockRelease();
			END_CRIT_SECTION();
			return InvalidXLogRecPtr;
		}
```

为什么持有**任意一个**插入锁就能安全读 `Insert->RedoRecPtr`？因为推进 RedoRecPtr 的那一方（checkpoint）必须持有**全部** 8 个锁（下面第 ④ 步与 §7.1）——这是经典的"读者持一把、写者持全部"的不对称锁协议。复核逻辑：`fpw_lsn <= RedoRecPtr` 表示"存在一个组装时没做镜像的页，按新 REDO 点其实需要镜像"——组装结果作废，返回 Invalid 让 §2.2 的外层循环重来。注意不对称性：**宁可推倒重来，绝不放过一个该做镜像的页**（漏镜像 = 恢复可能损坏；多镜像 = 只浪费空间）。反方向（doPageWrites 刚被关掉）则不强制重组——带着多余的镜像插入无损正确性（xlog.c:872-876 注释）。

**③ 预留空间（xlog.c:898-906）**：

```c
		/*
		 * Reserve space for the record in the WAL. This also sets the xl_prev
		 * pointer.
		 */
		ReserveXLogInsertLocation(rechdr->xl_tot_len, &StartPos, &EndPos,
								  &rechdr->xl_prev);

		/* Normal records are always inserted. */
		inserted = true;
```

§3.1 逐行分析这个函数。从此刻起 `[StartPos, EndPos)` 这段 WAL 地址空间归本进程所有。

**④ 两类特殊记录（xlog.c:908-942）**。`XLOG_SWITCH` 拿全部锁后调 `ReserveXLogSwitch`（xlog.c:1204：若当前已在段首则什么都不做返回 false，否则把**段内剩余全部空间**都预留给自己）；`XLOG_CHECKPOINT_REDO` 则是（xlog.c:925-942）：

```c
	else
	{
		Assert(class == WALINSERT_SPECIAL_CHECKPOINT);
		...
		Assert(!XLogRecPtrIsValid(fpw_lsn));
		WALInsertLockAcquireExclusive();
		ReserveXLogInsertLocation(rechdr->xl_tot_len, &StartPos, &EndPos,
								  &rechdr->xl_prev);
		RedoRecPtr = Insert->RedoRecPtr = StartPos;
		inserted = true;
	}
```

持全部锁 + 预留 + **把共享 RedoRecPtr 原子推进到本记录起点**，三件事在一个临界区里完成。这保证了一个不变式：**新 REDO 点生效的瞬间，不存在"按旧 REDO 点判定、却排在新 REDO 点之后"的在途记录**——所有正在插入的记录要么已持锁完成判定（排在 REDO 点之前），要么将在②的复核中被打回重组。

**⑤ CRC 终算与拷贝（xlog.c:944-961）**：

```c
	if (inserted)
	{
		/*
		 * Now that xl_prev has been filled in, calculate CRC of the record
		 * header.
		 */
		rdata_crc = rechdr->xl_crc;
		COMP_CRC32C(rdata_crc, rechdr, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(rdata_crc);
		rechdr->xl_crc = rdata_crc;

		/*
		 * All the record data, including the header, is now ready to be
		 * inserted. Copy the record in the space reserved.
		 */
		CopyXLogRecordToWAL(rechdr->xl_tot_len,
							class == WALINSERT_SPECIAL_SWITCH, rdata,
							StartPos, EndPos, insertTLI);
```

CRC 分两段算的原因在此显形：数据部分的 CRC 在组装时（无锁）已算好存在 `xl_crc` 里，此处只需把头部（含刚填上的 `xl_prev`）续进去——持锁路径上少算一整条记录的 CRC。`CopyXLogRecordToWAL` 把记录拷进 WAL 缓冲区（§3.4），**这一步多个进程完全并行**。

**⑥ 顺手记账（xlog.c:963-973）**：非 `XLOG_MARK_UNIMPORTANT` 的记录把 `StartPos` 写进本槽位的 `lastImportantAt`——零额外锁开销地维护"最后一条重要 WAL 的位置"，供 §7.1 的"空闲跳过 checkpoint"使用。

**⑦ 放锁与收尾（xlog.c:987-1011）**：`WALInsertLockRelease()`（会把 `insertingAt` 清零，§3.2）→ `END_CRIT_SECTION()` → 若记录跨了 WAL 页边界，更新共享的 `LogwrtRqst.Write`（提示后台该写盘了）。`XLOG_SWITCH` 还要同步 `XLogFlush(EndPos)` 把整段刷掉并触发归档（xlog.c:1018-1041）。最后（xlog.c:1112-1127）更新 `ProcLastRecPtr = StartPos`、`XactLastRecEnd = EndPos`（提交时 `XLogFlush` 用的就是它）并记 `pgWalUsage` 统计，返回 `EndPos`。

### 小结

写 WAL 是"无锁组装 + 短临界区插入"的两段式。FPW 判定就一行 `page_lsn <= RedoRecPtr`——checkpoint 后首改记整页，否则记增量；配合插入时的持锁复核 + 乐观重试解决与 checkpoint 的竞争，且方向不对称：宁可重来不可漏记。`XLogInsertRecord` 对三类记录用三种锁协议，CRC 拆成"无锁算数据 + 持锁算头"，`lastImportantAt` 顺手记账。

---

## 3. LSN 定序与并行拷贝：全库扩展性最敏感的几行代码

### 3.1 ReserveXLogInsertLocation 逐行

`src/backend/access/transam/xlog.c:1149`。先看函数头注释（xlog.c:1136-1139）给它的定性："**This is the performance critical part of XLogInsert that must be serialized across backends.** The rest can happen mostly in parallel. Try to keep this section as short as possible, insertpos_lck can be heavily contended on a busy system." 全函数逐字：

```c
static pg_always_inline void
ReserveXLogInsertLocation(int size, XLogRecPtr *StartPos, XLogRecPtr *EndPos,
						  XLogRecPtr *PrevPtr)
{
	XLogCtlInsert *Insert = &XLogCtl->Insert;
	uint64		startbytepos;
	uint64		endbytepos;
	uint64		prevbytepos;

	size = MAXALIGN(size);

	/* All (non xlog-switch) records should contain data. */
	Assert(size > SizeOfXLogRecord);

	/*
	 * The duration the spinlock needs to be held is minimized by minimizing
	 * the calculations that have to be done while holding the lock. The
	 * current tip of reserved WAL is kept in CurrBytePos, as a byte position
	 * that only counts "usable" bytes in WAL, that is, it excludes all WAL
	 * page headers. The mapping between "usable" byte positions and physical
	 * positions (XLogRecPtrs) can be done outside the locked region, and
	 * because the usable byte position doesn't include any headers, reserving
	 * X bytes from WAL is almost as simple as "CurrBytePos += X".
	 */
	SpinLockAcquire(&Insert->insertpos_lck);

	startbytepos = Insert->CurrBytePos;
	endbytepos = startbytepos + size;
	prevbytepos = Insert->PrevBytePos;
	Insert->CurrBytePos = endbytepos;
	Insert->PrevBytePos = startbytepos;

	SpinLockRelease(&Insert->insertpos_lck);

	*StartPos = XLogBytePosToRecPtr(startbytepos);
	*EndPos = XLogBytePosToEndRecPtr(endbytepos);
	*PrevPtr = XLogBytePosToRecPtr(prevbytepos);

	/*
	 * Check that the conversions between "usable byte positions" and
	 * XLogRecPtrs work consistently in both directions.
	 */
	Assert(XLogRecPtrToBytePos(*StartPos) == startbytepos);
	Assert(XLogRecPtrToBytePos(*EndPos) == endbytepos);
	Assert(XLogRecPtrToBytePos(*PrevPtr) == prevbytepos);
}
```

逐行解读，以及为什么说这是**全库扩展性最敏感的代码**：

- `size = MAXALIGN(size)`：对齐在锁外做。每条记录起点都落在 MAXALIGN 边界（§1.1），这样预留量恒为对齐值，锁内不用再算；
- **锁内只有五次赋值**：读 `CurrBytePos`、加法、读 `PrevBytePos`、写 `CurrBytePos`、写 `PrevBytePos`。系统里**每一条** WAL 记录——每次 INSERT/UPDATE/DELETE/COMMIT——都必须串行通过这几行。假设锁内多花 20ns，一台每秒产生 200 万条 WAL 记录的机器就多烧 4% 的墙钟时间在这个临界区上，而且是完全无法并行摊薄的串行部分（Amdahl 定律的分母）。这就是注释反复强调"keep this section as short as possible"的原因；
- **`CurrBytePos` 为什么不直接存 LSN？** LSN 是物理偏移，包含每 8KB 一个的页头和每段一个的长页头。若锁内维护 LSN，"预留 size 字节"就得判断"这段范围里跨了几个页边界、每个页头多大"——一串除法和分支。改存"扣除所有页头的可用字节位置"（usable byte position）后，预留退化为纯加法；**usable↔物理的换算（`XLogBytePosToRecPtr`，xlog.c:1899，本质是对 `UsableBytesInSegment` 的除法和取模）搬到锁外**，由每个进程并行地做。这是"把串行区里的计算搬到并行区"的教科书式手法；
- **`xl_prev` 反向链免费生成**：`PrevBytePos` 在同一临界区里顺手维护，每条记录天然知道前驱的起点，无需任何额外同步。这也解释了为什么 `xl_prev` 的填写（§2.4 第 ③ 步）必须在预留之后；
- 为什么用自旋锁而不是原子 CAS？因为要**成对更新两个变量**（CurrBytePos 和 PrevBytePos），单个 64 位 CAS 装不下。开发者邮件列表上多次讨论过用 128 位 CAS 或拆开 PrevBytePos 的方案（后者会丢掉 xl_prev 校验），至今维持自旋锁——它已经短到不是瓶颈的瓶颈。

### 3.2 正确性论证：为什么"全局字节预留 + 8 槽并行拷贝"既保证 LSN 全序又保证并行

这是本章第一处"所以然"。先陈述要同时满足的两个目标，再给出机制并论证。

**目标 A（LSN 全序）**：任意两条 WAL 记录在字节流中的位置不重叠、不留洞，且 `xl_prev` 链完整——恢复时才能把日志当作单一线性历史重放。
**目标 B（并行拷贝）**：多个后端可以同时把各自的记录拷进 WAL 缓冲区，不互相等待——否则 WAL 成为全局写入瓶颈。

**机制**：
1. 定序由 §3.1 的**单点串行预留**完成：自旋锁下 `CurrBytePos += size`。串行段只有几纳秒，之后 `[StartPos, EndPos)` 的所有权唯一。目标 A 由此直接成立——地址空间的分配本身是全序的，`xl_prev` 也在同一临界区生成；
2. 拷贝完全并行：每个插入者持 8 个 `WALInsertLock`（`NUM_XLOGINSERT_LOCKS`，xlog.c:157）中的**任意一个**，往自己的预留区间 memcpy。区间互不重叠，所以拷贝之间无需任何同步。

**但这制造了一个新问题**：预留是全序的，**完成是乱序的**。进程 P1 预留了 [100,200)，P2 预留了 [200,300)，P2 可能先拷完。此刻字节流上是一个"洞"：[200,300) 已就绪，[100,200) 还在拷。**刷盘者（XLogWrite）绝不能把带洞的前缀写到磁盘**——假如 [100,200) 所在页被写盘时还是半成品，而崩溃恰好发生在其余部分补齐之前，磁盘上就有一段"CRC 恰好可能碰对"的垃圾（概率极低但非零），更实际的是这违反了"Flush 指针之前的字节全部有效"的基本假设。

所以刷盘者需要回答：**连续就绪的前缀到哪里？** 这就是每把插入锁上那个 `insertingAt` 水位的用途。结构定义（xlog.c:374-379）：

```c
typedef struct
{
	LWLock		lock;
	pg_atomic_uint64 insertingAt;
	XLogRecPtr	lastImportantAt;
} WALInsertLock;
```

外层 `union WALInsertLockPadded`（xlog.c:388-392）把每个槽填充到整缓存行（`PG_CACHE_LINE_SIZE`），防止 8 个热点变量互相伪共享。

**水位协议**（结构体上方注释 xlog.c:338-373 + 代码印证）：
- 拿到锁时 `insertingAt = 0`（`InvalidXLogRecPtr`），含义是"我在插入，但不知道插到哪了，我的进度**可能落后于任何位置**"；
- 放锁时 `LWLockReleaseClearVar(..., 0)`（xlog.c:1501-1503）复位为 0；
- 插入过程中**只有在需要长时间停留时**才更新它：小记录拷完就放锁，一次原子写都不做（xlog.c:360-363 注释）；只有 `GetXLogBuffer` 发现目标 WAL 页还没初始化、需要调 `AdvanceXLInsertBuffer` 腾缓冲区（可能要等 I/O）时，才先 `WALInsertLockUpdateInsertingAt(CurrPos)` 公布进度（xlog.c:1743-1745）。

**刷盘者的推演**（`WaitXLogInsertionsToFinish`，xlog.c:1545，见 §3.3）：设刷盘目标为 `upto`。
1. 读全局 `CurrBytePos` 得 `reservedUpto`——预留头，任何已开始的插入都在它之前；
2. 把候选答案 `finishedUpto` 初始化为 `reservedUpto`，然后遍历 8 把锁：
   - 锁空闲 ⇒ 该槽没有在途插入，不构成约束；
   - 锁被持有且 `insertingAt ≥ upto` ⇒ 这个插入者的工作区间整体在目标之后（预留是全序的：它起点都 ≥ insertingAt ≥ upto），**与本次刷盘无关，直接忽略**；
   - 锁被持有且 `insertingAt < upto`（含 0 的情况）⇒ 它可能正在写目标区间内的字节，**必须等**：`LWLockWaitForVar` 睡到该锁被释放或 `insertingAt` 被推进；
   - 等到的 `insertingAt` 值如果比当前 `finishedUpto` 小，就把 `finishedUpto` 回退到它。
3. 循环结束时，`finishedUpto` 之前不存在任何在途插入 ⇒ **[0, finishedUpto) 是连续就绪前缀**，可以安全写盘。

**为什么这不只是优化，而是死锁预防**（xlog.c:353-358 注释）：WAL 缓冲区是环形的。插入者 A 持锁写到缓冲区尽头，需要淘汰一个旧页——淘汰要求先把旧页刷盘——刷盘要求等所有相关在途插入结束。如果"等"意味着等**所有**持锁者，而持锁者 B 恰好也在等缓冲区腾空（等 A），就形成循环等待。`insertingAt` 打破环路：A 在睡眠前公布了自己的进度，B 的刷盘只要目标在 A 的进度之前就不必等 A；反之 A 等的旧页一定在所有人进度之前，谁也不会反过来等 A 还没写的部分。**发布进度水位，把"等人"细化为"等特定字节范围的人"，环就成不了**。

**正确性小结**：全序由单点预留保证（串行段被压缩到 5 次赋值）；并行由区间所有权唯一保证（拷贝无需同步）；乱序完成与顺序刷盘的矛盾由 insertingAt 水位调和（刷盘者能算出连续前缀）；水位的"惰性更新"（只在睡眠前更新）把常态路径的开销降到零，同时恰好覆盖了死锁场景。四者合起来，就是 9.4 那次让 WAL 插入吞吐翻倍的重构（§11 commit 9a20a9b21b）。

**与 InnoDB 对照**：MySQL 8.0 重构后的 redo log 用"原子加预留 LSN + 并行拷贝 + link_buf 收敛写入点"。link_buf 是一个环形位图，每个完成拷贝的事务在自己区间对应的位上打标记，后台线程扫描连续标记段推进 `buf_ready_for_write_lsn`——与 PG 的差别在于：InnoDB 用**空间换精度**（每个完成事件都记录，写入点推进最紧凑），PG 用 **8 个水位粗粒度近似**（只有 8 把锁要检查，但可能保守——某个慢插入者会把 finishedUpto 拖回它的水位）。两者殊途同归：把"定序"压缩到极小的原子操作，把"搬数据"完全并行化。

### 3.3 WaitXLogInsertionsToFinish 走读

`src/backend/access/transam/xlog.c:1545`。骨架（省略注释，逻辑完整）：

```c
	inserted = pg_atomic_read_membarrier_u64(&XLogCtl->logInsertResult);
	if (upto <= inserted)
		return inserted;                    /* ① 快路径：已知连续前缀足够 */

	SpinLockAcquire(&Insert->insertpos_lck);
	bytepos = Insert->CurrBytePos;
	SpinLockRelease(&Insert->insertpos_lck);
	reservedUpto = XLogBytePosToEndRecPtr(bytepos);
	...
	finishedUpto = reservedUpto;
	for (i = 0; i < NUM_XLOGINSERT_LOCKS; i++)
	{
		XLogRecPtr	insertingat = InvalidXLogRecPtr;
		do
		{
			if (LWLockWaitForVar(&WALInsertLocks[i].l.lock,
								 &WALInsertLocks[i].l.insertingAt,
								 insertingat, &insertingat))
			{
				/* the lock was free, so no insertion in progress */
				insertingat = InvalidXLogRecPtr;
				break;
			}
		} while (insertingat < upto);       /* ② 进度未过目标 ⇒ 继续等 */

		if (XLogRecPtrIsValid(insertingat) && insertingat < finishedUpto)
			finishedUpto = insertingat;     /* ③ 用最慢的相关者收紧答案 */
	}
	finishedUpto = pg_atomic_monotonic_advance_u64(&XLogCtl->logInsertResult,
												   finishedUpto);
	return finishedUpto;
```

三个细节：① 的 `logInsertResult` 是 PG 17 加的共享缓存（commit f3ff7bf83b，§11）：把上次算出的连续前缀存下来，让"要刷的位置早已就绪"的调用一条原子读就返回，避免每次都扫 8 把锁 + 抢 insertpos_lck；② `LWLockWaitForVar` 是为这个场景定制的 LWLock 原语（§11 commit 68a2e52bba）：睡在"锁释放**或**变量值变化"上——插入者推进水位（不放锁）也能唤醒等待者；循环内不加内存屏障读到旧值也无妨（xlog.c:1615-1624 注释）：旧值只会让我们多等一轮进入带原子操作的等待队列，不会漏等；③ 返回值单调推进共享缓存，并发的等待者互相搭便车。

### 3.4 CopyXLogRecordToWAL 主线

`src/backend/access/transam/xlog.c:1266`。主线：`GetXLogBuffer(CurrPos, tli)`（xlog.c:1673）把 LSN 映射到环形 WAL 缓冲区（`wal_buffers` 个 8KB 页）里的地址——快路径只是查 `xlblocks[]` 数组验证该页确实缓存着目标 LSN；然后沿 `XLogRecData` 链逐段 memcpy。跨 WAL 页边界时（xlog.c:1308-1332）：

```c
			currpos = GetXLogBuffer(CurrPos, tli);
			pagehdr = (XLogPageHeader) currpos;
			pagehdr->xlp_rem_len = write_len - written;
			pagehdr->xlp_info |= XLP_FIRST_IS_CONTRECORD;
```

在新页页头填"上一条记录还剩多少字节"并打 contrecord 标志（§1.6），然后跳过页头继续拷。若目标页还没初始化，`GetXLogBuffer` 慢路径先 `WALInsertLockUpdateInsertingAt(initializedUpto)` 公布水位（§3.2 的关键一步，xlog.c:1743），再 `AdvanceXLInsertBuffer`（xlog.c:2026）淘汰旧页——必要时触发一次 WAL 写盘（xlog.c:2082 调 `WaitXLogInsertionsToFinish`），这正是死锁分析里的那条边。函数尾部核对 `CurrPos == EndPos`，预留与实拷不符直接 PANIC（xlog.c:1402-1405）。

### 小结

`ReserveXLogInsertLocation` 用"可用字节位置"技巧把全局串行段压缩到自旋锁下五次赋值，同时免费生成 `xl_prev`；8 槽插入锁让拷贝完全并行；insertingAt 水位让刷盘者能算出"连续就绪前缀"，其惰性更新既省了常态开销又恰好防死锁。LSN 全序来自预留的全序，与拷贝完成的乱序解耦——这是 PG 9.4 重构的全部精髓，也与 InnoDB 8.0 的 link_buf 方案殊途同归。

---

## 4. 落盘与组提交：XLogFlush 与 XLogWrite

### 4.1 XLogFlush 全函数走读：组提交的等待/接力协议

`src/backend/access/transam/xlog.c:2800`。调用者：事务提交（xact.c:1544）、刷脏页前的 bufmgr（bufmgr.c:4585）、checkpoint 等。全函数按序走读：

**① 恢复态旁路（xlog.c:2813-2817）**：恢复中不写 WAL，`XLogFlush` 退化为 `UpdateMinRecoveryPoint`——把"数据页已包含到哪条 WAL"记进 pg_control，保证备库崩溃后至少重放到该点才开门（否则数据比 WAL 新，状态不一致）。

**② 快路径（xlog.c:2820-2821)**：`record <= LogwrtResult.Flush`（本进程缓存的已刷盘位置）直接返回——多数提交在别人顺手刷盘后走这条零开销路径。

**③ 主循环（xlog.c:2848-2930，逐字摘录骨架）**：

```c
	for (;;)
	{
		XLogRecPtr	insertpos;

		/* done already? */
		RefreshXLogWriteResult(LogwrtResult);
		if (record <= LogwrtResult.Flush)
			break;                                          /* (a) */

		SpinLockAcquire(&XLogCtl->info_lck);
		if (WriteRqstPtr < XLogCtl->LogwrtRqst.Write)
			WriteRqstPtr = XLogCtl->LogwrtRqst.Write;       /* (b) */
		SpinLockRelease(&XLogCtl->info_lck);
		insertpos = WaitXLogInsertionsToFinish(WriteRqstPtr);

		if (!LWLockAcquireOrWait(WALWriteLock, LW_EXCLUSIVE))
		{
			/* (c) 锁刚被释放，但我们没拿：回头复查 */
			continue;
		}

		/* Got the lock; recheck whether request is satisfied */
		RefreshXLogWriteResult(LogwrtResult);
		if (record <= LogwrtResult.Flush)
		{
			LWLockRelease(WALWriteLock);
			break;                                          /* (d) */
		}

		if (CommitDelay > 0 && enableFsync &&
			MinimumActiveBackends(CommitSiblings))
		{
			pgstat_report_wait_start(WAIT_EVENT_COMMIT_DELAY);
			pg_usleep(CommitDelay);                         /* (e) */
			pgstat_report_wait_end();
			insertpos = WaitXLogInsertionsToFinish(insertpos);
		}

		/* try to write/flush later additions to XLOG as well */
		WriteRqst.Write = insertpos;
		WriteRqst.Flush = insertpos;                        /* (f) */

		XLogWrite(WriteRqst, insertTLI, false);

		LWLockRelease(WALWriteLock);
		/* done */
		break;
	}
```

组提交（group commit）在 PG 里**没有独立的组长选举代码**，它由这个锁协议自然涌现，三层机制叠加：

1. **搭便车（(a)(c)(d)）**：`LWLockAcquireOrWait` 是专为这个场景发明的原语（§11 commit 9b38d46d9f/1a01560cbb）：如果锁被占，**睡到锁释放但醒来时不持有锁**，返回 false。醒来后回到循环头复查——锁的前任按 (f) 刷到了全局最远插入点，大概率已覆盖我的 `record`，于是我在 (a) 直接 break，一次系统调用都没做。对比普通 `LWLockAcquire`：那会让 10 个等待者排队串行执行 10 次 fsync；AcquireOrWait 让第一个 fsync 替后面 9 个把活全干了。注释（xlog.c:2868-2872）明说这是为了"maintain a good rate of group committing when the system is bottlenecked by the speed of fsyncing"；
2. **顺手多刷（(b)(f)）**：拿到锁的"组长"不只刷自己需要的 `record`，而是把请求抬到共享的 `LogwrtRqst.Write`，再经 `WaitXLogInsertionsToFinish` 抬到**当前最远的连续就绪点**——把排在后面的兄弟的记录一并带走。这就是"接力"：每一棒都刷到当时能刷的最远处；
3. **主动等客（(e)）**：`commit_delay`/`commit_siblings` 让组长睡一小觉攒更多跟班，睡醒后把水位再推远一次（此处二次调用 `WaitXLogInsertionsToFinish` 不会真等人——注释 xlog.c:2908-2916 解释：insertpos 之前的插入早已完成，调用只为把答案推得更远）。默认关闭（0），因为前两层在高并发下已经足够。

**④ 收尾（xlog.c:2932-2975）**：唤醒 walsender 和等 LSN 的进程；最后的防御性检查——如果刷完还没到 `record`，说明调用者传来的 LSN 超过了 WAL 末尾（典型原因：损坏的数据页头里有一个乱七八糟的 `pd_lsn`），报 ERROR 而不是 PANIC。注释（xlog.c:2948-2957）讲了 7.1 时代的实战教训：当年这里 PANIC，结果一个坏页反复让整库起不来；如今坏 LSN 只会让那次刷盘失败，不拖垮全系统。

**与 InnoDB 对照**：InnoDB 8.0 把"写 log buffer → write 系统调用 → fsync"拆成专职线程（log_writer/log_flusher）+ 事件组唤醒；MySQL 还要再叠一层 binlog 组提交（`binlog_group_commit_sync_delay`，三阶段 FLUSH/SYNC/COMMIT）。PG 没有 binlog，一份 WAL 一层组提交，路径短得多；代价是复制/CDC 也都得从这一份 WAL 出。

### 4.2 XLogWrite 主线

`src/backend/access/transam/xlog.c:2325`。前置契约（xlog.c:2320-2322 注释）：**必须持 WALWriteLock，且调用前必须先 `WaitXLogInsertionsToFinish`**（顺序不能反，否则死锁——§3.2）。主线：

1. **聚批**：从上次写到的位置起遍历环形缓冲区页，把物理上连续的页攒成一批（`npages`），直到三种边界之一——请求点（`last_iteration`）、环形缓冲区回绕点（`curridx == XLogCtl->XLogCacheBlck`）、段文件边界（`finishing_seg`）——才发一次大 `pg_pwrite`（xlog.c:2426-2455）。跨段时先 `XLogFileClose/XLogFileInit` 切换文件描述符（xlog.c:2381-2398）；
2. **写失败即 PANIC**（xlog.c:2470-2473）：WAL 写不进去，数据库没有任何安全的继续方式；
3. **段写满时的顺手活**（xlog.c:2498-2526）：立即 `issue_xlog_fsync` 该段（保证"一段一 fsync，且只有一个进程做"）、通知归档（`XLogArchiveNotifySeg`，§9.1）、检查是否该按 WAL 量触发 checkpoint（`XLogCheckpointNeeded`→`RequestCheckpoint(CHECKPOINT_CAUSE_XLOG)`——`max_wal_size` 的执行点就在这里）；
4. **按请求 fsync**（xlog.c:2547-2578）：`wal_sync_method` 不是 open_sync/open_dsync（写时已同步）才需要显式 fsync，然后推进 `LogwrtResult.Flush` 并更新共享状态。

分工总览：`XLogWrite` 是唯一真正调 write/fsync 的函数；`XLogFlush` 是"保证到某 LSN"的同步入口；`XLogBackgroundFlush`（xlog.c:3003）是 walwriter 进程的周期入口，替 `synchronous_commit=off` 的异步提交擦屁股（最多三个 `wal_writer_delay` 周期内落盘，xlog.c:2991-2995 注释）。

### 小结

`XLogFlush` 的组提交不靠组长选举，靠三层机制涌现：AcquireOrWait 的"醒来先复查"、组长"刷到最远连续就绪点"、可选的 commit_delay 攒批。`XLogWrite` 聚批写、段尾 fsync + 触发归档/checkpoint、写失败 PANIC。walwriter 兜底异步提交。恢复态下 XLogFlush 变身为 minRecoveryPoint 的记账员。

---

## 5. Full-page write：页撕裂问题的完整推演

### 5.1 撕裂是怎么发生的

PG 的页是 8KB，但磁盘的**原子写单位**更小：机械盘扇区 512B，现代 SSD 通常 4KB；文件系统页缓存按 4KB 管理。OS 把一个 8KB 页写回磁盘时，物理上是两次（或十六次）设备写。**断电可以落在任意两次之间。**

具体场景：页 P 上有元组 T1（偏移 200，页前半部）和空闲空间（页后半部）。事务往 P 插入元组 T2（`PageAddItem` 从页尾往前分配，落在页后半部；行指针数组在页头追加）。checkpointer 开始把 P 写盘，OS 先写了前 4KB（含新行指针），**断电**，后 4KB（含 T2 的实际数据）没写。重启后磁盘上的 P：前半是新版本（行指针指向 T2 应在的位置），后半是旧版本（T2 的位置上是陈年垃圾）。这个页**既不是修改前的状态，也不是修改后的状态**——它是一个物理上自洽性被破坏的杂交体，页内 CRC（如果开了 data checksums）会报错，但没开 checksums 时它看起来"像个页"。

### 5.2 无 FPW 时 redo 如何产生静默损坏

假设 WAL 里只记增量（"在页 P 偏移 X 处加一条行指针，元组内容为 …"），推演恢复过程：

1. 恢复从 REDO 点开始，读到"P 插入 T2"的记录；
2. `XLogReadBufferForRedo` 读入撕裂的 P，比较 LSN。**灾难在此**：页头 `pd_lsn` 在前 4KB——已经被写成新值了。于是 `lsn <= PageGetLSN(page)` 成立，返回 `BLK_DONE`，恢复**跳过**这条记录，认为页上已经有 T2 了；
3. 恢复"成功"完成，数据库开门。页 P 的行指针指向的位置上是垃圾字节。某天一个 SELECT 扫到 T2 的行指针，按垃圾字节解出一个长度荒谬的元组——轻则 `invalid memory alloc request size` 报错，重则悄悄返回错误数据，或 crash。**没有任何一步检测到问题**——这就是"静默损坏"（silent corruption）：恢复流程每一步都按规则办事，规则的前提（页是某个一致版本）被物理世界打破了。

反过来若撕裂方向相反（后半新前半旧，`pd_lsn` 还是旧值），redo 会判 `BLK_NEEDS_REDO` 并**在杂交页上做增量重放**——比如往"行指针数说是 N 但实际数据只有 N-1 条"的页上再加一条——同样是未定义行为。

**FPW 如何解决**：§2.3 的判定保证 REDO 点之后每个页的**第一条**修改记录带整页镜像且置 `BKPIMAGE_APPLY`。恢复时 `XLogReadBufferForRedoExtended` 对这种块引用**根本不看页的现有内容**——`RBM_ZERO_AND_LOCK` 方式取页（连读都不读磁盘上的旧页）、`RestoreBlockImage` 整页覆盖、`PageSetLSN`（§8.4）。撕裂页被镜像无条件碾平，之后的增量记录站在镜像这个可信基础上重放。xlogutils.c:293-300 的注释把信任模型说得很直白：**WAL 数据过了 CRC 校验所以可信，数据库页则未必可信**。

### 5.3 WAL 自己也是文件，为什么不怕撕裂？

WAL 页也是 8KB，也会被撕裂——但 WAL 的读取协议天然免疫：
- 每条**记录**有 CRC，覆盖全部内容。半条记录 CRC 必然失败，恢复把它当作"日志末尾"截断（§8.2）——丢的是**尚未 fsync 成功的尾巴**，而提交协议保证已确认的事务都在 fsync 过的前缀里；
- 跨页记录靠 `xlp_rem_len`/`XLP_FIRST_IS_CONTRECORD` 校验续接是否完整，接不上同样按末尾处理（此时会写一条 `XLOG_OVERWRITE_CONTRECORD`，xlog.c:7979，声明"此处曾有半条记录被放弃"，防止复制下游困惑）；
- 关键区别：**WAL 是纯追加流，"撕裂"只可能发生在尾部，而尾部本来就是允许丢弃的**；数据页是随机改写，撕裂发生在"声称已经完好"的存量数据上，必须靠外力（镜像）修复。

### 5.4 对照 InnoDB doublewrite buffer

两者解决同一个问题，策略相反：

| | PG full-page writes | InnoDB doublewrite |
|---|---|---|
| 机制 | checkpoint 后首改的页整页写入 **WAL** | 每次刷脏页，先把整页顺序写入 doublewrite 文件并 fsync，再写数据文件 |
| 写放大位置 | WAL 流膨胀（checkpoint 越频繁越严重） | 每次刷页都双写：一次顺序 + 一次随机，页面写路径 I/O 翻倍 |
| 恢复方式 | 重放 WAL 时用镜像覆盖页 | 崩溃后扫描 doublewrite 区，用完好副本修复撕裂页，然后才能跑（增量式的）redo |
| 逻辑依赖 | redo 对首改页不依赖页内容 | redo 是增量的（记录页内 mtr 级变更），必须以"页完好"为前提，doublewrite 是这个前提的兜底 |
| 可关闭条件 | 文件系统/设备保证 ≥8KB 原子写（如 ZFS CoW）时可 `full_page_writes=off` | 同理可关 `innodb_doublewrite` |

一句话：**InnoDB 为每次刷脏页买保险，PG 为每个 checkpoint 周期的首次修改买保险**。热点页在一个周期内被改一万次，PG 只付一次整页代价（后续全是增量），InnoDB 每次刷它都双写；反之冷页偶尔改一下，PG 每次都是"周期内首改"要整页，InnoDB 只在真刷盘时才多写。PG 方案的调优抓手因此是**拉长 checkpoint 周期**（`checkpoint_timeout`/`max_wal_size`↑ ⇒ FPI 占比↓）和 `wal_compression`（FPI 是高度可压缩的，§11）；代价是恢复时间变长——这是 recovery time 与 runtime overhead 的经典权衡。

### 小结

8KB 页 × 4KB 原子写 = 断电可产生"半新半旧"的撕裂页；撕裂会把恢复的 LSN 判定和增量重放全部带进未定义行为，且全程无报错。FPW 用"REDO 点后首改必带镜像 + 恢复时无条件覆盖"把页的可信根从磁盘转移到 WAL（WAL 尾部撕裂被 CRC 协议天然消化）。InnoDB 用 doublewrite 在页面写路径上解决同一问题，PG 把代价挪到了 WAL 流上，于是"调大 checkpoint 间隔"成了 PG 特有的调优杠杆。

---

## 6. ARIES 对照：为什么 PG 敢没有 undo

### 6.1 ARIES 三要素在 PG 的对应

| ARIES（Mohan 1992） | PostgreSQL | 说明 |
|---|---|---|
| WAL 先行（脏页落盘前日志必须落盘） | ✔ 完全一致 | bufmgr 刷页前调 `XLogFlush(BufferGetLSN(buf))`（bufmgr.c:4565-4585） |
| Redo 重演历史（repeating history） | ✔ 页级物理重放 | §8 的恢复主循环；比 ARIES 的"逻辑 redo"更物理 |
| Undo 回滚未提交事务（loser transactions） | ✘ **没有 undo 阶段** | 恢复只有 redo，重放到 WAL 末尾即完成 |
| CLR（补偿日志记录） | ✘ 不需要 | 没有 undo 自然没有 CLR，也没有"undo 到一半再崩溃"的嵌套问题 |
| Analysis pass（重建脏页表/事务表） | ✘ 不需要 | 恢复起点直接由 pg_control 的 REDO 点给出，无需扫描重建状态 |

### 6.2 推演：为什么"未提交数据留在页里"无害

InnoDB 是**原地更新**存储：UPDATE 直接改行，旧值搬进 undo log。崩溃时磁盘上可能存在"未提交事务已经写进数据页"的状态，且旧值已不在页上——必须靠 undo 把新值**物理擦除**、旧值搬回来，否则任何读者都会读到脏数据。所以 InnoDB 恢复 = redo（重演历史，含未提交的）+ undo（回滚 loser）。

PG 是 **no-overwrite（不覆盖写）** 存储（源自 Stonebraker《The Design of POSTGRES》1986）。推演一遍崩溃场景：

1. 事务 T（XID=1000）执行 UPDATE：插入新版本元组 V2（`xmin=1000`），旧版本 V1 打上 `xmax=1000`，**V1 原封不动留在堆里**；
2. 脏页恰好被 bgwriter 刷盘（完全合法——WAL 先行只要求日志先落盘，没说未提交数据不能落盘）；
3. T 尚未提交，断电。
4. 恢复：redo 把所有页推进到 WAL 末尾的状态——V2 在页上，`xmax` 标记在 V1 上，和崩溃前一模一样。**没有任何针对 T 的特殊处理**；
5. 开门后某查询扫到 V2：MVCC 可见性检查（`HeapTupleSatisfiesMVCC`）查 `xmin=1000` 的提交状态——CLOG（`src/backend/access/transam/clog.c`，每事务 2 bit 的状态位图）里 1000 的状态是 `IN_PROGRESS`（从未写过 COMMITTED 位），且 1000 不在任何活跃事务列表里 ⇒ 按 aborted 处理，**V2 不可见**；扫到 V1：`xmax=1000` 未提交 ⇒ 删除无效，**V1 仍可见**。

结论：**"回滚"在 PG 里不是一个动作，而是一个不动作**——只要 CLOG 里没有 COMMITTED 位，那个事务的所有修改在可见性上自动蒸发，一个字节的数据页都不用改。显式 ROLLBACK 也只是往 CLOG 写个 ABORTED（纯粹是优化：让可见性检查不用去查"它还活着吗"）。这也解释了 §7.1 第 ⑤ 步 checkpoint 为什么要等"commit 记录已写、CLOG 还没更新"的事务——**CLOG 是唯一的真相登记处**，它和 commit 记录的一致性必须由 checkpoint 协议维护。

**代价由谁付（守恒定律）**：InnoDB 的旧版本在 undo 链里，由 purge 线程按事务顺序消化，undo 表空间大小可控、清理有序；PG 的旧版本（V1 这类死元组）散落在堆页里，由 VACUUM 全表扫描回收（见第 10 篇）。**undo 没有消失，只是改名叫"死元组"并搬进了堆本身**——长事务拖住 VACUUM 时表膨胀，就是这笔账单的利息。反过来 PG 赢得的是：回滚 O(1)（InnoDB 回滚大事务要逐条反做，可能比正做还慢）、恢复无 undo pass（宕机后到可写的时间只取决于 redo 量）、无 undo 表空间爆炸问题（MySQL 的长事务会撑爆 undo，PG 的长事务撑的是表和 `pg_wal`）。

### 6.3 恢复流程对照总表

| 阶段 | ARIES/InnoDB | PG |
|---|---|---|
| 起点确定 | 最近 checkpoint + analysis 扫描 | `pg_control->checkPointCopy.redo`，直接读出 |
| redo | 从最老脏页起，logical/physiological | 从 REDO 点起，页级物理（含镜像覆盖） |
| undo | 回滚所有 loser 事务（需 CLR 保证幂等） | 无 |
| 开门时机 | redo+undo 完成后（InnoDB 可后台 rollback） | redo 完成 + end-of-recovery checkpoint（§8.6） |

### 小结

PG 只有 redo 没有 undo，根源是 no-overwrite 存储 + CLOG 集中裁决：未提交数据落盘无害（可见性把它过滤掉），回滚是"不动作"。代价转移给 VACUUM（死元组回收）；收益是 O(1) 回滚和更简单的恢复。ARIES 的 analysis/undo/CLR 三件套在 PG 里整体蒸发。

---

## 7. Checkpoint：给恢复画一条起跑线

### 7.1 CreateCheckPoint 全函数走读

checkpoint 的目的：把某时刻之前的所有脏页刷盘，从而允许恢复从该时刻（REDO 点）开始，并回收更早的 WAL。主函数 `CreateCheckPoint`（`src/backend/access/transam/xlog.c:7400`），由 checkpointer 进程调用。函数头注释（xlog.c:7379-7394）概括了在线/停库两种形态，正文按执行顺序走读：

**① 判定类型（xlog.c:7418-7421）**：`CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY` 视为 shutdown checkpoint——"An end-of-recovery checkpoint is really a shutdown checkpoint, just issued at a different time"（xlog.c:7414-7416 注释）。随后 `SyncPreCheckpoint()`（fsync 请求队列换代计数，必须在定 REDO 点之前）并进入 critical section——**checkpoint 中途出错必须 PANIC**，因为半个 checkpoint 的状态（比如已推进的 RedoRecPtr）无法回退。

**② 空闲跳过（xlog.c:7487-7497）**：

```c
	if ((flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY |
				  CHECKPOINT_FORCE)) == 0)
	{
		if (last_important_lsn == ControlFile->checkPoint)
		{
			END_CRIT_SECTION();
			ereport(DEBUG1,
					(errmsg_internal("checkpoint skipped because system is idle")));
			return false;
		}
	}
```

`GetLastImportantRecPtr()` 扫 8 把插入锁上的 `lastImportantAt`（§2.4 第 ⑥ 步顺手维护的那个值）取最大者。若上次 checkpoint 以来没有任何"重要"WAL（连 checkpoint 记录自己这类标记为 UNIMPORTANT 的都不算），直接跳过——空闲系统不再每 5 分钟空转出一条 checkpoint 记录、不再阻止磁盘休眠（§11 commit 6ef2eba3f5）。

**③ 确定 REDO 点——shutdown 分支（xlog.c:7516-7562）**：持全部插入锁（`WALInsertLockAcquireExclusive`）读出 `CurrBytePos`，REDO 点 = 下一条记录的位置（若正好在页边界还要跳过页头，xlog.c:7539-7547）：

```c
		checkPoint.redo = curInsert;
		...
		RedoRecPtr = XLogCtl->Insert.RedoRecPtr = checkPoint.redo;
```

停库时不会再有并发插入，checkpoint 记录本身就是 REDO 点。注释（xlog.c:7553-7559）解释了失败语义：如果 checkpoint 后面失败了，RedoRecPtr 白推进了一截也无害——后果只是之后的插入多做几个不必要的 FPW（又一次体现 §2.4 的"多镜像无害"不对称性）。

**④ 确定 REDO 点——在线分支（xlog.c:7579-7602）**：

```c
	if (!shutdown)
	{
		xl_checkpoint_redo redo_rec;
		...
		XLogBeginInsert();
		XLogRegisterData(&redo_rec, sizeof(xl_checkpoint_redo));
		(void) XLogInsert(RM_XLOG_ID, XLOG_CHECKPOINT_REDO);

		/*
		 * XLogInsertRecord will have updated XLogCtl->Insert.RedoRecPtr in
		 * shared memory and RedoRecPtr in backend-local memory, but we need
		 * to copy that into the record that will be inserted when the
		 * checkpoint is complete.
		 */
		checkPoint.redo = RedoRecPtr;
	}
```

插入一条 `XLOG_CHECKPOINT_REDO` 记录，**这条记录的起始 LSN 就是新 REDO 点**。它走 §2.4 第 ④ 步的 `WALINSERT_SPECIAL_CHECKPOINT` 分支：持全部插入锁，在预留空间的同一临界区里 `Insert->RedoRecPtr = StartPos`。从这一刻起，所有后续插入的 FPW 判定都以新 REDO 点为准——"REDO 点之后每页的首次修改必有镜像"这一恢复正确性的根基由此建立。PG 17 之前 REDO 点是一个"虚拟位置"（持全部锁读出插入点但不写记录），引入显式记录（§11 commit afd12774ae）一是让增量备份的 WAL summarizer 能在流里看到 checkpoint 边界，二是恢复时可以校验 REDO 点确实是 REDO 记录（§8.2 的 FATAL 检查）。

**⑤ 采集事务系统水位（xlog.c:7631-7654）**：分别短暂持 XidGenLock/CommitTsLock/OidGenLock 读出 `nextXid`、`oldestXid`、`nextOid`、multixact 水位等填进 `CheckPoint` 结构体——恢复启动时事务系统就从这些值初始化。注意这一步在刷脏页**之前**、REDO 点**之后**：这些值只要"不比 REDO 点旧"即可，恢复重放 REDO 点之后的 WAL 时会把它们推到最新（`ApplyWalRecord` 里的 `AdvanceNextFullTransactionIdPastXid`，§8.3）。

**⑥ 等待悬空的提交（xlog.c:7664-7713）**：退出 critical section 后：

```c
	vxids = GetVirtualXIDsDelayingChkpt(&nvxids, DELAY_CHKPT_START);
	if (nvxids > 0)
	{
		do
		{
			AbsorbSyncRequests();
			pgstat_report_wait_start(WAIT_EVENT_CHECKPOINT_DELAY_START);
			pg_usleep(10000L);	/* wait for 10 msec */
			pgstat_report_wait_end();
		} while (HaveVirtualXIDsDelayingChkpt(vxids, nvxids,
											  DELAY_CHKPT_START));
	}
```

commit 分"写 commit 记录到 WAL"和"更新 CLOG"两步（不同锁保护，xact.c 为降低锁竞争故意拆开）。若某事务的 commit 记录恰好落在 REDO 点**之前**、而 CLOG 更新还没做，恢复不会重放那条 commit 记录（在 REDO 点前）⇒ CLOG 里它永远不提交 ⇒ **已向客户端确认的事务凭空消失**。所以这类夹在中间的事务用 `delayChkptFlags` 举手，checkpoint 每 10ms 轮询等它们出临界区，保证它们的 CLOG 更新会被本次 `CheckPointCLOG()` 刷盘。注释（xlog.c:7689-7693）论证了无漏网：还没举手的事务 commit 记录必然在 REDO 点后（会被重放），已放手的 CLOG 已更新（会被刷盘）。

**⑦ 刷盘（xlog.c:7715）**：`CheckPointGuts(checkPoint.redo, flags)`，见 §7.2。这是耗时主体，可能持续几分钟（受 §7.3 节流）。

**⑧ 写 checkpoint 记录并落盘（xlog.c:7743-7754）**：重新进入 critical section，把 `CheckPoint` 结构体作为一条 `XLOG_CHECKPOINT_ONLINE/SHUTDOWN` 记录插入，然后 `XLogFlush(recptr)`。此时 WAL 里的图景（在线情形）：`REDO 记录 …(刷盘期间的正常流量)… CHECKPOINT_ONLINE 记录`，后者内嵌指回前者的 `checkPoint.redo`。

**⑨ 更新控制文件（xlog.c:7783-7805）**：

```c
	LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
	if (shutdown)
		ControlFile->state = DB_SHUTDOWNED;
	ControlFile->checkPoint = ProcLastRecPtr;
	ControlFile->checkPointCopy = checkPoint;
	/* crash recovery should always recover to the end of WAL */
	ControlFile->minRecoveryPoint = InvalidXLogRecPtr;
	ControlFile->minRecoveryPointTLI = 0;
	...
	UpdateControlFile();
	LWLockRelease(ControlFileLock);
```

`checkPoint` 指向记录位置，`checkPointCopy` 是整个结构体的副本（恢复时不用先读 WAL 就知道 REDO 点在哪）。**只有这一步完成，checkpoint 才算生效**——之前任何时刻崩溃，pg_control 还指着旧 checkpoint，恢复从旧 REDO 点开始，一切照常（只是白干了刷盘的活）。pg_control 自身 512B < 扇区大小，其写入按原子处理，且带 CRC。

**⑩ 回收旧 WAL（xlog.c:7846-7872）**：按新 REDO 点算出可删段号，`KeepLogSeg` 再考虑 `wal_keep_size` 与复制槽的保留需求；超限的复制槽先失效（`InvalidateObsoleteReplicationSlots`）再重算；`RemoveOldXlogFiles` 删除或**改名复用**旧段（复用避免了新段的文件系统分配开销，也是 §1.2 里"旧记录残留"的来源）；`PreallocXlogFiles` 预建未来的段。最后截断 pg_subtrans、记统计。

**顺序的必然性**：先刷脏页 → 再写 checkpoint 记录 → 最后改 pg_control → 才能删 WAL。任何一步倒置都制造数据丢失窗口：pg_control 先行 = "宣称可从 REDO 点恢复，但 REDO 点前的脏页还没落盘"；删 WAL 先行 = 恢复起点之后的日志没了。

### 7.2 CheckPointGuts 的刷盘顺序

`src/backend/access/transam/xlog.c:8048`（逐字，仅省略 TRACE/统计行）：

```c
static void
CheckPointGuts(XLogRecPtr checkPointRedo, int flags)
{
	CheckPointRelationMap();
	CheckPointReplicationSlots(flags & CHECKPOINT_IS_SHUTDOWN);
	CheckPointSnapBuild();
	CheckPointLogicalRewriteHeap();
	CheckPointReplicationOrigin();

	/* Write out all dirty data in SLRUs and the main buffer pool */
	CheckPointCLOG();
	CheckPointCommitTs();
	CheckPointSUBTRANS();
	CheckPointMultiXact();
	CheckPointPredicate();
	CheckPointBuffers(flags);

	/* Perform all queued up fsyncs */
	ProcessSyncRequests();

	/* We deliberately delay 2PC checkpointing as long as possible */
	CheckPointTwoPhase(checkPointRedo);
}
```

先刷元状态（关系映射、复制槽、逻辑解码快照），再刷 SLRU（CLOG 必须伴随数据页持久化——⑥ 步等来的 CLOG 更新在这里落盘），然后 `CheckPointBuffers` 遍历缓冲池把 `BM_CHECKPOINT_NEEDED` 的脏页 `write()` 出去（每页之间调 `CheckpointWriteDelay` 节流，§7.3），最后 `ProcessSyncRequests` 统一 fsync 所有被写过的文件——**write 与 fsync 分离**，给 OS 留出合并回写的窗口。

### 7.3 checkpoint_completion_target：摊平 I/O 的控制论

一口气刷几十 GB 脏页会打爆磁盘、挤压前台查询的 I/O。PG 的方案是**闭环节流**：`CheckPointBuffers` 每写一页调一次 `CheckpointWriteDelay`（`src/backend/postmaster/checkpointer.c:795`），传入进度 `progress`（已写页数/总页数）：

```c
	if (!(flags & CHECKPOINT_FAST) &&
		!ShutdownXLOGPending &&
		!ShutdownRequestPending &&
		!FastCheckpointRequested() &&
		IsCheckpointOnSchedule(progress))
	{
		...
		AbsorbSyncRequests();
		...
		WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
				  100,
				  WAIT_EVENT_CHECKPOINT_WRITE_DELAY);
		ResetLatch(MyLatch);
	}
```

进度超前就睡 100ms（顺手消化别的进程转来的 fsync 请求）。判定器 `IsCheckpointOnSchedule`（checkpointer.c:865）是控制论的核心——**双进度尺**：

```c
	/* Scale progress according to checkpoint_completion_target. */
	progress *= CheckPointCompletionTarget;
	...
	/* ① WAL 尺：消耗的 WAL 段数 / CheckPointSegments */
	elapsed_xlogs = (((double) (recptr - ckpt_start_recptr)) /
					 wal_segment_size) / CheckPointSegments;
	if (progress < elapsed_xlogs)
	{
		ckpt_cached_elapsed = elapsed_xlogs;
		return false;
	}

	/* ② 时间尺：经过的秒数 / CheckPointTimeout */
	elapsed_time = ((double) ((pg_time_t) now.tv_sec - ckpt_start_time) +
					now.tv_usec / 1000000.0) / CheckPointTimeout;
	if (progress < elapsed_time)
	{
		ckpt_cached_elapsed = elapsed_time;
		return false;
	}
	return true;
```

推演这个闭环：设 `checkpoint_completion_target = 0.9`（默认）。控制目标是**让"写脏页的进度"始终不落后于"本周期时间/WAL 预算的消耗进度 ÷ 0.9"**——即计划在周期的 90% 处刚好写完，留 10% 余量给最后的 fsync 阶段。两把尺子取更紧的那把：
- 平稳负载下时间尺主导：脏页均匀摊到 0.9 × `checkpoint_timeout` 里，磁盘写入曲线是一条平缓的直线（监控里 checkpoint 的"锯齿"被熨平就是它的功劳）；
- 写入突增时 WAL 尺主导：WAL 长得快 ⇒ `elapsed_xlogs` 涨得快 ⇒ 判"落后" ⇒ 停止睡眠全速刷——因为按这个速度下一次 checkpoint（由 `max_wal_size` 触发，§4.2 第 3 步）会提早到来，必须赶在它之前完成本轮；
- 为什么必须在预算内完成？checkpoint 之间是**流水线**：下一轮请求到达时上一轮还没完，就会出现 checkpoint 排队、`max_wal_size` 超标（WAL 只能在 checkpoint 完成后回收）、日志里出现 "checkpoints are occurring too frequently" 警告。

`ckpt_cached_elapsed` 缓存上次算出的进度尺，避免每页都做 gettimeofday/读 LSN——先和缓存比，超过缓存才重算（checkpointer.c:877-884）。

对照 InnoDB：等价物是 adaptive flushing——按 redo 产生速率和 checkpoint age（`innodb_max_dirty_pages_pct_lwm` 等一堆旋钮）调节 page cleaner 的刷脏速率，同样是"以日志消耗速度为参考输入的闭环控制"，只是 InnoDB 的 checkpoint 是连续推进的（fuzzy checkpoint，min dirty page LSN 就是 checkpoint），PG 是离散批次。

### 小结

checkpoint = 定 REDO 点（在线时插一条 `XLOG_CHECKPOINT_REDO` 并在同一临界区原子推进共享 RedoRecPtr）→ 等悬空提交出临界区 → 按序刷元状态/SLRU/缓冲池（write 与 fsync 分离）→ 写 checkpoint 记录并 fsync → 更新 pg_control（生效点）→ 回收旧 WAL。空闲时整体跳过。刷盘速率被双进度尺（时间、WAL 量）闭环节流到 `checkpoint_completion_target`，兼顾"摊平 I/O"与"赶在下一轮之前完成"。

---

## 8. 崩溃恢复：把历史重演一遍

### 8.1 总览与起点确定

启动时 postmaster 先派生 startup 进程执行 `StartupXLOG`（`src/backend/access/transam/xlog.c:5845`），主线：

```
StartupXLOG
 ├─ InitWalRecovery()            xlogrecovery.c:457   决定从哪开始、要不要归档恢复
 ├─ ... 初始化事务系统水位（nextXid 等来自 checkpoint 记录副本）
 ├─ PerformWalRecovery()         xlogrecovery.c:1612  ★ redo 主循环
 ├─ FinishWalRecovery()          xlogrecovery.c:1417  收尾：确定新 WAL 插入点
 ├─ PerformRecoveryXLogAction()  xlog.c:6785          end-of-recovery checkpoint 或轻量记录
 └─ 打开读写服务
```

**恢复起点**在 `InitWalRecovery` 中确定（无 backup_label 的普通崩溃场景，xlogrecovery.c:719-723）：

```c
		/* Get the last valid checkpoint record. */
		CheckPointLoc = ControlFile->checkPoint;
		CheckPointTLI = ControlFile->checkPointCopy.ThisTimeLineID;
		RedoStartLSN = ControlFile->checkPointCopy.redo;
		RedoStartTLI = ControlFile->checkPointCopy.ThisTimeLineID;
		record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc,
									  CheckPointTLI);
```

`pg_control`（`global/pg_control`，可用 `pg_controldata` 查看）保存着 §7.1 第 ⑨ 步写入的东西。恢复从 `checkPointCopy.redo`（REDO 点）开始重放，而不是从 checkpoint 记录本身开始——在线 checkpoint 的 REDO 点在记录之前，刷脏页期间产生的 WAL 也必须重放。若存在 `backup_label`（基础备份恢复），起点改从 label 文件里取，防止用了备份过程中被推进的 pg_control——这正是"备份必须包含从 start 到 stop 之间全部 WAL"的源码依据。

### 8.2 PerformWalRecovery 全走读

`src/backend/access/transam/xlogrecovery.c:1612`。按序走读（省略 `WAL_DEBUG` 段与进度报告——与主线无关）：

**① 初始化重放进度共享变量（xlogrecovery.c:1623-1644）**：把 `lastReplayedEndRecPtr` 等初始化为"REDO 点之前那条记录刚重放完"的状态；通知 postmaster 恢复已开始（可以拉起 archiver）；调 `CheckRecoveryConsistency`——热备场景若已一致可立即放开只读连接。

**② 定位第一条要重放的记录并校验（xlogrecovery.c:1662-1686）**：

```c
	if (RedoStartLSN < CheckPointLoc)
	{
		/* back up to find the record */
		replayTLI = RedoStartTLI;
		XLogPrefetcherBeginRead(xlogprefetcher, RedoStartLSN);
		record = ReadRecord(xlogprefetcher, PANIC, false, replayTLI);

		/*
		 * If a checkpoint record's redo pointer points back to an earlier
		 * LSN, the record at that LSN should be an XLOG_CHECKPOINT_REDO
		 * record.
		 */
		if (record->xl_rmid != RM_XLOG_ID ||
			(record->xl_info & ~XLR_INFO_MASK) != XLOG_CHECKPOINT_REDO)
			ereport(FATAL,
					errmsg("unexpected record type found at redo point %X/%08X",
						   LSN_FORMAT_ARGS(xlogreader->ReadRecPtr)));
	}
	else
	{
		/* just have to read next record after CheckPoint */
		Assert(xlogreader->ReadRecPtr == CheckPointLoc);
		replayTLI = CheckPointTLI;
		record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
	}
```

在线 checkpoint（REDO 点 < checkpoint 记录）：从 REDO 点读起，且**该位置必须是一条 `XLOG_CHECKPOINT_REDO` 记录**，否则 FATAL——pg_control 损坏或 WAL 错位在第一步就被抓住（PG 17 起才有的交叉校验，§7.1 第 ④ 步）。shutdown checkpoint：直接读 checkpoint 记录的下一条。注意读取经过 `xlogprefetcher`——按 WAL 里的块引用提前发起数据页预读（`recovery_prefetch`），掩盖恢复的随机读延迟。

**③ 打日志 `redo starts at %X/%08X`（xlogrecovery.c:1699-1701）**——DBA 最熟悉的一行字，然后进入主循环。

**④ 主循环（xlogrecovery.c:1710-1806，去掉 WAL_DEBUG/进度条后的完整逻辑）**：

```c
		do
		{
			/* Handle interrupt signals of startup process */
			ProcessStartupProcInterrupts();

			if (((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState !=
				RECOVERY_NOT_PAUSED)
				recoveryPausesHere(false);           /* 热备暂停点 */

			if (recoveryStopsBefore(xlogreader))     /* PITR：目标在本条之前？ */
			{
				reachedRecoveryTarget = true;
				break;
			}

			if (recoveryApplyDelay(xlogreader))      /* recovery_min_apply_delay */
			{ ... }

			/* Apply the record */
			ApplyWalRecord(xlogreader, record, &replayTLI);

			/* 唤醒等待重放进度的进程（pg_wal_replay_wait 等） */
			WaitLSNWakeup(WAIT_LSN_TYPE_STANDBY_REPLAY, ...);
			...

			if (recoveryStopsAfter(xlogreader))      /* PITR：目标在本条之后？ */
			{
				reachedRecoveryTarget = true;
				break;
			}

			/* Else, try to fetch the next WAL record */
			record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
		} while (record != NULL);
```

要点：
- **循环终止条件 `record == NULL`**：`ReadRecord` 在 CRC 失败、`xl_prev` 对不上、contrecord 续接不上或段文件缺失时返回 NULL——**WAL 的"末尾"就是第一条读不出的合法记录**（崩溃恢复模式下，错误级别只是 LOG，因为"读到头"是预期结局）；归档/流复制恢复模式下 `ReadRecord` 会在源之间轮询并等待新 WAL，循环变成"永不结束的恢复"——这正是备库的运行形态；
- `recoveryStopsBefore/After` 检查 PITR 目标（时间戳/XID/LSN/命名恢复点，分"停在目标前"和"停在目标后"两类），普通崩溃恢复恒为 false；
- 每条记录之间都是安全的中断点/暂停点——恢复可以随时被打断（§12.3 论证为什么安全）。

**⑤ 收尾（xlogrecovery.c:1812-1876）**：到达 PITR 目标时按 `recoveryTargetAction` 决定 shutdown（`proc_exit(3)`）/pause/promote；打出 `redo done at ...` 与最后完成事务的时间戳；若设置了恢复目标却直到 WAL 尽头都没到达，FATAL 报错（防止"以为恢复到了某时刻，实际归档缺了一截却静默继续"——这个 FATAL 是 2023 年才补的安全网）。

### 8.3 ApplyWalRecord 与 rm_redo 分发

`ApplyWalRecord`（xlogrecovery.c:1883）对每条记录做六件事：① `AdvanceNextFullTransactionIdPastXid(record->xl_xid)`——保证重放后 nextXid 总在见过的最大 XID 之后（新主库分配 XID 不会撞车）；② 时间线切换检查（`XLOG_CHECKPOINT_SHUTDOWN`/`XLOG_END_OF_RECOVERY` 记录可携带新 TLI，xlogrecovery.c:1907-1940）；③ 先更新 `replayEndRecPtr` **再**重放——redo 中若触发 `XLogFlush`（恢复态 = 更新 minRecoveryPoint，§4.1 ①）要用到本条的结束位置；④ 热备时把 XID 喂给 KnownAssignedXids（快照基础设施）；⑤ 真正的分发一行（xlogrecovery.c:1966）：

```c
	/* Now apply the WAL record itself */
	GetRmgr(record->xl_rmid).rm_redo(xlogreader);
```

⑥ 重放成功后更新 `lastReplayedEndRecPtr`、唤醒级联 walsender。若记录带 `XLR_CHECK_CONSISTENCY`，重放后调 `verifyBackupPageConsistency` 把页与附带镜像逐字节比对（`wal_consistency_checking` 调试设施，抓 redo 函数与正向函数不一致的 bug）。

rmgr 表由 `src/include/access/rmgrlist.h` 生成，每行一个资源管理器：

```c
/* symbol name, textual name, redo, desc, identify, startup, cleanup, mask, decode */
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, ...)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, ...)     /* 提交/中止 → 更新 CLOG */
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, ...)
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, ...)
```

X-macro 技巧：同一份列表以不同的 `PG_RMGR` 宏定义展开多次，生成回调数组、ID 枚举等。表中顺序决定 `xl_rmid` 数值，只能尾部追加。`pg_waldump` 输出里的 "Heap/INSERT" 就是 textual name + desc 回调的产物。

### 8.4 XLogReadBufferForRedo 全函数：幂等的通用入口

redo 必须幂等——上次恢复可能中途崩溃（§12.3），同一条记录会被重放两次；物理页级操作盲目重放两次就会错（"在偏移 5 加一条行指针"×2 = 两条行指针）。PG 的方案是把"该不该重放"收敛到统一取页入口。`XLogReadBufferForRedo`（`src/backend/access/transam/xlogutils.c:302`）直接转发给 `XLogReadBufferForRedoExtended`（xlogutils.c:361），后者全函数走读：

```c
XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *record,
							  uint8 block_id,
							  ReadBufferMode mode, bool get_cleanup_lock,
							  Buffer *buf)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	...
	if (!XLogRecGetBlockTagExtended(record, block_id, &rlocator, &forknum, &blkno,
									&prefetch_buffer))
	{
		/* Caller specified a bogus block_id */
		elog(PANIC, "failed to locate backup block with ID %d in WAL record", block_id);
	}

	/* WILL_INIT 与 zeromode 必须成对出现，两方向都查（xlogutils.c:388-393） */
	zeromode = (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK);
	willinit = (XLogRecGetBlock(record, block_id)->flags & BKPBLOCK_WILL_INIT) != 0;
	if (willinit && !zeromode)
		elog(PANIC, "block with WILL_INIT flag in WAL record must be zeroed by redo routine");
	if (!willinit && zeromode)
		elog(PANIC, "block to be initialized in redo routine must be marked with WILL_INIT flag in the WAL record");

	/* If it has a full-page image and it should be restored, do it. */
	if (XLogRecBlockImageApply(record, block_id))
	{
		Assert(XLogRecHasBlockImage(record, block_id));
		*buf = XLogReadBufferExtended(rlocator, forknum, blkno,
									  get_cleanup_lock ? RBM_ZERO_AND_CLEANUP_LOCK : RBM_ZERO_AND_LOCK,
									  prefetch_buffer);
		page = BufferGetPage(*buf);
		if (!RestoreBlockImage(record, block_id, page))
			ereport(ERROR, ...);

		/* The page may be uninitialized. If so, we can't set the LSN ... */
		if (!PageIsNew(page))
		{
			PageSetLSN(page, lsn);
		}

		MarkBufferDirty(*buf);

		/* init fork 特例：立刻刷盘（unlogged 表的模板页不走恢复） */
		if (forknum == INIT_FORKNUM)
			FlushOneBuffer(*buf);

		return BLK_RESTORED;
	}
	else
	{
		*buf = XLogReadBufferExtended(rlocator, forknum, blkno, mode, prefetch_buffer);
		if (BufferIsValid(*buf))
		{
			if (mode != RBM_ZERO_AND_LOCK && mode != RBM_ZERO_AND_CLEANUP_LOCK)
			{
				if (get_cleanup_lock)
					LockBufferForCleanup(*buf);
				else
					LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);
			}
			if (lsn <= PageGetLSN(BufferGetPage(*buf)))
				return BLK_DONE;
			else
				return BLK_NEEDS_REDO;
		}
		else
			return BLK_NOTFOUND;
	}
}
```

**四种返回值，各给一个具体场景**：

| 返回值 | 判定条件 | 典型场景 | 调用者的义务 |
|---|---|---|---|
| `BLK_NEEDS_REDO` | 无镜像（或不 APPLY），且页 LSN < 记录 LSN | 常态：页上还没有本记录的效果（比如页在 checkpoint 后第 2 次被改，磁盘上只有第 1 次的效果） | 施加修改，然后**必须** `PageSetLSN(page, lsn) + MarkBufferDirty`——否则下次重放判定失效 |
| `BLK_DONE` | 无镜像，且页 LSN ≥ 记录 LSN | 上次恢复跑到一半崩了，这个页当时已重放并被刷盘；本次恢复重放到同一条记录 | 什么都不做（页上已含本记录乃至更晚的效果） |
| `BLK_RESTORED` | 块引用带 `BKPIMAGE_APPLY` 镜像 | checkpoint 后首改记录：不管页现在是什么（哪怕撕裂成杂交体，§5.2），镜像整页覆盖 + 设 LSN | 通常无事可做；个别 rmgr 要补页外状态（如恢复 FSM） |
| `BLK_NOTFOUND` | 页（甚至文件）不存在 | 记录要改表 X 的第 100 页，但 WAL 后面有"truncate 到 50 页"或"DROP TABLE X"——文件已被后续操作删短 | 跳过。页面既然会被后续 WAL 删掉，改不改无所谓；恢复结束时若没见到对应的删除证据则报警 |

`BLK_DONE` 的比较用的 `lsn` 是**记录结束位置**（`record->EndRecPtr`）——与正向路径 `PageSetLSN(page, recptr)` 用的值一致（§2.1），两边用同一把尺子，闭环成立："写路径打版本，恢复路径比版本"。`BLK_RESTORED` 分支则体现 §5.2 的信任模型：取页用 `RBM_ZERO_AND_LOCK`（不读磁盘旧内容），覆盖天然幂等，且顺带修复撕裂。

### 8.5 一个具体的 rmgr redo：heap_xlog_insert 全走读

`src/backend/access/heap/heapam_xlog.c:402`。它是 §2.1 那段 `heap_insert` 写路径的镜像，走读全函数：

```c
static void
heap_xlog_insert(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
	...
	XLogRecGetBlockTag(record, HEAP_INSERT_BLKREF_HEAP, &target_locator, NULL,
					   &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);
```

**① 解出目标**：从块引用 0 拿物理坐标（表 + 块号），加上主数据里的 `offnum`，重建出目标 TID。注意 WAL 里存的全是物理坐标，恢复不需要（也没法）打开系统目录做名字解析——恢复早期目录本身还没恢复好，这是**物理 redo** 与逻辑复制的本质区别。

```c
	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
		heap_xlog_vm_clear(record, target_locator,
						   blkno, HEAP_INSERT_BLKREF_VM,
						   VISIBILITYMAP_VALID_BITS);
```

**② 先修 VM**：插入会清掉页的 all-visible 位。VM 页（块引用 1）和堆页（块引用 0）**各有各的 LSN、各自独立做幂等判断**——堆页可能已重放过而 VM 页还没有（两者可被分别刷盘），所以即使堆页 BLK_DONE 也要处理 VM。这是"幂等以页为单位"的一个精妙注脚。

```c
	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		buffer = XLogInitBufferForRedo(record, HEAP_INSERT_BLKREF_HEAP);
		page = BufferGetPage(buffer);
		PageInit(page, BufferGetPageSize(buffer), 0);
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, HEAP_INSERT_BLKREF_HEAP,
									   &buffer);
```

**③ 取页**：`XLOG_HEAP_INIT_PAGE`（对应写路径的 `REGBUF_WILL_INIT`，heapam.c:2121-2126）走 `XLogInitBufferForRedo`（xlogutils.c:315，内部即 RBM_ZERO_AND_LOCK）——直接给一张零页然后 `PageInit` 重建，不读磁盘；否则走 §8.4 的通用入口。

```c
	if (action == BLK_NEEDS_REDO)
	{
		...
		data = XLogRecGetBlockData(record, HEAP_INSERT_BLKREF_HEAP, &datalen);

		newlen = datalen - SizeOfHeapHeader;
		Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
		memcpy(&xlhdr, data, SizeOfHeapHeader);
		data += SizeOfHeapHeader;

		htup = &tbuf.hdr;
		MemSet(htup, 0, SizeofHeapTupleHeader);
		/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
		memcpy((char *) htup + SizeofHeapTupleHeader, data, newlen);
		newlen += SizeofHeapTupleHeader;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;

		if (PageAddItem(page, htup, newlen, xlrec->offnum, true, true) == InvalidOffsetNumber)
			elog(PANIC, "failed to add tuple");
```

**④ 重建元组并插入**：WAL 里为省空间只存了精简头（`xl_heap_header` 三个字段）+ 裸数据，此处在栈上拼回完整元组头——`xmin` 直接取记录的 `xl_xid`（WAL 里不用重复存），`cmin` 恒为 FirstCommandId（**命令号不需要恢复**：cmin 只在产生它的事务内部有意义，而那个事务已随崩溃消亡——又一处 no-undo 设计的省钱现场）。`PageAddItem` 指定 `offnum` 精确重演写路径的槽位分配；失败即 PANIC——WAL 说这个位置放得下，页却说放不下，说明页状态与历史脱节，绝不能继续。

```c
		freespace = PageGetHeapFreeSpace(page);	/* needed to update FSM below */
		...
		PageSetLSN(page, lsn);

		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/*
	 * If the page is running low on free space, update the FSM as well. ...
	 */
	if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(target_locator, blkno, freespace);
```

**⑤ 收尾**：`PageSetLSN` 推进页版本（幂等闭环的关键一步）+ 标脏 + 放锁。最后一段是个有意思的角落：**FSM 本身不写 WAL**（空闲空间信息错了无非是插入时多试几个页，不值得日志开销），但恢复顺手用刚算出的 freespace 更新 FSM——让新主库开门时 FSM 不至于太离谱。注释还点明 `BLK_RESTORED` 时懒得更新 FSM 的原因："it doesn't need to be totally accurate anyway"。

### 8.6 恢复结束：checkpoint 还是轻量记录？

主循环退出后，`FinishWalRecovery`（xlogrecovery.c:1417）确定最后一条合法记录的位置——新 WAL 将从那里续写（崩溃恢复总是恢复到 WAL 末尾）。然后 `PerformRecoveryXLogAction`（xlog.c:6785，全函数）：

```c
static bool
PerformRecoveryXLogAction(void)
{
	bool		promoted = false;
	...
	if (ArchiveRecoveryRequested && IsUnderPostmaster &&
		PromoteIsTriggered())
	{
		promoted = true;
		/*
		 * Insert a special WAL record to mark the end of recovery, since we
		 * aren't doing a checkpoint. ...
		 */
		CreateEndOfRecoveryRecord();
	}
	else
	{
		RequestCheckpoint(CHECKPOINT_END_OF_RECOVERY |
						  CHECKPOINT_FAST |
						  CHECKPOINT_WAIT);
	}

	return promoted;
}
```

**为什么有两种收尾？** 权衡的是"再次崩溃的恢复成本"与"现在就能服务的时间"：

- **崩溃恢复路径**：做一个真正的 end-of-recovery checkpoint（本质是 shutdown checkpoint，§7.1 ①），且 `CHECKPOINT_WAIT` 同步等它完成。动机：此刻 pg_control 里的 checkpoint 还是崩溃**前**的那个，如果不做 checkpoint 就开门、然后又崩一次，第二次恢复要把刚才重放过的 WAL **全部重来**；更糟的是恢复期间可能改写过时间线等状态。付出的是开门前的一次全量刷脏页（刚重放完，脏页可能很多），换来一个干净的新起点；
- **备库提升路径**：只写一条几十字节的 `XLOG_END_OF_RECOVERY` 记录（`CreateEndOfRecoveryRecord`，xlog.c:7908：记录新旧时间线 + 时间戳，`XLogFlush` 后把 minRecoveryPoint 推到该记录，保证提升不可回退）。动机是**故障切换的 RTO**：主库挂了，业务在等新主库接客，此刻先做一个可能几分钟的 checkpoint 是不可接受的。而且提升场景下"再崩一次要多重放一些 WAL"的风险是可接受的——大不了从上一个 restartpoint 重放。checkpoint 没有被省略，只是**异步化**：完全出恢复模式后 `RequestCheckpoint(CHECKPOINT_FORCE)`（xlog.c:6701-6702），由 checkpointer 慢慢做。注释（xlog.c:6807-6814）还解释了另一层：此刻 checkpointer 很可能正做着 restartpoint 做到一半，让它自然收尾比强行掉头更简单。

`XLOG_END_OF_RECOVERY` 记录同时是时间线切换的载体——下游备库重放到它就知道要跟去新时间线（§8.3 ②）。

### 小结

恢复起点 = pg_control 里最近 checkpoint 的 REDO 点（且 PG 17 起校验该处必须是 REDO 记录）；主循环逐条读（CRC/xl_prev/contrecord 校验界定 WAL 尽头）、按 `xl_rmid` 分发 rm_redo；幂等由统一取页入口保证——四种返回值分别对应"需重放/已重放/镜像覆盖/页已消亡"；`heap_xlog_insert` 展示了物理 redo 的全套纪律：物理坐标寻址、精确重演、PageSetLSN 闭环、页间独立幂等。收尾二选一：崩溃恢复同步 checkpoint 换干净起点，提升写轻量记录换秒级可用。

---

## 9. 归档、PITR 与 restartpoint

### 9.1 归档

- 段文件写满（`XLogWrite` 的 `finishing_seg` 分支，xlog.c:2507-2508）或 `XLOG_SWITCH` 后，`XLogArchiveNotify`（`src/backend/access/transam/xlogarchive.c:445`）在 `pg_wal/archive_status/` 下创建 `<segname>.ready` 标记文件；
- archiver 进程（`PgArchiverMain`，`src/backend/postmaster/pgarch.c:221`）的 `pgarch_ArchiverCopyLoop`（pgarch.c:379）轮询 `.ready` 文件，逐个执行 `archive_command`（或 archive module 回调）拷走段文件，成功后改名 `.done`；
- 归档失败会无限重试（间隔退避），`.ready` 堆积 ⇒ 段文件不能被 §7.1 第 ⑩ 步回收 ⇒ `pg_wal` 膨胀——连锁反应见 §12.4。

### 9.2 PITR（时间点恢复）

PITR = 基础备份 + 归档 WAL 重放到指定目标：
1. 恢复基础备份，配置 `restore_command`（从归档取段文件，实现在 `RestoreArchivedFile`，xlogarchive.c:55）；
2. 设置 `recovery_target_time/_xid/_lsn/_name` 之一；
3. 启动后走与崩溃恢复**完全相同**的 `PerformWalRecovery` 主循环，只是 WAL 来源换成归档，且 `recoveryStopsBefore/After`（§8.2 ④）会在到达目标时停下；
4. 到达目标后按 `recovery_target_action` 暂停/提升；提升开启**新时间线**（文件名第一段 +1），写 `.history` 文件记录分叉点——同一份归档可以反复恢复到不同时间点而互不污染，恢复时也靠 `.history` 决定在哪个 LSN 从旧时间线的段切到新时间线。

崩溃恢复、归档恢复（PITR）、流复制备库三者共享同一套 xlogrecovery.c 代码，区别只是 WAL 从哪来、到哪停：**备库 = 永不结束的崩溃恢复**，这句话在源码层面是字面成立的。

### 9.3 restartpoint

备库不能自己做 checkpoint（不能写 WAL），但同样需要"截断重放起点"防止每次重启都从头 redo。方案是 **restartpoint**：
- startup 进程每重放到一条主库的 checkpoint 记录，`RecoveryRestartPoint`（xlog.c:8089）把它存进共享内存当候选；
- checkpointer 按自己的节奏（`checkpoint_timeout`，同受 §7.3 节流——注意 checkpointer.c:906-907：恢复态下进度尺用**重放位置**代替插入位置）调用 `CreateRestartPoint`（xlog.c:8129）：刷脏页（复用 `CheckPointGuts`）、把 pg_control 的恢复起点推进到那条主库 checkpoint——**不写任何新 WAL**；
- 效果：备库崩溃后从最近 restartpoint 重放。restartpoint 只能建立在主库 checkpoint 之上（不能凭空选点），所以备库的"恢复起点密度"受主库 checkpoint 频率的上限约束。

### 小结

归档 = `.ready`/`.done` 标记 + archiver 执行拷贝命令，失败堆积会撑爆 `pg_wal`；PITR 复用恢复主循环，用 recovery target 提前停车并切新时间线；restartpoint 是备库版 checkpoint——只刷脏页和推进 pg_control，不产生 WAL，且必须搭在主库 checkpoint 记录上。

---

## 10. 不变式清单

每条给出：表述 → 由谁维护 → 违反后果。

**I-1（WAL 先行）**：数据页刷盘前，`pd_lsn` 之前的 WAL 必须已 fsync。
维护者：`FlushBuffer` 刷页前 `XLogFlush(BufferGetLSN(buf))`（bufmgr.c:4565-4585，仅 `BM_PERMANENT` 页）。
违反后果：崩溃后磁盘上有一个"包含某次修改效果"的页，但描述该修改的 WAL 不存在。恢复无从知道这个页超前了——若后续记录再改此页，LSN 判定会把它们当作"已重放"跳过（页 LSN 偏大），**该页永久领先又永久残缺**，静默不一致。

**I-2（提交持久）**：向客户端报告 COMMIT 成功前，commit 记录必须已 fsync（`synchronous_commit=on` 时）。
维护者：`RecordTransactionCommit` 的 `XLogFlush(XactLastRecEnd)`（xact.c:1544）。
违反后果：客户端拿到成功应答，崩溃后事务消失——违反 D（持久性）。`synchronous_commit=off` 正是**有意识地**放弃本条换延迟（窗口 ≤ 3 × `wal_writer_delay`，§4.2），但保证不产生不一致（WAL 顺序不变，丢的是尾部整段）。

**I-3（REDO 点后首改必 FPW）**：`full_page_writes=on` 时，REDO 点之后每个页的第一条修改记录必须带 `BKPIMAGE_APPLY` 镜像。
维护者：`XLogRecordAssemble` 的 `page_lsn <= RedoRecPtr` 判定（xloginsert.c:694）+ `XLogInsertRecord` 的持锁复核（xlog.c:885-896）+ `XLOG_CHECKPOINT_REDO` 插入与 RedoRecPtr 推进在同一临界区（xlog.c:937-940）。
违反后果：撕裂页 + 增量重放 = §5.2 的静默损坏。

**I-4（checkpoint 生效顺序）**：脏页刷盘完成 → checkpoint 记录 fsync → pg_control 更新 → 旧 WAL 删除，四步严格有序。
维护者：`CreateCheckPoint` 的语句顺序（§7.1 ⑦→⑧→⑨→⑩）。
违反后果：任一倒置都产生"恢复起点声明与磁盘实态不符"：pg_control 先行 ⇒ REDO 点前有脏页未落盘，恢复不重放它们 ⇒ 丢数据；删 WAL 先行 ⇒ 恢复起点之后无日志可放。

**I-5（commit 记录与 CLOG 的 checkpoint 一致性）**：commit 记录在 REDO 点之前的事务，其 CLOG 更新必须被本次 checkpoint 刷盘。
维护者：`delayChkptFlags` 举手 + checkpoint 的 10ms 轮询等待（xlog.c:7695-7712）。
违反后果：已确认提交的事务在恢复后 CLOG 状态为未提交 ⇒ 整个事务的修改集体"蒸发"（可见性过滤），客户端视角的数据丢失。

**I-6（恢复幂等）**：任何 WAL 记录对任何页重放任意多次，最终页状态相同。
维护者：`XLogReadBufferForRedoExtended` 的 LSN 比较与镜像覆盖（§8.4）+ 每个 rmgr redo 在修改后必须 `PageSetLSN`。
违反后果：恢复中途崩溃后的二次恢复（§12.3）会重复施加修改——重复插入行指针、重复递增计数，页结构损坏。

**I-7（LSN 全序无洞）**：WAL 字节流中记录首尾相接，`xl_prev` 链完整，刷盘的前缀内无未完成拷贝。
维护者：`ReserveXLogInsertLocation` 的串行预留 + `WaitXLogInsertionsToFinish` 的水位收敛（§3.2）+ `CopyXLogRecordToWAL` 尾部的 `CurrPos == EndPos` PANIC 检查。
违反后果：恢复读到"洞"里的垃圾——最好情况 CRC 失败提前截断（丢已提交事务），最坏情况垃圾恰好像一条合法记录。

**I-8（恢复期间不写 WAL）**：恢复态的任何进程不得插入 WAL 记录（end-of-recovery 例外，由 `LocalSetXLogInsertAllowed` 临时豁免）。
维护者：`XLogInsertRecord` 开头的 `XLogInsertAllowed()` 检查（xlog.c:815-816）；恢复态 `XLogFlush` 改道 minRecoveryPoint（xlog.c:2813-2817）。
违反后果：重放位置和插入位置在同一条流上互相踩踏；备库与主库的 WAL 分叉。

**I-9（数据页领先必须封顶，备库）**：备库刷过盘的数据页所含效果，不得超过 pg_control 的 minRecoveryPoint；备库重启后必须至少重放到 minRecoveryPoint 才能开门。
维护者：恢复态 `XLogFlush`→`UpdateMinRecoveryPoint`（xlog.c:2769-2788）。
违反后果：备库崩溃重启后在"数据比已重放 WAL 新"的状态下提供只读服务——查询看到未来数据，提升后时间线错乱。

---

## 11. 设计权衡与历史：git 考古

以下 commit 均可在本仓库 `git show` 验证（日期为 commit date）。

**WAL 并行插入的三级火箭**：

- `9a20a9b21b`（2013-07-08，Heikki Linnakangas，PG 9.4）"Improve scalability of WAL insertions."——本章 §3 的一切由此而来。9.4 之前整个"预留 + 拷贝"都在一把 `WALInsertLock` 大锁里，多核高并发下 WAL 插入是全局第一瓶颈。此 commit 引入"自旋锁字节预留 + N 个 insertion slot 并行拷贝 + insertingAt 水位"的架构；
- `68a2e52bba`（2014-03-21，Heikki）"Replace the XLogInsert slots with regular LWLocks."——把 9.4 开发周期内发明的专用 slot 原语收编进 LWLock 家族（新增 `LWLockWaitForVar`/`LWLockUpdateVar`），减少重复的等待队列代码。§3.3 用到的正是这套 API；
- `5fa6c81a43`（2014-10-01）"Remove num_xloginsert_locks GUC, replace with a #define"——发布前把插入锁数量从 GUC 固化为 `NUM_XLOGINSERT_LOCKS = 8`（xlog.c:157）：测试显示 8 已够消除竞争，暴露成旋钮只会诱导误调（更多锁 = 刷盘者要扫更多水位）；
- 后续微优化：`71e4cc6b8e`（2023-07-25，PG 17）用原子操作替代 insertingAt 更新路径上的自旋锁；`f3ff7bf83b`（2024-04-07，PG 17）加 `XLogCtl->logInsertResult` 共享缓存，给 `WaitXLogInsertionsToFinish` 装上快路径（§3.3 ①）。一条主线：**十年间不断削薄定序临界区**。

**组提交**：

- `9b38d46d9f`（2012-01-30，Heikki，PG 9.2）"Make group commit more effective."——引入"等锁者醒来先复查、不重复 fsync"的协议（配套原语 `1a01560cbb` 定名 `LWLockAcquireOrWait`）。在此之前 fsync 队列是串行的，fsync 慢盘上 N 个并发提交近似 N 次 fsync；之后近似 1 次。§4.1 的 (a)(c)(d) 就是这个 commit 的遗产；
- `f11e8be3e8`（2012-07-02）"Make commit_delay much smarter."——把 `commit_delay` 的睡眠从"每个提交者都睡"改成"只有拿到 WALWriteLock 的组长睡"，старый 版本的 commit_delay 因此从"几乎总是有害"变成"偶尔有用"（§4.1 (e)）。

**FPW 的代价控制**：

- `57aa5b2bb1`（2015-03-11，PG 9.5）"Add GUC to enable compression of full page images stored in WAL."——`wal_compression` 诞生，用 CPU 换 FPI 体积（FPI 常占 WAL 一半以上）；
- `4035cd5d4e`（2021-06-29，PG 14）与 `e9537321a7`（2022-03-11，PG 15）先后加入 LZ4 和 zstd——pglz 压缩比一般且慢，现代算法让 `wal_compression` 从"谨慎开启"变成"多数场景推荐"。§1.3 的 `BKPIMAGE_COMPRESS_*` 三个位就是这段历史的地层剖面。

**checkpoint 协议**：

- `6ef2eba3f5`（2016-12-22）"Skip checkpoints, archiving on idle systems."——利用插入锁上顺手维护的 `lastImportantAt`（零锁开销）实现 §7.1 ② 的空闲跳过；此前空闲系统每个 `checkpoint_timeout` 都产生 checkpoint 记录 + 段切换 + 归档，永不安静；
- `afd12774ae`（2023-10-19，Robert Haas，PG 17）"During online checkpoints, insert XLOG_CHECKPOINT_REDO at redo point."——REDO 点从"虚拟位置"变成流内显式记录（§7.1 ④）。直接动机是给增量备份的 WAL summarizer（`174c480508`）一个可靠的 checkpoint 边界；副产品是恢复起点的交叉校验（§8.2 ②）。这是"WAL 不只是恢复日志，更是系统的公共事件流"这一趋势的注脚——越多下游（复制、CDC、备份）消费 WAL，流内自描述就越重要。

### 小结

WAL 写路径的历史是一条"持续削薄串行段"的单行道：大锁 → 预留/拷贝分离（9.4）→ 原语收编（9.4/9.5）→ 原子化与缓存（17）。组提交与 commit_delay 在 9.2 定型。FPW 的代价问题用压缩算法的代际升级消化。checkpoint 协议的近期演化（显式 REDO 记录）由增量备份这类新下游驱动。

---

## 12. what-if 推演

### 12.1 恢复中途 WAL 段缺失/损坏——恢复停在哪，数据什么状态？

**场景**：崩溃恢复需要重放段 `...0C5`、`...0C6`、`...0C7`，但 `...0C6` 被误删（或中间某页被磁盘位翻转破坏）。

**推演**：主循环在 `...0C5` 尾部调 `ReadRecord` 取下一条——文件打不开/页头校验失败/记录 CRC 失败/`xl_prev` 不匹配，统统返回 NULL（崩溃恢复模式）。按 §8.2 ④ 的循环条件，**这被当作"WAL 的自然末尾"**：打出 `redo done at ...`，做 end-of-recovery checkpoint，开门。
**数据状态**：数据库一致（截至 `...0C5` 末尾的前缀历史被完整重演），但 `...0C6` 起的所有已提交事务**静默消失**——没有任何错误提示，因为 PG 无法区分"日志被破坏"与"崩溃时恰好只写到这里"（两者在读取侧同构；这正是 WAL 用"第一条坏记录"定义末尾的原理性代价，§1.2）。更阴险的次生灾害：如果 `...0C7` 还在且其后有段存在，它们与新历史分叉——PG 用**新时间线/段回收**避免它们被误读，但人工从备份混拷段文件时可能踩中。
**对照**：归档恢复/备库模式下同样的读失败不会被当作末尾——`ReadRecord` 会换源重试（归档→pg_wal→流复制）并等待，所以 PITR 缺段表现为**恢复卡住反复报 restore_command 失败**，而不是静默截断——这是"我知道日志应该还有"（有归档清单）与"我不知道日志到哪"（裸崩溃）的信息差。

### 12.2 fsync 骗子磁盘（写缓存丢电）——哪个假设被打破，什么后果？

**场景**：消费级 SSD/RAID 卡开着易失写缓存且谎报 fsync 完成（数据只到缓存就返回）。断电，缓存里最后几百毫秒的写全部蒸发，且**蒸发是乱序的**（缓存回写顺序与 fsync 顺序无关）。

**推演**：PG 的全部正确性都建立在"fsync 返回 ⇒ 数据持久"上，逐条检查哪些不变式被击穿：
- I-2 击穿：已应答的 COMMIT 对应的 WAL 尾巴消失——丢事务，但这只是最温和的一层；
- I-1 击穿（**致命**）：`FlushBuffer` 的顺序是"先 `XLogFlush` 后写页"，但骗子缓存可能让**页落了盘而 WAL 没落**（页写比 WAL 的 fsync 晚发出，却先被缓存刷出）。结果：页领先于日志，§10 I-1 的后果——该页的 LSN 判定从此系统性说谎，后续恢复静默跳过本该重放的记录；
- I-4 击穿：checkpoint 的"脏页 → checkpoint 记录 → pg_control"三步顺序被缓存乱序作废——pg_control 可能宣称一个"其脏页并未真正落盘"的 REDO 点。此时 I-3 的 FPW 也救不了：FPW 假设"REDO 点之前的页完好"，而现在 REDO 点之前的页可能整页丢失（不是撕裂，是回退到更旧版本）——若该页在 REDO 点后未被再改，没有镜像会来覆盖它。
**结论**：软件层无解——PG 的协议只排列 fsync 的**顺序**，无法验证其**真实性**。这就是 `pg_test_fsync` 存在的意义（跑出离谱的高 IOPS = 大概率在说谎）、企业盘用电容保护写缓存的意义。相关的真实教训是 2018 年的 fsyncgate：Linux 的 fsync 失败后会**清除错误标志**，重试的 fsync 会"成功"——PG 自 `9ccdd7f6` 起把刷脏页时的 fsync 失败直接升级为 PANIC（data_sync_retry），宁可崩溃重放也不信第二次 fsync。

### 12.3 恢复重放到一半再次崩溃——为什么安全？

**场景**：恢复重放了 60% 的 WAL，其间 checkpointer 可能刷了一些页，又断电。

**推演**：第二次恢复面对的磁盘状态是三种页的混合：A. 从未被本轮恢复碰过的页（还是崩溃前状态）；B. 被恢复改过且被刷盘的页（LSN 已推进）；C. 被恢复改过但只在缓冲池里、没刷盘的页（磁盘上仍是旧状态——缓冲池随断电蒸发，等价于 A）。
第二次恢复的起点：pg_control 没变（end-of-recovery checkpoint 还没做过，I-4 保证 pg_control 只在 checkpoint 完成后推进），所以**仍从原 REDO 点开始**，重放同样的记录序列。对 A/C 类页：LSN 还是旧的，`BLK_NEEDS_REDO`，正常重放——与第一次恢复无异。对 B 类页：`lsn <= PageGetLSN(page)` 成立，`BLK_DONE` 跳过，直到重放进度追上该页的 LSN 后恢复 `BLK_NEEDS_REDO`。带镜像的记录则无条件覆盖（`BLK_RESTORED`），连判断都不用。
**安全的根源就是不变式 I-6（幂等）+ I-4（起点不提前推进）**：恢复过程自身不产生新的"历史"，它只是把页集合从"参差不齐的旧状态"单调推向"WAL 末尾状态"，这个推进函数是幂等的、可从任意中间点重启的。同理，恢复期间备库的 restartpoint 也只在主库 checkpoint 重放完之后才推进起点——任何时刻断电，起点之前的效果都已落盘。
**对照 ARIES**：ARIES 的 redo 同样幂等（pageLSN 判定即源自 ARIES），但 undo 阶段需要 CLR 来记录"回滚进行到哪"才能安全重启——PG 没有 undo，连这个问题都不存在。

### 12.4 archive_command 持续失败——连锁反应到宕机

**场景**：归档目标 NFS 挂了 / 对象存储凭证过期，`archive_command` 每次返回非零。

**推演链条**：
1. archiver 无限重试当前段（失败退避，pgarch.c 的循环），`.ready` 文件开始堆积；
2. **WAL 回收被阻断**：checkpoint 的第 ⑩ 步（xlog.c:7850-7864）只能回收"已归档"的段——`.ready` 未变 `.done` 的段被 `KeepLogSeg`/`RemoveOldXlogFiles` 跳过。`max_wal_size` 是**软限制**，此时形同虚设，`pg_wal` 只增不减；
3. 若有基于 WAL 保留的复制槽，叠加保留需求（同一步里 `InvalidateObsoleteReplicationSlots` 可按 `max_slot_wal_keep_size` 断槽自保，但归档堆积没有对应的自动断头机制——**归档积压没有"最大保留量"旋钮**）；
4. `pg_wal` 所在文件系统写满。下一次 WAL 段创建/写入失败——§4.2 第 2 步：**写 WAL 失败 = PANIC**，实例硬宕机；
5. **宕机后更糟**：崩溃恢复的 end-of-recovery checkpoint 也要写 WAL，磁盘还是满的 ⇒ 恢复本身 PANIC，实例起不来。运维此时的正确动作是扩容/清理**归档积压之外**的空间或修好归档后让 archiver 消化积压；历史上最经典的事故操作——手删 `pg_wal` 里"看起来很旧"的段——会把 REDO 点之后的日志删掉，从"暂时起不来"恶化为"需要从备份重建"（§12.1）。
**教训**：监控必盯 `pg_stat_archiver.failed_count` 与 `archive_status/*.ready` 计数；归档失败是**慢性毒药**，从第一次失败到宕机可能隔着几小时到几天，而告警窗口全在这段时间里。

### 12.5 附：full_page_writes=off 但硬件不保证原子写

**推演**：一切正常运行——FPW 只在恢复时才兑现价值。直到某次断电产生撕裂页（§5.1），恢复对它做增量重放或误判 BLK_DONE（§5.2），损坏悄悄埋进数据。**可能几个月后**才由某次全表扫描/pg_checksums 报出 `invalid page in block ...`，而此时最早的干净备份可能已过保留期。这是所有 what-if 里潜伏期最长的一个：错误配置与灾难兑现之间隔着"下一次不巧的断电"。云盘/ZFS 等确知 ≥8KB 原子写的环境才可关闭。

---

## 自测问题（附详解）

**1. 为什么 `XLogRecordAssemble` 用 `page_lsn <= RedoRecPtr` 判定是否写整页镜像？两个方向的错误各会导致什么？**

页 LSN ≤ REDO 点说明该页自最近 checkpoint 后未被改过，本次是首次修改；崩溃时它可能撕裂，而恢复恰好要从 REDO 点开始基于该页做增量重放，故必须记整页（不变式 I-3）。漏记（该记时没记）：恢复可能在撕裂页上做增量重放或因页 LSN 被半写而误判 BLK_DONE——静默损坏（§5.2）。多记（不该记时也记）：只损失 WAL 空间与带宽，正确性无损。正因这个不对称，`XLogInsertRecord` 复核发现 REDO 点变化时宁可返回 Invalid 全部重来（xlog.c:885-896），而 doPageWrites 被关闭的反方向变化不触发重组（xlog.c:872-876）。

**2. `ReserveXLogInsertLocation` 的自旋锁临界区里为什么只有五次赋值？"可用字节位置"技巧省掉了什么？**

临界区是全库所有 WAL 记录的必经串行段，Amdahl 定律下它的每纳秒都直接决定写入扩展性上限。`CurrBytePos` 存"扣除全部页头的可用字节位置"，使预留退化为 `CurrBytePos += size` 一个加法；而"可用位置 ↔ 物理 LSN"的换算（要对 `UsableBytesInSegment` 做除法、考虑长短页头）全部搬到锁外由各进程并行完成（xlog.c:1162-1170 注释，换算函数 xlog.c:1899/1939/1982）。同一临界区顺手维护 `PrevBytePos`，让 `xl_prev` 反向链零成本生成。MAXALIGN 也在锁外做。

**3. 完整论证：8 槽 WALInsertLock 下写者乱序完成，刷盘者如何知道"到哪里是连续的"？为什么 insertingAt 的惰性更新不破坏正确性、反而防死锁？**

预留全序但完成乱序，字节流上可能有洞。刷盘者（`WaitXLogInsertionsToFinish`）从 `reservedUpto` 出发，扫 8 把锁：空闲锁无约束；`insertingAt ≥ upto` 的持有者其整个工作区间都在目标之后（预留全序保证其起点 ≥ insertingAt），可忽略；`insertingAt < upto`（含 0）的必须等其推进或放锁，并用等到的水位收紧答案 `finishedUpto`。结束时 `finishedUpto` 之前无在途插入 ⇒ 连续前缀。惰性更新（只在持锁睡眠前更新水位）不破坏正确性：不更新只会让水位偏保守（0 = 最保守），刷盘者多等、不会少等。防死锁：插入者腾 WAL 缓冲区需要刷盘、刷盘需要等插入者——若"等"是等所有人，两个都在腾缓冲区的插入者会互相等；水位把"等人"细化为"等特定字节范围的人"，而被等者睡眠前已公布"我在写哪"，环路必断（xlog.c:353-363 注释）。

**4. `XLogFlush` 里 `LWLockAcquireOrWait` 与普通 `LWLockAcquire` 的行为差异是什么？它如何造就组提交？**

普通 Acquire：锁释放时等待者**持锁**醒来，必然执行自己的临界区 ⇒ N 个提交者串行 N 次 fsync。AcquireOrWait：锁被占时睡到释放，但醒来**不持锁**、返回 false ⇒ 调用者回到循环头先查 `LogwrtResult.Flush`——前任组长按协议刷到了"当时最远的连续就绪点"（不只是它自己的 record），大概率已覆盖我 ⇒ 直接 break，零系统调用。于是 fsync 次数从 O(提交数) 变成 O(fsync 时长内的批次数)。第三层 `commit_delay` 让组长睡一觉多攒人，睡醒再推一次水位（此时的 `WaitXLogInsertionsToFinish` 不会真等人，xlog.c:2908-2916）。（§4.1；commit 9b38d46d9f）

**5. 在线 checkpoint 的 REDO 点为什么必须在刷脏页开始之前确定？`XLOG_CHECKPOINT_REDO` 记录的插入为什么要与 RedoRecPtr 推进在同一临界区？**

刷脏页期间系统继续写入，这些修改可能没赶上本轮刷盘，必须从刷盘**开始前**的位置重放才完整——所以 REDO 点在前、checkpoint 记录在后（§7.1 ④⑧）。同一临界区（持全部插入锁，xlog.c:937-940）保证不变式：新 REDO 点生效瞬间不存在"按旧 REDO 点做的 FPW 判定、却排在新 REDO 点之后"的在途记录——要么已完成插入（在 REDO 点前），要么会在持锁复核中被打回重组。若拆成两步，缝隙里溜进去的记录可能该做镜像而没做，I-3 破产。恢复侧从 PG 17 起还校验 REDO 点处必须真的是一条 REDO 记录（xlogrecovery.c:1674-1678），pg_control 错位立刻 FATAL。

**6. `XLogReadBufferForRedo` 四种返回值分别在什么场景出现？为什么 `BLK_DONE` 的比较必须用记录的 EndRecPtr？**

BLK_NEEDS_REDO：页落后于记录，正常重放；BLK_DONE：上次恢复已重放此页并刷盘（或备库上页本就领先），跳过；BLK_RESTORED：带 `BKPIMAGE_APPLY` 镜像，零读盘整页覆盖（顺带修复撕裂）；BLK_NOTFOUND：页/文件已被 WAL 流中更晚的 truncate/drop 消灭，跳过（§8.4 表格）。用 EndRecPtr 是因为正向路径 `PageSetLSN(page, recptr)` 写的就是 `XLogInsert` 返回的记录**结束**位置（§2.1）——两边同一把尺子，"页 LSN ≥ 记录 EndRecPtr ⇔ 页已含此记录效果"才严格成立；若一边用起点一边用终点，恰好等于的边界会误判。

**7. PostgreSQL 的恢复为什么没有 undo 阶段？"回滚免费"的代价由谁支付？InnoDB 在同一问题上的收支如何？**

no-overwrite 存储下 UPDATE 不覆盖旧版本，未提交元组落盘也只是堆里一个 `xmin` 未提交的版本；CLOG 里该 XID 无 COMMITTED 位，MVCC 可见性自动过滤，无需物理擦除（§6.2 五步推演）。回滚 = 在 CLOG 标 ABORTED（甚至不标）。代价转移给 VACUUM：死元组占据堆空间等待回收，长事务下体现为表膨胀。InnoDB 反向：原地更新 + undo 链，崩溃恢复必须 redo 之后再 undo（回滚 loser 事务、需要 CLR 保证 undo 幂等），大事务回滚昂贵、undo 表空间会被长事务撑爆；换来的是堆里没有死元组、purge 有序。两边守恒：旧版本管理的成本要么在恢复/回滚路径（InnoDB），要么在后台清理路径（PG）。

**8. 崩溃恢复结束时为什么要同步做一个 checkpoint 才开门，而备库提升却只写一条 `XLOG_END_OF_RECOVERY` 就开门？**

崩溃恢复：pg_control 还指着崩溃前的 checkpoint，若不做新 checkpoint 就开门后再崩，全部 WAL 要重放第二遍；end-of-recovery checkpoint（本质 shutdown checkpoint）把恢复成果固化成新起点，代价是开门前的一次全量刷脏（`CHECKPOINT_WAIT` 同步等待，xlog.c:6820-6822）。提升：RTO 优先——业务在等新主库，几分钟的 checkpoint 不可接受；轻量记录只标记时间线切换与终点（`CreateEndOfRecoveryRecord` 顺带把 minRecoveryPoint 推到该记录，保证提升不可回退），真正的 checkpoint 异步补做（xlog.c:6701-6702）。风险可控：提升前必有 restartpoint 兜底，再崩不过是多放一段 WAL（§8.6）。

**9. 同一段 WAL 被重放两次为什么不会破坏数据？请分别说明无镜像块、有镜像块，以及"一条记录改两个页"的情形。**

无镜像块：`lsn <= PageGetLSN(page)` 则 BLK_DONE 跳过；每次成功重放都 `PageSetLSN` 推进页版本，保证第二次必然跳过（I-6）。有镜像块（`BKPIMAGE_APPLY`）：`RestoreBlockImage` 覆盖整页再设 LSN——覆盖天然幂等，且不依赖页的现有内容。多页记录（如 `heap_xlog_insert` 的堆页 + VM 页，§8.5 ②）：**幂等以页为单位独立判定**——两页可能一个已刷盘一个没有，重放时一个 BLK_DONE 一个 BLK_NEEDS_REDO，各自收敛到正确状态；所以 rmgr 代码必须对每个块引用独立走 `XLogReadBufferForRedo`，不能用页 A 的判定结果决定页 B 的处理。三者合起来实现 ARIES 的 "repeating history"，且无需 per-page 重放计数或 CLR。
