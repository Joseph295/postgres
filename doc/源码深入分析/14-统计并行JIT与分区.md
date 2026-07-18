# 第 14 篇 · 查询处理的高级模块：统计信息、并行查询、JIT 与分区表

> 对应《PostgreSQL 源码学习指南》第 16 章。源码基线：PostgreSQL 20devel（master 分支）。

## 本章导读

前面的篇章走完了"一条 SQL 的一生"：解析 → 优化 → 执行 → 落盘。本篇讲的是叠加在这条主干上的四个"高级模块"，它们分别回答四个问题：

1. **统计信息**——优化器凭什么估算行数？（`commands/analyze.c` + `statistics/`）
2. **并行查询**——单机多核怎么用起来？它和你熟悉的 Spark/MPP 有什么本质区别？（`access/transam/parallel.c` + `executor/nodeGather*.c` + `storage/ipc/shm_mq.c`）
3. **JIT**——解释执行的表达式怎么编译成机器码？为什么 OLTP 常建议关掉它？（`jit/llvm/`）
4. **分区表**——分区裁剪为什么有三个时机？partitionwise 优化和 Hive 分区有什么异同？（`partitioning/` + `executor/execPartition.c`）

这四个模块有一个共同点：都是"锦上添花"型基础设施——关掉它们 SQL 照样能跑，但打开后分析型负载可能快一个数量级。对大数据工程师来说，这一章是把 Spark/Hive 的直觉映射进单机数据库内核的最佳入口。

## 前置知识

- 执行器的 Volcano 模型（每个节点实现 `ExecProcNode`，逐 tuple 拉取）；
- 优化器的 Path/RelOptInfo 概念，以及选择率（selectivity）如何决定行数估计；
- 进程模型：PG 是多进程架构，没有线程，进程间共享数据靠共享内存（DSM，dynamic shared memory）；
- MVCC 快照：谁能看见哪些元组由快照决定——这解释了为什么并行 worker 必须拿到与 leader 完全相同的快照。

---

## 14.1 统计信息：优化器的"眼睛"

### 14.1.1 ANALYZE 的整体流程

入口在 `src/backend/commands/analyze.c`：

```
analyze_rel()            analyze.c:110   -- 加锁、权限检查、决定用哪个采样函数
  └─ do_analyze_rel()    analyze.c:306   -- 主流程
       ├─ examine_attribute() → std_typanalyze()  analyze.c:1950  -- 每列选定“怎么算”
       ├─ acquire_sample_rows()           analyze.c:1262  -- 两阶段随机采样
       ├─ compute_scalar_stats() 等       analyze.c:2461  -- 对样本计算统计量
       ├─ update_attstats()               analyze.c:1714  -- 写入 pg_statistic
       └─ BuildRelationExtStatistics()    statistics/extended_stats.c  -- 扩展统计
```

一个关键常数：样本量 = `300 × statistics_target`。`std_typanalyze()` 中 `stats->minrows = 300 * stats->attstattarget`（analyze.c:1999 附近），默认 target 100，因此默认采 30000 行。300 这个系数来自 Chaudhuri 等人关于直方图误差界的论文（源码注释里有引用）——对大数据同学来说，这就是"采样 3 万行就敢对 10 亿行的表下结论"的理论依据：直方图 bin 边界的误差与表大小几乎无关，只与样本量有关。

### 14.1.2 acquire_sample_rows：两阶段采样与 Vitter 水库算法

PG 的采样是**两阶段**的：第一阶段用 Knuth 的 Algorithm S 随机选**块**（`BlockSampler_Init`，实现在 `src/backend/utils/misc/sampling.c`），第二阶段在被选中的块内用 **Vitter 水库采样**选**行**。这样既保证 I/O 有限（最多读 targrows 个块），又保证行级别的等概率。

逐段走读 `acquire_sample_rows()`（analyze.c:1262）：

```c
	totalblocks = RelationGetNumberOfBlocks(onerel);

	/* Prepare for sampling block numbers */
	randseed = pg_prng_uint32(&pg_global_prng_state);
	nblocks = BlockSampler_Init(&bs, totalblocks, targrows, randseed);
	...
	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	scan = table_beginscan_analyze(onerel);
	slot = table_slot_create(onerel, NULL);
```

- `BlockSampler_Init`：初始化块级采样器，最多选 `targrows` 个块（每块至少能出 1 行样本，所以块数不需要超过目标行数）；
- `reservoir_init_selection_state`（sampling.c:133）：初始化 Vitter 算法的状态 W——这是 Vitter 1985 年论文《Random sampling with a reservoir》里 Algorithm Z 的初始值；
- 20devel 这里还接入了 `read_stream_begin_relation`（analyze.c:1303 附近）——PG 17 引入的流式预读框架，把"下一个要读哪个块"交给 `block_sampling_read_stream_next` 回调，使采样 I/O 也能异步预取。

核心循环（analyze.c:1318-1358）：

```c
			if (numrows < targrows)
				rows[numrows++] = ExecCopySlotHeapTuple(slot);
			else
			{
				if (rowstoskip < 0)
					rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

				if (rowstoskip <= 0)
				{
					/*
					 * Found a suitable tuple, so save it, replacing one old
					 * tuple at random
					 */
					int	k = (int) (targrows * sampler_random_fract(&rstate.randstate));

					Assert(k >= 0 && k < targrows);
					heap_freetuple(rows[k]);
					rows[k] = ExecCopySlotHeapTuple(slot);
				}
				rowstoskip -= 1;
			}
			samplerows += 1;
```

逐行解说：

- 前 `targrows` 行直接填满"水库"（`rows[]` 数组）；
- 水库满了以后，不是对每一行都掷一次骰子，而是调用 `reservoir_get_next_S()`（sampling.c:147）**一次性算出接下来要跳过多少行**——这是 Vitter 算法对朴素水库采样（Algorithm R）的关键改进，把每行一次随机数调用降为每次"命中"一次，对亿行大表意义重大；
- 命中时随机选水库里的一个下标 `k` 替换掉——数学上可证明：任意时刻水库都是已扫过行的均匀随机样本；
- 循环结束后，如果水库真的被替换过，样本行的物理顺序被打乱了，需要 `qsort_interruptible(..., compare_rows, ...)`（analyze.c:1380 附近）按 TID 排回物理顺序——**后面算相关性（correlation）依赖这个顺序**。

最后按"扫过的块的平均行密度"外推 `totalrows`（analyze.c:1390 附近）：`*totalrows = floor((liverows / bs.m) * totalblocks + 0.5)`，这就是 `pg_class.reltuples` 的来源。

### 14.1.3 compute_scalar_stats：从样本到 MCV/直方图/相关性

对有序类型（有 `<` 操作符）的列，`std_typanalyze()` 选择 `compute_scalar_stats()`（analyze.c:2461）；只有 `=` 没有 `<` 的类型退化为 `compute_distinct_stats()`（analyze.c:2118，无直方图）。

`compute_scalar_stats` 的流程：

1. **抽值 + 排序**：把非 NULL 值收进 `values[]` 数组，用 `PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup)`（analyze.c:2502）拿到该类型 `<` 操作符对应的比较器排序。排序时顺便记录每个值来自第几行样本（`tupno`），供相关性计算用；
2. **ndistinct 估计**：扫描排序后的数组统计"出现一次的值的个数 f1"和"总 distinct 数 d"，代入 **Haas-Stokes 的 Duj1 估计器**（analyze.c:2669 附近的注释与代码）：

```c
			int		f1 = ndistinct - nmultiple + toowide_cnt;
			int		d = f1 + nmultiple;
			double	n = samplerows - null_cnt;
			double	N = totalrows * (1.0 - stats->stanullfrac);
			double	stadistinct;

			if (N > 0)
				stadistinct = (n * d) / ((n - f1) + f1 * n / N);
```

   即 `n*d / (n - f1 + f1*n/N)`。直觉：样本里"只出现一次"的值越多，说明表里还有大量没见过的值，估计值就要往上抬。若估出的 distinct 数超过总行数的 10%，则改存**负数比例**（`stadistinct = -(stadistinct/totalrows)`，analyze.c:2716 附近）——表示"distinct 数随表增长而线性增长"（比如唯一列存 -1）。这个正负号约定在 `pg_statistic.h:55-71` 的注释里有完整说明；

3. **MCV（最常见值）**：排序过程中用 `track[]` 数组追踪出现次数最多的 `num_mcv`（= statistics_target）个值。是否全部入选由 `analyze_mcv_list()` 决定（analyze.c:2756 附近调用）：只保留"比平均频率显著更常见"的值，避免把噪声写进统计。若样本能完整覆盖列的取值域（枚举型列），则整个值域入 MCV，此时优化器拥有**完备信息**；
4. **直方图**：把已进 MCV 的值剔除后，剩余值做**等高直方图**（equi-height）：`num_hist = ndistinct - num_mcv`，上限 `num_bins + 1` 个边界（analyze.c:2803-2805），从排序数组中等间隔取值作为 bucket 边界，`stakind = STATISTIC_KIND_HISTOGRAM`（analyze.c:2899）。等高（每桶行数相同）而非等宽，才能保证倾斜数据下每桶的估计误差一致——与 NumPy/Hive 默认的等宽直方图不同；
5. **相关性（correlation）**：利用排序时记下的 `(值序 i, 物理序 tupno)` 对，累加 `corr_xysum += ((double) i) * ((double) tupno)`（analyze.c:2602），最后用皮尔逊相关系数的化简公式（analyze.c:2925-2943）：

```c
			corr_xsum = ((double) (values_cnt - 1)) * ((double) values_cnt) / 2.0;
			corr_x2sum = ((double) (values_cnt - 1)) * ((double) values_cnt) *
				(double) (2 * values_cnt - 1) / 6.0;

			/* And the correlation coefficient reduces to */
			corrs[0] = (values_cnt * corr_xysum - corr_xsum * corr_xsum) /
				(values_cnt * corr_x2sum - corr_xsum * corr_xsum);

			stats->stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
```

   因为 x、y 都恰好是 `0..values_cnt-1` 的排列，Σx、Σx² 有闭式解，整个公式只剩一个交叉项。correlation ∈ [-1, 1]，衡量"逻辑序和物理序的一致程度"——`cost_index()` 用它在"顺序读代价"和"随机读代价"之间插值，这就是为什么对时序表（天然物理有序）索引扫描估价便宜。

### 14.1.4 pg_statistic 的存储格式：5 个通用槽位

`src/include/catalog/pg_statistic.h` 定义的表结构是一套**类型无关的槽位系统**：

- 固定列：`stanullfrac`（NULL 比例）、`stawidth`（平均宽度）、`stadistinct`（pg_statistic.h:71，正负号语义见上）；
- **5 个槽位**（`STATISTIC_NUM_SLOTS = 5`，pg_statistic.h:131），每个槽位是一个五元组：`stakindN`（种类标签，pg_statistic.h:90）、`staopN`（关联操作符）、`stacollN`、`stanumbersN`（float4 数组，pg_statistic.h:109）、`stavaluesN`（`anyarray` 伪类型，pg_statistic.h:121——这是系统目录里少数用 anyarray 的地方，因为要存任意列类型的值数组）。

内建种类：`STATISTIC_KIND_MCV = 1`（stavalues 存值、stanumbers 存频率）、`STATISTIC_KIND_HISTOGRAM = 2`（stavalues 存桶边界）、`STATISTIC_KIND_CORRELATION = 3`（stanumbers 存单个相关系数）（pg_statistic.h:194/214/226）。槽位是"先到先得"的开放格式——数组类型会写入种类 4/5（元素级 MCV、长度直方图），范围类型写 6/7，扩展和自定义 typanalyze 也可以定义自己的 kind。消费端是 `utils/adt/selfuncs.c`：`eqsel` 查 MCV，`scalarltsel` 查直方图插值。

用 SQL 看它：`SELECT * FROM pg_stats WHERE tablename='t';`（`pg_stats` 是对 pg_statistic 的可读视图，做了权限过滤）。

### 14.1.5 扩展统计：修正"列间独立"假设

单列统计的致命假设：多个谓词的选择率**相乘**（独立假设）。`WHERE city='北京' AND province='北京市'` 会被估成 P(city)×P(province)，低估几个数量级——这是优化器估错行数的头号原因。`CREATE STATISTICS` 建的扩展统计就是解药，实现在 `src/backend/statistics/`：

| 种类 | 文件 | 存什么 | 修正什么 |
|---|---|---|---|
| `ndistinct` | mvdistinct.c（构建入口 `statext_ndistinct_build`，mvdistinct.c:85） | 各列组合的 distinct 数 | GROUP BY 多列的组数估计 |
| `dependencies` | dependencies.c | 函数依赖度（a→b 的"决定程度"，0~1 的 degree） | AND 连接的等值谓词 |
| `mcv` | mcv.c | **多列联合 MCV 列表** | 最精确：直接查联合频率，支持不等值 |

接入点在优化器的选择率总入口 `clauselist_selectivity()`（`src/backend/optimizer/path/clausesel.c:100` → `clauselist_selectivity_ext`，clausesel.c:117）：

```c
	rel = find_single_rel_for_clauses(root, clauses);
	if (use_extended_stats && rel && rel->rtekind == RTE_RELATION && rel->statlist != NIL)
	{
		s1 = statext_clauselist_selectivity(root, clauses, varRelid,
											jointype, sjinfo, rel,
											&estimatedclauses, false);
	}
```

- 只有当所有子句引用**同一个基表**且该表建了扩展统计（`rel->statlist`）时才启用；
- `statext_clauselist_selectivity()`（extended_stats.c:2024）内部的次序很讲究：**先 MCV 后依赖**——先 `statext_mcv_clauselist_selectivity()`（extended_stats.c:1737 → mcv.c:2043 的 `mcv_clauselist_selectivity`），能被联合 MCV 覆盖的子句拿到精确联合频率；剩下的交给 `dependencies_clauselist_selectivity()`（dependencies.c:1350），用依赖度公式 `P(a=x ∧ b=y) = P(a=x) × [degree + (1-degree) × P(b=y)]` 修正；
- 被扩展统计估过的子句下标记入 `estimatedclauses` 位图，后续单列逻辑跳过它们（clausesel.c:158 之后的循环），避免重复计入。

三个 README（`statistics/README`、`README.dependencies`、`README.mcv`）是极好的算法说明,建议通读。

### 小结

- ANALYZE = 块级 Knuth S + 行级 Vitter 水库两阶段采样，默认 30000 行样本，理论依据是直方图误差界与表大小无关；
- `compute_scalar_stats` 一次排序产出四种统计量：Haas-Stokes 的 ndistinct、显著性筛选后的 MCV、等高直方图、物理-逻辑序相关系数；
- `pg_statistic` 是 5 槽位的开放格式，`anyarray` 让它类型无关；
- 扩展统计在 `clauselist_selectivity` 处拦截多列子句，按"MCV 优先、依赖兜底"的次序修正独立假设。

---

## 14.2 并行查询：单机版 scatter-gather

### 14.2.1 基础设施：parallel.c 到底把什么序列化给了 worker

PG 的并行 worker 是**全新 fork 出的进程**，不共享 leader 的任何私有内存。所以 leader 必须把"让 worker 表现得像自己"所需的全部状态打包进 DSM（动态共享内存段）。看 `src/backend/access/transam/parallel.c:67-81` 的 key 清单就知道打包了什么：

```c
#define PARALLEL_KEY_FIXED					UINT64CONST(0xFFFFFFFFFFFF0001)
#define PARALLEL_KEY_ERROR_QUEUE			UINT64CONST(0xFFFFFFFFFFFF0002)
#define PARALLEL_KEY_LIBRARY				UINT64CONST(0xFFFFFFFFFFFF0003)
#define PARALLEL_KEY_GUC					UINT64CONST(0xFFFFFFFFFFFF0004)
#define PARALLEL_KEY_COMBO_CID				UINT64CONST(0xFFFFFFFFFFFF0005)
#define PARALLEL_KEY_TRANSACTION_SNAPSHOT	UINT64CONST(0xFFFFFFFFFFFF0006)
#define PARALLEL_KEY_ACTIVE_SNAPSHOT		UINT64CONST(0xFFFFFFFFFFFF0007)
#define PARALLEL_KEY_TRANSACTION_STATE		UINT64CONST(0xFFFFFFFFFFFF0008)
```

`InitializeParallelDSM()`（parallel.c:213）分两趟：先 `EstimateXXXSpace()` 估算每类状态的大小并累加进 estimator（parallel.c:276-300 附近），创建 DSM 后再 `SerializeXXX()` + `shm_toc_insert()` 逐项写入（parallel.c:363-497）。TOC（table of contents）就是共享内存里的一个 key→指针目录。序列化的状态包括：

- **已加载的动态库列表**（LIBRARY）——worker 要 load 同样的 .so，扩展的 hook 才能生效；
- **全部非默认 GUC**（GUC）——`work_mem`、`enable_seqscan` 等必须一致，否则 worker 的执行行为会偏离计划假设；
- **组合 CID 映射**（COMBO_CID）——同一事务内先插入再更新同一行时，元组头的 cmin/cmax 会被压缩成 combo cid，映射表在私有内存里，worker 判断"本事务内可见性"必须带上它；
- **两个快照**（TRANSACTION_SNAPSHOT / ACTIVE_SNAPSHOT）——保证 worker 与 leader 看到完全相同的元组集合。函数开头 `GetTransactionSnapshot()` / `GetActiveSnapshot()`（parallel.c:231-232），REPEATABLE READ 以上才需要传事务级快照（`IsolationUsesXactSnapshot()` 分支，parallel.c:283）；
- **事务状态**（TRANSACTION_STATE）——XID、当前用户、子事务栈等，worker 以"参与者"身份加入 leader 的事务但**禁止写**（并行模式下写操作会报错）；
- 另有 pending syncs、REINDEX 状态、relmapper、未提交枚举值、客户端连接信息（parallel.c:77-81）等边角状态。

每个 worker 还有一条**错误队列**（ERROR_QUEUE，每条 16KB）：worker 里 ereport 抛出的错误被序列化后经 shm_mq 传回 leader 重新抛出——这就是并行查询报错时你在客户端能看到 worker 内部错误的原因。之后 `LaunchParallelWorkers()`（parallel.c:583）向 postmaster 注册 bgworker，worker 入口 `ParallelWorkerMain()`（parallel.c:1301）按同样的 key 逐项恢复状态。

执行器层再叠加一层自己的 TOC：`ExecInitParallelPlan()`（`src/backend/executor/execParallel.c:653`）序列化 `PARALLEL_KEY_PLANNEDSTMT`（计划树本身！worker 拿到的是同一棵计划树的副本，execParallel.c:823）、参数、每 worker 一条元组队列（`PARALLEL_KEY_TUPLE_QUEUE`，execParallel.c:642）和 instrumentation 汇总区。

### 14.2.2 执行器侧：Gather 与 GatherMerge

`Gather` 节点（`src/backend/executor/nodeGather.c`）是并行子树与串行上层的边界。`ExecGather()`（nodeGather.c:138）第一次被调用时才真正 `LaunchParallelWorkers`，然后进入 `gather_getnext()`（nodeGather.c:264）：优先从 worker 队列读元组，队列全空时 leader **自己也执行一遍子计划**（`parallel_leader_participation`）。读队列的核心在 `gather_readnext()`（nodeGather.c:312）：

```c
		reader = gatherstate->reader[gatherstate->nextreader];
		tup = TupleQueueReaderNext(reader, true, &readerdone);

		if (readerdone)
		{
			--gatherstate->nreaders;
			if (gatherstate->nreaders == 0)
			{
				ExecShutdownGatherWorkers(gatherstate);
				return NULL;
			}
			memmove(&gatherstate->reader[gatherstate->nextreader], ...);
			continue;
		}
		if (tup)
			return tup;
		/* 该队列暂时无数据：round-robin 换下一个 worker */
		gatherstate->nextreader++;
```

- `TupleQueueReaderNext(..., nowait=true, ...)`（`executor/tqueue.c:176`）非阻塞读；
- 读完的 worker 从数组里 memmove 掉；
- 注释里点明一个性能细节：**尽量黏着同一个队列读**，直到读空才换下一个——比逐 tuple 轮转的 cache 局部性好得多。因此 Gather 的输出顺序是乱的（谁快谁先出）。

`GatherMerge`（nodeGatherMerge.c）解决"子路径有序、汇聚后仍要有序"的场景：每个 worker 产出有序流，leader 用**二叉堆**做 N 路归并（`binaryheap_allocate(nreaders + 1, ...)`，nodeGatherMerge.c:430）。`gather_merge_getnext()`（nodeGatherMerge.c:547）的逻辑就是教科书归并：弹出堆顶所属 reader → 从该 reader 补一条 → 重新入堆。这与 Spark 的 sort-merge 落盘归并、Kafka 消费者多分区按时间戳归并是同一个算法。

### 14.2.3 shm_mq：无锁的单读单写队列

worker → leader 的元组传输走 `src/backend/storage/ipc/shm_mq.c`，文件头注释（shm_mq.c:4）自称 "single-reader, single-writer shared memory message queue"。它的无锁设计浓缩在两个原子变量上（shm_mq.c:78-79）：

```c
	pg_atomic_uint64 mq_bytes_read;      /* 只有 receiver 会改 */
	pg_atomic_uint64 mq_bytes_written;   /* 只有 sender 会改 */
```

shm_mq.c:36-71 的注释解释了正确性论证：环形缓冲区里 `[read, written)` 是未读数据。**每个变量只有一方写**，所以不需要互斥锁，只需要内存屏障保证"先写数据、再推进 written 计数"（sender 侧）与"先读 written、再读数据"（receiver 侧，shm_mq.c:1118 的 `pg_read_barrier()`）的顺序。这是经典的 SPSC ring buffer——和 Disruptor、io_uring 的 SQ/CQ 是同一个模式。API 入口：`shm_mq_create`（shm_mq.c:179）、`shm_mq_send`（shm_mq.c:331，实际走 `shm_mq_sendv`，shm_mq.c:363）、`shm_mq_receive`（shm_mq.c:574）。还有一个批量优化：sender 可以积攒多条消息后再一次性推进 `mq_bytes_written`（`mqh_send_pending`，见 shm_mq.c:357 附近注释），减少原子写和唤醒次数。

每对 (worker, leader) 一条队列，N 个 worker 就是 N 条独立 SPSC 队列——**没有多写单读的争用**，这是"每 worker 一条队列"设计的动机。

### 14.2.4 并行度决策与函数安全标记

**几个 worker？** `compute_parallel_worker()`（`src/backend/optimizer/path/allpaths.c:4973`）：

```c
			heap_parallel_threshold = Max(min_parallel_table_scan_size, 1);
			while (heap_pages >= (BlockNumber) (heap_parallel_threshold * 3))
			{
				heap_parallel_workers++;
				heap_parallel_threshold *= 3;
				...
			}
```

即**按表大小的对数（底为 3）配 worker**：表 ≥ 8MB（`min_parallel_table_scan_size` 默认值）给 1 个，≥ 24MB 给 2 个，≥ 72MB 给 3 个……再与 `max_parallel_workers_per_gather`（默认 2，`optimizer/path/costsize.c:144`）取小。表级 `parallel_workers` reloption 可以直接指定跳过启发式。注释自嘲 "This probably needs to be a good deal more sophisticated"——这确实是全内核最朴素的启发式之一。

**能不能并行？** 每个函数在 `pg_proc.proparallel` 里带一个标记（`src/include/catalog/pg_proc.h:177-179`）：

```c
#define PROPARALLEL_SAFE		's' /* can run in worker or leader */
#define PROPARALLEL_RESTRICTED	'r' /* can run in parallel leader only */
#define PROPARALLEL_UNSAFE		'u' /* banned while in parallel mode */
```

- `safe`：worker 里随便跑；
- `restricted`：只能在 leader 执行（如 `currval`——序列状态在 leader 私有内存里），计划器会把含它的表达式保留在 Gather 之上；
- `unsafe`：整条查询禁用并行（如任何会写库的函数）。

计划器用 `max_parallel_hazard()`（`optimizer/util/clauses.c:763`）对整棵 Query 树做一次遍历取"最坏标记"，决定 `glob->parallelModeOK`；对单个表达式则用 `is_parallel_safe()`（clauses.c:782）判断能否推到 Gather 之下。自定义函数默认 unsafe——写扩展时忘了标 `PARALLEL SAFE` 是"为什么我的查询不并行"的高频答案。

### 14.2.5 与 MPP / Spark 的架构对比

用 Spark 的词汇来描述 PG 并行查询：**只有 scatter-gather，没有 shuffle**。

| 能力 | Spark/MPP | PostgreSQL 并行 |
|---|---|---|
| 数据切分 | 按 key re-partition（exchange） | 只按**物理块**切（Parallel Seq Scan 按页分发） |
| 节点间交换 | 任意 stage 间 shuffle | 只有一种交换：worker → leader（Gather/GatherMerge） |
| 并行 join | 两表都按 join key 重分布 | 一侧广播（Parallel Hash 共享哈希表）或内侧整表每 worker 重复执行 |
| 并行聚合 | shuffle 后按 key 聚合 | **两阶段**：partial + finalize |

关键差异：PG 的 worker 之间**互相不通信**，拓扑是星形的。没有 re-partition exchange 意味着"按 GROUP BY key 把数据重新分给 worker"做不到，PG 的替代方案是**两阶段聚合**：worker 各自对手上的数据做部分聚合（`AGGSPLIT_INITIAL_SERIAL`，输出转移状态），Gather 汇聚后 leader 做 `AGGSPLIT_FINAL_DESERIAL` 合并（`optimizer/plan/planner.c:7606` 的 `create_partial_grouping_paths()`；两个 split 标记见 planner.c:5988/7511）。这正是 Spark `partialAggregate + finalAggregate`（combiner 模式）的单机版——但 Spark 在两阶段之间还有一次 shuffle 使 final 阶段也能并行，PG 的 finalize 只能在 leader 单线程做，所以**高基数 GROUP BY 是 PG 并行聚合的短板**。同理，窗口函数、DISTINCT 等需要"按 key 汇聚"的算子并行能力都受限。理解这一点，就理解了 Citus/Greenplum 这类基于 PG 的 MPP 产品补的正是"跨节点 exchange"这一课。

### 小结

- 并行的本质成本是**状态克隆**：快照、GUC、combo CID、事务状态、库列表全部序列化进 DSM，worker 才能"假装是 leader"；
- 元组回传用每 worker 一条的 SPSC 无锁环形队列（shm_mq），Gather 乱序汇聚、GatherMerge 堆归并保序；
- 并行度按表大小对数决定，函数按 safe/restricted/unsafe 三级标记决定"能不能并行、在哪并行"；
- 架构上是单机 scatter-gather：没有 shuffle，两阶段聚合是无 exchange 条件下的工程替代。

---

## 14.3 JIT：把表达式编译成机器码

### 14.3.1 目录结构与 provider 机制

```
src/backend/jit/
├── jit.c                 -- 与 LLVM 无关的薄壳：按需 dlopen provider
└── llvm/
    ├── llvmjit.c         -- provider 实现：上下文、模块、符号解析、代码发射
    ├── llvmjit_expr.c    -- 表达式树 → LLVM IR（本节主角）
    ├── llvmjit_deform.c  -- 为具体元组描述符生成专用的 tuple deform 函数
    ├── llvmjit_inline.cpp-- 跨函数内联（读 bitcode）
    ├── llvmjit_types.c   -- 用 clang 编译出内核类型的 bitcode“字典”
    └── llvmjit_wrap.cpp / llvmjit_error.cpp / SectionMemoryManager.cpp
```

内核本体**不链接 LLVM**。`jit.c` 只定义回调表：`jit_provider` GUC（默认 "llvmjit"，jit.c:34），第一次需要 JIT 时 `provider_init()`（jit.c:68）拼出 `$pkglibdir/llvmjit.so` 路径 dlopen 之（jit.c:91），失败则静默回退解释执行（并记住失败不再重试，jit.c:81）。`jit_compile_expr()`（jit.c:152）是转发入口。这个"薄壳 + 可插拔 provider"设计让发行版可以把 JIT 拆成独立子包，也给未来更换后端留了口子。

### 14.3.2 llvmjit_expr.c：把 ExprState steps 翻译成 IR

回忆执行器篇：表达式编译产物 `ExprState` 是一个**线性 opcode 数组**（steps），解释器 `ExecInterpExpr` 用 computed goto 逐步执行。JIT 的接入点极其干净——`ExecReadyExpr()`（`executor/execExpr.c:904`）：

```c
static void
ExecReadyExpr(ExprState *state)
{
	if (jit_compile_expr(state))
		return;
	ExecReadyInterpretedExpr(state);
}
```

编译成功则 `state->evalfunc` 指向机器码，失败落回解释器——**上层执行器完全无感知**。

`llvm_compile_expr()`（`jit/llvm/llvmjit_expr.c:80`）的翻译策略是"**每个 step 一个基本块**"：为 steps 数组的每个下标建一个 `opblocks[opno]`，然后 `switch (opcode)`（llvmjit_expr.c:324）逐个把 step 语义展开成 IR。以 `EEOP_QUAL`（WHERE 条件短路）为例（llvmjit_expr.c:972）：

```c
			case EEOP_QUAL:
				{
					...
					v_resvalue = l_load(b, TypeDatum, v_resvaluep, "");
					v_resnull = l_load(b, TypeStorageBool, v_resnullp, "");

					v_nullorfalse =
						LLVMBuildOr(b,
									LLVMBuildICmp(b, LLVMIntEQ, v_resnull,
												  l_sbool_const(1), ""),
									LLVMBuildICmp(b, LLVMIntEQ, v_resvalue,
												  l_datum_const(0), ""),
									"");

					LLVMBuildCondBr(b, v_nullorfalse,
									b_qualfail, opblocks[opno + 1]);
```

逐行解说：加载当前结果值和 NULL 标志 → 构造 "为 NULL 或为 false" 的布尔 → 条件跳转：失败进 `b_qualfail` 块（置 false 后直接跳到 `op->d.qualexpr.jumpdone`，即整个 qual 的出口），成功落到下一个 step 的基本块。对比解释器：同样的语义在解释器里是"取 opcode → 查跳转表 → 间接跳转"，JIT 后变成**直连的条件分支**——消灭了 dispatch 开销，且常量（如比较的目标 opno）都被烘焙进了代码。函数调用类 step（`EEOP_FUNCEXPR`，llvmjit_expr.c:665）则生成对具体 `fmgr` 函数的直接调用。

另一大收益来自 `llvmjit_deform.c`：为**具体表的 TupleDesc** 生成专用解包函数——列数、每列类型、对齐、NULL 位图布局全是编译期常量，循环完全展开。分析查询大量时间耗在 deform 上，这常是 JIT 最大的单项收益。

### 14.3.3 inline：引用内核的 bitcode

光把 step 串起来还不够——`int4pl` 这类小函数如果仍是 call，就吃不到常量传播。`llvm_inline()`（`jit/llvm/llvmjit_inline.cpp:167`）实现跨模块内联：构建期用 clang 把整个后端源码额外编译一份 **LLVM bitcode**，安装到 `$pkglibdir/bitcode/postgres/`（扩展也可以装自己的 bitcode）。运行时按需读取：

- 搜索路径默认加入 `$libdir/postgres`（llvmjit_inline.cpp:192），扩展函数按其模块名加入（llvmjit_inline.cpp:253）；
- 通过汇总索引 `*.index.bc`（llvmjit_inline.cpp:812）定位函数所在模块，再打开 `$pkglibdir/bitcode/<module>` 下对应文件（llvmjit_inline.cpp:492）；
- 把函数体导入当前模块后交给 LLVM 优化器，`strict_deform + int4lt` 这类热路径就能被折叠成几条机器指令。

### 14.3.4 代价联动：什么时候 JIT，以及为什么 OLTP 建议关

是否 JIT 由**计划总代价**决定，逻辑在 `standard_planner()` 末尾（`optimizer/plan/planner.c:698-720`）：

```c
	result->jitFlags = PGJIT_NONE;
	if (jit_enabled && jit_above_cost >= 0 &&
		top_plan->total_cost > jit_above_cost)
	{
		result->jitFlags |= PGJIT_PERFORM;

		if (jit_optimize_above_cost >= 0 &&
			top_plan->total_cost > jit_optimize_above_cost)
			result->jitFlags |= PGJIT_OPT3;
		if (jit_inline_above_cost >= 0 &&
			top_plan->total_cost > jit_inline_above_cost)
			result->jitFlags |= PGJIT_INLINE;
```

三级阈值（默认 100000 / 500000 / 500000）：先决定"编不编"，再决定"要不要开 -O3"和"要不要内联"——因为优化和内联本身很贵，只有更大的查询才摊得平。

**为什么 OLTP 常建议关 JIT？** 三个原因叠加：

1. 编译发生在**每次执行**（编译产物挂在执行态上，不缓存、不跨查询复用），一条毫秒级查询摊上几十毫秒的 LLVM 编译是净亏损；
2. 阈值比较的是**估算代价**——统计一旦失真（比如分区表把所有分区的代价加总），廉价查询也会被误触发 JIT，生产事故报告里"P99 突然多出几百毫秒"很多都归因于此；
3. 代价模型只算"执行有多贵"，从不算"编译要花多久"，两者量纲根本没有对齐。

**PG 18 起的方向**（简述）：18 把 ABI 兼容层进一步收紧并持续跟进 LLVM 新版 ORC API；社区在 18/19 周期里活跃讨论的方向包括**编译产物缓存/复用**（prepared statement 场景摊薄编译成本）与探索比 LLVM 轻得多的后端（如 copy-and-patch 风格的快速基线编译）。方向共识是明确的：让"编译快但代码稍差"的选项存在，JIT 才能对短查询友好。落地进度请以各版本 release notes 为准。

### 小结

- JIT 是可插拔 provider：`jit.c` 薄壳按需 dlopen `llvmjit.so`，失败静默回退解释器；
- `llvm_compile_expr` 按"每 step 一基本块"翻译 opcode 数组，消灭 dispatch 并常量化；deform 专用化常是最大收益；
- inline 靠构建期生成的内核 bitcode 副本，运行时按索引导入函数体；
- 触发与估算代价联动但不计编译成本，因此长分析查询赚、短 OLTP 查询亏——这是"OLTP 关 JIT"建议的根源。

---

## 14.4 分区表：裁剪的三个时机

### 14.4.1 partbounds.c：边界的规范化表示

分区定义（FOR VALUES ...）在内存里被规范化为 `PartitionBoundInfoData`（`src/include/partitioning/partbounds.h:79`）：

```c
typedef struct PartitionBoundInfoData
{
	PartitionStrategy strategy; /* hash, list or range? */
	int			ndatums;		/* Length of the datums[] array */
	Datum	  **datums;
	PartitionRangeDatumKind **kind; /* range 专用：值/MINVALUE/MAXVALUE */
	Bitmapset  *interleaved_parts;	/* list 专用：值交错的分区 */
	int			nindexes;
	int		   *indexes;		/* datums[i] 属于哪个分区 */
	int			null_index;		/* 接收 NULL 的分区下标；-1 表示没有 */
	int			default_index;	/* default 分区下标；-1 表示没有 */
} PartitionBoundInfoData;
```

要点：`datums` 是**排好序**的边界值数组（由 `partition_bounds_create()` 构建，partbounds.c:300），`indexes[i]` 把第 i 个边界映射到分区号。于是"这个值落在哪个分区"就是对 datums 的**二分查找**——list 找相等、range 找上界、hash 取模后查。这个"排序数组 + 二分"的表示同时服务三个消费者：DDL 时检查分区重叠（`partition_bounds_equal`，partbounds.c:889 附近一族函数）、优化器裁剪、执行期元组路由。

### 14.4.2 三阶段裁剪

裁剪回答"这个查询要扫哪几个分区"。难点在于**约束条件里的值什么时候可知**：字面量在计划期可知，`$1` 参数在执行器启动期可知，子查询/参数化连接的值每一行都可能变。PG 的答案是把同一套"裁剪步骤"（pruning steps）在三个时机分别求值：

**阶段一：计划期**。`prune_append_rel_partitions()`（`partitioning/partprune.c:780`）在优化器展开分区表时调用：

```c
Bitmapset *
prune_append_rel_partitions(RelOptInfo *rel)
{
	List	   *clauses = rel->baserestrictinfo;
	List	   *pruning_steps;
	GeneratePruningStepsContext gcontext;
	...
	gen_partprune_steps(rel, clauses, PARTTARGET_PLANNER, &gcontext);
```

`gen_partprune_steps()`（partprune.c:744）把 WHERE 子句与分区键匹配，生成一个小型"裁剪程序"：基本步骤（`PartitionPruneStepOp`：用某个操作符和值对边界做二分）与组合步骤（`PartitionPruneStepCombine`：多个结果集求交/并）。计划期能算的立即求值，被裁掉的分区**根本不会进计划树**——EXPLAIN 里看不到它们。

**阶段二：执行器启动期（initial pruning）**。含 `$1` 这类外部参数的步骤在计划期算不了，`make_partition_pruneinfo()`（partprune.c:225）把它们打包成 `PartitionPruneInfo` 挂在 Append/MergeAppend 节点上。执行器初始化时 `CreatePartitionPruneState()`（`executor/execPartition.c:2145`，经由 execPartition.c:2008 的调用）对"只含外部参数"的步骤求值，直接**不初始化**被裁掉的子计划——EXPLAIN ANALYZE 里显示为 `Subplans Removed: N`。

**阶段三：运行期（exec pruning）**。`ExecInitPartitionExecPruning()`（execPartition.c:2051）处理引用**执行期才变化的参数**（如 nestloop 内侧的 join 参数）的步骤：Append 每次 rescan 时重算存活分区位图。典型场景 `t JOIN part_table p ON p.key = t.x`——外侧每来一行，内侧只扫 x 命中的那个分区。这是 Hive 做不到的粒度。

### 14.4.3 写路径：ExecFindPartition 的元组路由

INSERT 进分区表时，每行要沿分区层级"下钻"找到叶子分区——`ExecFindPartition()`（execPartition.c:268）：

```c
	/* start with the root partitioned table */
	dispatch = pd[0];
	while (dispatch != NULL)
	{
		...
		ecxt->ecxt_scantuple = slot;
		FormPartitionKeyDatum(dispatch, slot, estate, values, isnull);

		if (partdesc->nparts == 0 ||
			(partidx = get_partition_for_tuple(dispatch, values, isnull)) < 0)
		{
			...  /* 报“找不到分区”的错 */
```

逐层循环：从元组算出分区键值（分区键可以是表达式，所以要走表达式求值机制）→ `get_partition_for_tuple` 对 `PartitionBoundInfoData.datums` 二分 → 命中的如果还是分区表就继续下钻，是叶子就返回其 `ResultRelInfo`（带缓存，同一批插入重复命中同一分区时很快）。多级分区（比如先按月 range、再按租户 hash）就是这个 while 循环多转几圈。

### 14.4.4 Partitionwise join / aggregate 与 Hive 对比

两个 `enable_partitionwise_join` / `enable_partitionwise_aggregate` 控制的优化（默认都是 **off**，`costsize.c:161-162`，因为计划期内存/时间开销随分区数放大）：

- **分区智能连接**：两表按相同方式分区且 join key = 分区键时，`try_partitionwise_join()`（`optimizer/path/joinrels.c:1611`）把"大表 join 大表"分解为"逐对分区 join 再 Append"。每对分区的 join 更小、更容易走 hash join 且内存放得下——等价于 Spark 里两个 bucketed 表的 co-located join 免 shuffle；
- **分区智能聚合**：GROUP BY 含全部分区键时，`create_partitionwise_grouping_paths()`（planner.c:8366）把聚合下推到每个分区内部完成（PARTITIONWISE_AGGREGATE_FULL），否则退化为"分区内 partial + 上层 finalize"。

**与 Hive 静态分区裁剪对比**：

| | Hive | PostgreSQL |
|---|---|---|
| 分区元数据 | metastore 里的目录名 | 系统目录 + 规范化边界数组 |
| 静态裁剪 | 编译期按字面量过滤目录 | 阶段一（计划期），能力等价 |
| 动态裁剪 | 需要 DPP（dynamic partition pruning，物化广播侧再过滤），Hive/Spark 都是后加的补丁特性 | 阶段二/三是执行器**原生机制**，逐参数、逐行粒度 |
| 分区 = 物理对象 | 目录，无独立统计 | 每个分区是完整的表：自己的统计、索引、甚至不同的存储参数 |

PG 的三阶段裁剪本质上是把"常量折叠"从编译期延展到了运行期的每个求值时机；而 Hive 的分区是文件系统布局的衍生物。反过来，Hive 轻松支撑十万级分区，PG 在几千分区以上就要留意计划期开销（锁、relcache、裁剪步骤生成都是每分区成本）——这也是 partitionwise 默认关闭的原因。

### 小结

- 边界统一规范化为排序 datums 数组 + 分区号映射，裁剪与路由都是二分查找；
- 裁剪步骤只生成一次，在计划期/执行器启动期/运行期三个时机按"值何时可知"分别求值；
- INSERT 路由是逐层下钻的二分；partitionwise join/agg 把大问题切成分区粒度的小问题，但默认关闭以护住计划期开销；
- 对比 Hive：静态裁剪等价，动态/运行期裁剪 PG 原生且更细，但分区规模上限低得多。

---

## 自测问题

1. **为什么 ANALYZE 默认只采 30000 行就敢给亿行大表做统计？采样分几个阶段，各用什么算法？**
   简答：直方图 bin 误差界只与样本量相关、与表大小几乎无关（300×target 的系数来自 Chaudhuri 等的论文）。两阶段：块级用 Knuth Algorithm S 随机选块，行级用 Vitter 水库采样（Algorithm Z，跳跃式，`reservoir_get_next_S` 一次算出跳过多少行），见 analyze.c:1262。

2. **pg_statistic 里 stadistinct = -0.5 是什么意思？这个值是怎么估出来的？**
   简答：负数表示比例：distinct 数 ≈ 0.5 × 行数，随表增长而增长（估计值超过总行数 10% 时改存比例）。估计用 Haas-Stokes 的 Duj1 估计器 `n*d/(n - f1 + f1*n/N)`，f1 是样本中只出现一次的值的个数（analyze.c:2669 附近）。

3. **InitializeParallelDSM 为什么必须序列化 combo CID 映射和快照？漏掉会发生什么？**
   简答：快照决定元组可见性，worker 快照不同就会返回与 leader 不一致的行集；combo CID 是本事务"先插后改"元组的 cmin/cmax 压缩映射，存在 leader 私有内存，worker 没有它就无法正确判断本事务内修改过的元组的可见性。两者都通过 shm_toc 传入 DSM（parallel.c:397/407/414）。

4. **PG 并行查询为什么做不了高效的高基数 GROUP BY？内核用什么机制部分弥补？**
   简答：worker 间无 re-partition exchange（唯一的数据交换是 worker→leader 的 Gather），无法按 group key 重分布数据。弥补机制是两阶段聚合：worker 做 partial（AGGSPLIT_INITIAL_SERIAL）、leader 做 finalize（planner.c:7606）——但 finalize 在 leader 单线程执行，组数极多时成为瓶颈。

5. **一条 `WHERE ts = $1` 的查询查分区表，裁剪发生在哪个阶段？如果是 `JOIN ... ON p.key = t.x` 呢？**
   简答：`$1` 在执行器启动期才可知——initial pruning（CreatePartitionPruneState，execPartition.c:2145），未命中的子计划不初始化（Subplans Removed）。join 参数每行变化——运行期 exec pruning（ExecInitPartitionExecPruning，execPartition.c:2051），Append 每次 rescan 重算存活分区。
