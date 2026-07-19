# 第 14 篇 · 查询处理的高级模块：统计信息、并行查询、JIT 与分区表（v2）

> 对应《PostgreSQL 源码学习指南》第 16 章。源码基线：PostgreSQL 20devel（master 分支），所有 `文件:行号` 均在本仓库现场核实。

## 本章导读

前面的篇章走完了"一条 SQL 的一生"：解析 → 优化 → 执行 → 落盘。本篇讲的是叠加在这条主干上的四个"高级模块"，它们分别回答四个问题：

1. **统计信息**——优化器凭什么估算行数？采 3 万行样本就敢对 10 亿行的表下结论，数学依据是什么？哪个统计量天生估不准？（`commands/analyze.c` + `statistics/`）
2. **并行查询**——单机多核怎么用起来？为什么 PG 只做了 scatter-gather 而没做 Spark 那样的 shuffle？漏传一个快照或一张 combo CID 映射表会发生什么？（`access/transam/parallel.c` + `executor/nodeGather*.c` + `storage/ipc/shm_mq.c`）
3. **JIT**——解释执行的表达式怎么一步步变成 LLVM IR？为什么 PG 选择"只编译表达式"而不是像 HyPer 那样编译整个查询？为什么 OLTP 常建议关掉它？（`jit/llvm/`）
4. **分区表**——同一套"裁剪程序"为什么要在三个时机分别求值？prepared statement 的泛型计划怎么把计划期裁剪打回原形，运行期裁剪又如何救场？（`partitioning/` + `executor/execPartition.c`）

这四个模块有一个共同点：都是"锦上添花"型基础设施——关掉它们 SQL 照样能跑，但打开后分析型负载可能快一个数量级。它们也共享同一种工程气质：**在不推翻 Volcano 模型和多进程架构的前提下做增量改造**。统计信息不改执行器，只喂给优化器；并行查询不引入线程，把"进程 + 共享内存"的老三样组合出 worker；JIT 不重写执行器，只替换表达式求值函数指针；分区裁剪不发明新执行节点，把裁剪逻辑挂在现成的 Append 上。对大数据工程师来说，这一章是把 Spark/Hive 的直觉映射进单机数据库内核的最佳入口——你会反复看到"PG 为什么不这么做"比"PG 怎么做的"更有信息量。

**v2 相比 v1 的升级**：全函数级走读（`acquire_sample_rows`、`InitializeParallelDSM`、`gather_readnext`、`ExecFindPartition` 等逐段对应源码）；每个模块补齐"所以然"论证（采样误差界的统计学、scatter-gather vs shuffle 的架构权衡、JIT 编译成本摊销模型、泛型计划与运行期裁剪）；新增不变式清单、git 历史考古（12+ commit）、三个 what-if 故障推演；自测题扩到 10 题带详解。

## 前置知识

- 执行器的 Volcano 模型（每个节点实现 `ExecProcNode`，逐 tuple 拉取）与表达式求值的 steps 数组（`ExprState->steps`，解释器 `ExecInterpExpr` 用 computed goto 逐步执行）——第 4 篇；
- 优化器的 Path/RelOptInfo 概念，选择率（selectivity）如何决定行数估计——第 3 篇；
- 进程模型：PG 是多进程架构，没有线程，进程间共享数据靠共享内存；动态共享内存（DSM）可以在运行时按需创建段——第 1、12 篇；
- MVCC 快照：谁能看见哪些元组由快照决定——这解释了为什么并行 worker 必须拿到与 leader 完全相同的快照——第 8 篇；
- CommandId（cmin/cmax）与 combo CID：同一事务内"先插入再更新同一行"时，元组头只有一个字段却要同时记录插入命令号和删除命令号，于是压缩成 combo CID，映射表在 backend 私有内存——第 8 篇。

---

## 14.1 统计信息：优化器的"眼睛"

### 14.1.1 ANALYZE 的整体流程

入口在 `src/backend/commands/analyze.c`：

```
analyze_rel()            analyze.c:110   -- 加锁、权限检查、决定用哪个采样函数（analyze.c:216 选 acquire_sample_rows）
  └─ do_analyze_rel()    analyze.c:306   -- 主流程
       ├─ examine_attribute() → std_typanalyze()  analyze.c:1950  -- 每列选定"怎么算"
       ├─ acquire_sample_rows()           analyze.c:1262  -- 两阶段随机采样
       ├─ compute_scalar_stats() 等       analyze.c:2461  -- 对样本计算统计量
       ├─ update_attstats()               analyze.c:1714  -- 写入 pg_statistic（调用点 612/619）
       └─ BuildRelationExtStatistics()    analyze.c:624 → statistics/extended_stats.c  -- 扩展统计
```

`std_typanalyze()`（analyze.c:1950）按"该类型有哪些操作符"三档分派：既有 `=` 又有 `<` 的走 `compute_scalar_stats`（analyze.c:2461，全套统计量）；只有 `=` 的走 `compute_distinct_stats`（analyze.c:2118，无直方图无相关性）；两者皆无的走 `compute_trivial_stats`（analyze.c:2028，只有 NULL 率和宽度）。这个分派本身就说明了统计量的依赖关系：**MCV 依赖等值比较，直方图和相关性依赖全序**。

样本量在 `std_typanalyze()` 里定死：`stats->minrows = 300 * stats->attstattarget`（analyze.c:1999），默认 target 100，因此默认采 30000 行。300 这个系数的出处是 analyze.c:1980-1997 的大段注释，下一节展开。

### 14.1.2 acquire_sample_rows 全函数走读：块级 Knuth S + 行级 Vitter Z

PG 的采样是**两阶段**的：第一阶段随机选**块**，第二阶段在被选中的块内随机选**行**。为什么要两阶段？因为行级均匀采样的朴素做法要求扫全表（每行掷骰子），I/O 代价是 O(表大小)；先随机选至多 `targrows` 个块，I/O 代价就被限制在 O(样本量)。函数头注释（analyze.c:1240-1253）坦承这个方法**并非完美**：每行入选概率相等，但"每种样本组合"入选概率不相等——大表的样本所覆盖的块数偏少（同块的行有相关性）。这是"I/O 有限"换来的偏差，注释原话是 "We can live with that for now."

逐段走读 `acquire_sample_rows()`（analyze.c:1262）。

**准备阶段**（analyze.c:1283-1310）：

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

	stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE |
										READ_STREAM_USE_BATCHING,
										vac_strategy,
										scan->rs_rd,
										MAIN_FORKNUM,
										block_sampling_read_stream_next,
										&bs,
										0);
```

- `BlockSampler_Init`（`src/backend/utils/misc/sampling.c:39`）：初始化块采样器，返回 `Min(bs->n, bs->N)`——最多选 `targrows` 个块。为什么块数上限等于目标行数？因为每块至少能贡献 1 行样本，选更多块只会浪费 I/O；
- `reservoir_init_selection_state`（sampling.c:133）：初始化 Vitter 算法状态 `W = exp(-log(random)/n)`（sampling.c:143）——这是 Vitter 1985 论文《Random sampling with a reservoir》Algorithm Z 的初始随机变量；
- `read_stream_begin_relation`：PG 17 引入的流式预读框架（commit 041b96802e，2024-04-08，"Use streaming I/O in ANALYZE."），把"下一个读哪个块"交给 `block_sampling_read_stream_next` 回调（analyze.c:1219），回调内部调 `BlockSampler_Next` 吐出下一个被选中的块号，read stream 负责异步预取。采样的块号是随机跳跃的，正是预读收益最大的场景。

**块级采样：Knuth Algorithm S**（`BlockSampler_Next`，sampling.c:64）。Knuth TAOCP 3.4.2 的算法 S：顺序扫描块号 0..N-1，当前块以概率 k/K 入选（k = 还需要的块数，K = 剩余块数）。归纳可证每块入选概率恰为 n/N，且必然恰好选出 n 块——当 K 降到等于 k 时概率变为 1，"不会选不够"。PG 的实现有个精致的优化（sampling.c:80-111 的注释与循环）：朴素实现每个块号都要掷一次骰子；PG 把"连续跳过若干块"合并成一次随机数——生成一个 V，然后累乘跳过概率 `p *= 1 - k/K` 直到 `V >= p`，把随机数调用从 O(N) 降到 O(n)：

```c
	V = sampler_random_fract(&bs->randstate);
	p = 1.0 - (double) k / (double) K;
	while (V < p)
	{
		/* skip */
		bs->t++;
		K--;					/* keep K == N - t */
		p *= 1.0 - (double) k / (double) K;
	}
```

**行级采样：Vitter 水库**。核心循环（analyze.c:1312-1364）：

```c
	while (table_scan_analyze_next_block(scan, stream))
	{
		vacuum_delay_point(true);

		while (table_scan_analyze_next_tuple(scan, &liverows, &deadrows, slot))
		{
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
					int			k = (int) (targrows * sampler_random_fract(&rstate.randstate));

					Assert(k >= 0 && k < targrows);
					heap_freetuple(rows[k]);
					rows[k] = ExecCopySlotHeapTuple(slot);
				}

				rowstoskip -= 1;
			}

			samplerows += 1;
		}

		pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_DONE, ++blksdone);
	}
```

逐行解说：

- 前 `targrows` 行直接填满"水库"（`rows[]` 数组）；
- 水库满了以后，**不是对每一行掷骰子**（那是朴素的 Algorithm R：第 t 行以 n/t 概率入选），而是调 `reservoir_get_next_S()`（sampling.c:147）**一次性算出接下来要跳过多少行**。该函数内部又分两档（sampling.c:152 的判断 `t <= 22.0 * n`）：t 较小时用 Algorithm X（逐步累乘求满足不等式的最小 S，sampling.c:154-169）；t 大时切到 Algorithm Z 的拒绝采样（sampling.c:171-224），期望 O(1) 随机数就能算出 S。随着 t 增大，跳跃长度期望 ≈ t/n 线性增长——扫到 1 亿行时平均每 3000+ 行才醒一次，这是它对亿行大表可行的原因；
- 命中时随机选水库下标 k 替换。归纳不变式（analyze.c:1319-1329 的注释）：**任意时刻水库都是已扫过行的均匀随机样本**，所以扫到表尾即得最终样本；
- 注意两阶段是**交织执行**的（analyze.c:1244-1247 注释）：块的选择由 read stream 回调驱动，行的水库逻辑对"被选中块内的行流"运作，Vitter 算法根本不需要预知总行数——这正是它相对 Knuth S 的价值（块数已知可用 S，行数未知必须用 Z）。

**收尾**（analyze.c:1379-1394）：

```c
	if (numrows == targrows)
		qsort_interruptible(rows, numrows, sizeof(HeapTuple),
							compare_rows, NULL);
	...
	if (bs.m > 0)
	{
		*totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
		*totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
	}
```

- 水库若发生过替换，样本的物理顺序被打乱，必须按 TID 排回物理序（`compare_rows`，analyze.c:1420：先比块号再比行内偏移）——**后面算相关性依赖"样本第 i 行"就是"物理序第 i 个样本"这一事实**；若 `numrows < targrows`（小表没装满水库）则天然有序，跳过排序；
- `totalrows` 按"已扫块的平均活行密度 × 总块数"外推，这就是 `pg_class.reltuples` 的来源。函数头注释（analyze.c:1255-1259）特意说明：因为块集合是统计无偏的，行密度估计也无偏——修复了更早年代"只看表头几个块"高估密度的毛病。

### 14.1.3 为什么 30000 行就敢对 10 亿行下结论：直方图误差的统计学

`std_typanalyze()` 的注释（analyze.c:1980-1997）引用了 Chaudhuri, Motwani & Narasayya（SIGMOD 1998）《Random sampling for histogram construction: how much is enough?》的 Corollary 1 to Theorem 5：表大小 n、直方图桶数 k、桶大小最大相对误差 f、失败概率 γ，所需样本量为

```
r = 4 * k * ln(2n/γ) / f²
```

取 f=0.5、γ=0.01、n=10⁶，得 r ≈ 305.82k——这就是 300 系数的来历。**关键在于 n 只出现在对数里**：注释算过，n 涨到 10¹²（万亿行），300k 样本的桶误差仍以 99% 概率 ≤ 0.66。为什么会这样？直方图桶边界本质是**分位数估计**：等高直方图第 i 个边界是 i/k 分位数。样本分位数对总体分位数的收敛由 Dvoretzky–Kiefer–Wolfowitz 不等式刻画：经验分布函数与真实分布函数的最大偏差以 exp(-2rε²) 的速率衰减——**收敛速度只与样本量 r 有关，与总体大小无关**（抽样调查"3000 人样本预测 3 亿人选举"是同一个原理）。表大小只通过 ln(n) 影响联合失败概率（k 个桶同时达标），所以增长极慢。

这也解释了 `default_statistics_target` 的调参逻辑：target 提到 1000（样本 30 万行），桶数 ×10 使每桶更细，但每桶的**相对**误差界不变——你买到的是分辨率，不是每桶精度；代价是 ANALYZE 的 I/O、排序 CPU 和 pg_statistic 行宽全部放大。

**Spark/Hive 对照**：Hive 的 `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` 默认全表扫 + HLL 概略算法；Spark CBO 同样倾向全量。PG 选固定小样本，因为它是**在线 OLTP 内核**，ANALYZE 与生产负载抢 I/O（还要受 `vacuum_delay_point`，analyze.c:1315，节流），而批处理系统的统计任务本身就是离线作业。同一个问题，两种成本函数，两种答案。

### 14.1.4 compute_scalar_stats 主线：一次排序产出四种统计量

`compute_scalar_stats()`（analyze.c:2461）的输入是 30000 行样本中该列的值，输出 NULL 率、平均宽度、ndistinct、MCV、直方图、相关性。主线是"**抽值 → 排序 → 单遍扫描顺手算完**"。

**第一步：抽值**（analyze.c:2505-2557）。逐样本行取值：NULL 计数后跳过；varlena 类型累计宽度，且 detoast 后超过 `WIDTH_THRESHOLD` 的超宽值**不参与排序**（只计入 `toowide_cnt`，analyze.c:2539-2543——排序 30000 个 1MB 大值的内存代价不可接受，后面把它们按"各自 distinct"处理）。入选值记录 `(value, tupno)`，tupno 是它在样本中的序号（= 物理序，因为样本已按 TID 排序）。

**第二步：排序**（analyze.c:2572）。用 `PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup)`（analyze.c:2502）拿该类型 `<` 操作符的比较器。这里有个隐藏的优化（analyze.c:2579-2592 注释）：比较器 `compare_scalars` 在排序过程中**顺手维护 `tupnoLink`**——每个元素记录"曾与我比较相等的最大 tupno"。排序结束后，`tupnoLink[tupno] == tupno` 当且仅当该元素是其重复组的最后一个。于是下一步扫描**不需要再做一次相邻比较**就能划分重复组——排序时已经比过的功不浪费第二遍。

**第三步：单遍扫描**（analyze.c:2594-2637），同时干三件事：

```c
		corr_xysum = 0;
		ndistinct = 0;
		nmultiple = 0;
		dups_cnt = 0;
		for (i = 0; i < values_cnt; i++)
		{
			int			tupno = values[i].tupno;

			corr_xysum += ((double) i) * ((double) tupno);
			dups_cnt++;
			if (tupnoLink[tupno] == tupno)
			{
				/* Reached end of duplicates of this value */
				ndistinct++;
				if (dups_cnt > 1)
				{
					nmultiple++;
					if (track_cnt < num_mcv ||
						dups_cnt > track[track_cnt - 1].count)
					{ ... 插入 track[] 有序小顶列表 ... }
				}
				dups_cnt = 0;
			}
		}
```

- `corr_xysum` 累加 (值序 i × 物理序 tupno)——相关性的交叉项；
- 每到一个重复组末尾，ndistinct++；出现 ≥2 次的组进入 `track[]`（容量 `num_mcv` = target 的按出现次数降序列表，插入时冒泡下沉，analyze.c:2611-2633）——**MCV 候选只收"出现多于一次"的值**，出现一次的值频率估计毫无意义。

**第四步：ndistinct 估计（Haas-Stokes Duj1）**（analyze.c:2647-2717）。三分支：

1. `nmultiple == 0`：没有任何重复 → 视为唯一列，`stadistinct = -1.0 * (1.0 - stanullfrac)`；
2. `toowide_cnt == 0 && nmultiple == ndistinct`：**每个值都出现了 ≥2 次** → 认为样本已覆盖全部取值（布尔/枚举列），`stadistinct = ndistinct` 定值；
3. 否则用 Duj1 估计器：

```c
			int			f1 = ndistinct - nmultiple + toowide_cnt;
			int			d = f1 + nmultiple;
			double		n = samplerows - null_cnt;
			double		N = totalrows * (1.0 - stats->stanullfrac);
			double		stadistinct;

			/* N == 0 shouldn't happen, but just in case ... */
			if (N > 0)
				stadistinct = (n * d) / ((n - f1) + f1 * n / N);
```

即 `n·d / (n − f1 + f1·n/N)`，f1 是样本中只出现一次的值数。直觉：f1 越大，说明"表里还有大量没见过的值"，估计就要往上抬——极端地，f1=0（每个值都重复出现）时公式退化为 d（样本大概率已见过所有值）；f1=d（全部唯一）时分母趋近 n²/N 量级，估计被抬向 N。注释（analyze.c:2676-2678）说明选 Duj1 是因为 Haas-Stokes 论文推荐的更精确估计器"数值上非常不稳定"当 n ≪ N。

若估出的 distinct 数超过总行数 10%，改存**负数比例**（analyze.c:2716-2717：`stadistinct = -(stadistinct/totalrows)`）——语义是"distinct 数随表线性增长"（唯一列存 -1，pg_statistic.h:55-63 注释）。为什么要有这个约定？表在两次 ANALYZE 之间会增长，`reltuples` 由 VACUUM 等随时更新；比例形式让优化器用"当前行数 × 比例"外推，避免统计陈旧时 ndistinct 严重滞后。

**第五步：MCV 入选判据**（analyze.c:2735-2761）。两种情形：

- **完备情形**：`track_cnt == ndistinct && toowide_cnt == 0 && stadistinct > 0 && track_cnt <= num_mcv`——样本里的所有 distinct 值都进了 track 且放得下 → 全部入 MCV。此时优化器拥有该列的**完备频率表**，等值查询的估计几乎精确；
- **不完备情形**：调 `analyze_mcv_list()`（analyze.c:3039）做显著性筛选——只保留"频率显著高于非 MCV 值平均频率"的头部值，宁缺毋滥。为什么？MCV 里的频率会被优化器**直接当真**；把采样噪声（比如真实频率 1/N 却在样本里撞出 3 次的值）写进 MCV，会让等值估计错得理直气壮。

**第六步：等高直方图**（analyze.c:2803-2910）。把 MCV 已覆盖的值从排序数组中**物理剔除**（analyze.c:2827-2860 的 memmove 压缩循环），剩余值取等间隔分位点做桶边界：`num_hist = ndistinct - num_mcv`，上限 `num_bins + 1` 个边界（analyze.c:2803-2805）。取分位点的循环（analyze.c:2878-2895）用整数+分数双计数器避免 `i*(nvals-1)/(num_hist-1)` 的整型溢出。选**等高**（每桶行数相同）而非等宽：倾斜数据下等宽直方图的热点桶误差爆炸，等高保证每桶的估计误差一致——范围查询 `WHERE x < c` 的估计 = 完整桶数/总桶数 + 边界桶内线性插值。**MCV 与直方图是互斥分解**：MCV 精确处理头部，直方图只负责"长尾的形状"，这比单独一个直方图对 Zipf 分布友好得多。

**第七步：相关性**（analyze.c:2913-2949）。x = 值序、y = 物理序，两者都恰是 0..values_cnt-1 的排列，Σx、Σx² 有闭式解（analyze.c:2925-2937），皮尔逊相关系数化简到只剩交叉项：

```c
			corrs[0] = (values_cnt * corr_xysum - corr_xsum * corr_xsum) /
				(values_cnt * corr_x2sum - corr_xsum * corr_xsum);

			stats->stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
```

correlation ∈ [-1,1]，衡量逻辑序与物理序的一致程度。消费者是 `cost_index()`：correlation² 在"最好情形（顺序读）"与"最坏情形（每行一次随机读）"之间插值——这就是为什么天然按时间追加的时序表走索引范围扫描估价便宜，而被反复更新打乱物理序的表同一个索引会被估得很贵。

### 14.1.5 反面教材：为什么 ndistinct 是采样最难估准的量

直方图（分位数）有"误差与表大小无关"的定理护体，ndistinct 没有——**它有不可能定理**。Charikar & Chaudhuri 等证明过：任何基于 r 行样本的 ndistinct 估计器，都存在使其相对误差达到 √(N/r) 量级的分布。直觉：分位数是"分布的整体形状"，样本天然反映；distinct 数取决于**尾部稀有值的个数**，而稀有值恰恰最难被采到。两个方向的失败场景：

- **低估场景**：列有 1000 万个 distinct 值，其中 100 个热值占 90% 行，剩下 999 万+ 个值各出现一两次。30000 行样本里 f1 相对不大（大部分样本落在热值上），Duj1 输出可能只有几十万——低估 20 倍。后果链：`GROUP BY 该列` 的组数被低估 → planner 选 HashAgg 且认为哈希表放得下 → 执行时哈希表膨胀溢盘（PG 13 前甚至 OOM）；join 基数同样连锁放大；
- **高估场景**：列实际只有 5 万个 distinct 值但分布极均匀（每值约 N/50000 行），30000 行样本几乎全是"只出现一次"的值（f1 ≈ d ≈ n），公式把估计抬向 N ——把"低频均匀"误判成"接近唯一"。后果：等值选择率被估成 1/N 量级，nested loop 被误选。

这就是 `ALTER TABLE ... ALTER COLUMN ... SET (n_distinct = ...)` 存在的原因——ndistinct 是 pg_statistic 中唯一提供**人工覆盖开关**的统计量，官方承认采样在这件事上无能为力。Spark/Hive 的解法是 HyperLogLog：全量扫描 + O(1) 内存的概略计数，误差 ~2% 且与分布无关——但代价是必须扫全表。这是"采样 vs 概略算法"的本质取舍：采样对分位数便宜而准，对基数计数天生残废；HLL 对基数准，但没有免费的分位数。

### 14.1.6 pg_statistic 的存储格式：5 个通用槽位

`src/include/catalog/pg_statistic.h` 定义的是一套**类型无关的槽位系统**：

- 固定列：`stanullfrac`、`stawidth`、`stadistinct`（pg_statistic.h:71，正负号语义见上）；
- **5 个槽位**（`STATISTIC_NUM_SLOTS = 5`，pg_statistic.h:131），每槽五元组：`stakindN`（种类，pg_statistic.h:90）、`staopN`（关联操作符，pg_statistic.h:96）、`stacollN`、`stanumbersN`（float4 数组，pg_statistic.h:109）、`stavaluesN`（`anyarray` 伪类型，pg_statistic.h:121——系统目录里少数用 anyarray 之处，因为要存任意列类型的值数组）。

内建种类：`STATISTIC_KIND_MCV = 1`（pg_statistic.h:194，stavalues 存值、stanumbers 存频率）、`STATISTIC_KIND_HISTOGRAM = 2`（pg_statistic.h:214，stavalues 存桶边界）、`STATISTIC_KIND_CORRELATION = 3`（pg_statistic.h:226，stanumbers 存单个系数）。槽位"先到先得"：数组类型写元素级统计（kind 4/5），范围类型写 6/7，自定义 typanalyze 可注册自己的 kind。消费端在 `utils/adt/selfuncs.c`：`eqsel` 查 MCV、`scalarltsel` 查直方图插值。为什么 staop 要存操作符？因为"相等"和"有序"是**操作符定义的语义**而非类型内禀属性——一个类型可以有多套操作符类，统计量必须声明自己是按哪套语义算的，消费者按操作符匹配而不是按类型匹配。

用 SQL 看它：`SELECT * FROM pg_stats WHERE tablename='t';`（pg_stats 是做了权限过滤的可读视图）。

### 14.1.7 扩展统计与 dependencies_clauselist_selectivity 走读

单列统计的致命假设：多谓词选择率**相乘**（独立假设）。`WHERE city='北京' AND province='北京市'` 被估成 P(city)×P(province)，低估数个数量级。`CREATE STATISTICS` 的扩展统计是解药，实现在 `src/backend/statistics/`：

| 种类 | 构建入口 | 存什么 | 修正什么 | 引入版本 |
|---|---|---|---|---|
| `ndistinct` | `statext_ndistinct_build`（mvdistinct.c:85） | 各列组合的 distinct 数 | GROUP BY 多列组数 | PG 10（commit 7b504eb282） |
| `dependencies` | dependencies.c | 函数依赖度 degree ∈ [0,1] | AND 等值谓词 | PG 10（commit 2686ee1b7c） |
| `mcv` | mcv.c | 多列联合 MCV | 最精确，支持不等值 | PG 12（commit 7300a69950） |

接入点在选择率总入口 `clauselist_selectivity()`（clausesel.c:100 → `clauselist_selectivity_ext`，clausesel.c:117）：`find_single_rel_for_clauses`（clausesel.c:144）确认所有子句引用同一基表且 `rel->statlist` 非空，才调 `statext_clauselist_selectivity()`（extended_stats.c:2024）。其内部次序讲究：**先 MCV 后依赖**——`statext_mcv_clauselist_selectivity()`（extended_stats.c:1737 → mcv.c:2043）先用联合 MCV 拿精确联合频率，剩下的交给 `dependencies_clauselist_selectivity()`。被估过的子句下标记入 `estimatedclauses` 位图，clausesel.c:158 之后的单列循环跳过它们，避免重复计入。

**`dependencies_clauselist_selectivity()`（dependencies.c:1350）主线走读**：

1. **筛子句**（dependencies.c:1406-1462）：逐子句判断是否"依赖统计兼容"——`dependency_is_compatible_clause` 只接受 `Var = Const` 形态（函数依赖的语义只对等值成立：a→b 说的是"a 相同则 b 相同"，对范围谓词无意义）。兼容的记下 attnum；PG 14 起表达式统计也支持，表达式被赋予负数伪 attnum 再统一偏移进位图（dependencies.c:1401-1404 注释）；
2. **门槛检查**（dependencies.c:1522）：`bms_membership(clauses_attnums) != BMS_MULTIPLE` 则直接返回 1.0——不足两个不同属性谈不上依赖；
3. **加载依赖**（dependencies.c:1540 起）：把该表所有 dependencies 统计对象中"匹配 ≥2 个属性"的反序列化进来；
4. **循环应用**：每轮用 `find_strongest_dependency()`（dependencies.c:911）选 degree 最高、覆盖最宽的依赖，交给 `clauselist_apply_dependencies()`（dependencies.c:994）。修正公式（dependencies.c:1106-1120）把被依赖属性的选择率 s2 替换为条件概率：

```c
			/*
			 * P(b|a) = f * Min(P(a), P(b)) / P(a) + (1-f) * P(b)
			 */
			f = dependency->degree;

			if (s1 <= s2)
				attr_sel[attidx] = f + (1 - f) * s2;
			else
				attr_sel[attidx] = f * s2 / s1 + (1 - f) * s2;
```

对照独立假设 P(a,b) = P(a)·P(b)，依赖修正后 P(a,b) = P(a)·P(b|a) = P(a)·[f·min(P(a),P(b))/P(a) + (1−f)·P(b)]。当 degree=1（完全函数依赖，如"城市→省份"）且 P(a) ≤ P(b) 时化简为 P(a,b) = P(a)——**符合直觉：b 完全由 a 决定时，加上 b 的条件不再缩小结果集**。当 degree=0 退回独立相乘。多列时递归应用：P(a,b,c) 先按最强依赖 (a,b→c) 拆成 P(a,b)·P(c|a,b)，再拆 P(a,b)（dependencies.c:1341-1347 注释）。

函数依赖的**局限**也要点破（README.dependencies 有详细讨论）：degree 是全表平均，它假设"查询的常量组合是相容的"。`WHERE city='北京' AND province='广东省'`（矛盾组合）真实结果为 0，依赖修正却仍给出 ≈P(city)——**高估**。联合 MCV 没有这个问题（它按值查），这也是"先 MCV 后依赖"次序的深层原因：MCV 能落地的绝不留给依赖。

三个 README（`statistics/README`、`README.dependencies`、`README.mcv`）是极好的算法文档，建议通读。

### 小结

- 两阶段采样 = 块级 Knuth S（块数已知，顺序概率法 + 跳跃优化）+ 行级 Vitter Z（行数未知，水库 + 期望 O(1) 跳跃）；样本按 TID 回排以保物理序；totalrows 按块密度外推；
- 30000 行的底气来自分位数估计的收敛性（误差只依赖样本量，表大小只在对数里）；ndistinct 则有不可能定理，Duj1 在重尾/均匀两个方向都可能错一个数量级——这是唯一提供人工覆盖的统计量；
- `compute_scalar_stats` 一次排序四种产出：tupnoLink 复用排序比较、MCV 显著性筛选、MCV/直方图互斥分解、相关性闭式化简；
- pg_statistic 是 5 槽位开放格式，操作符标注语义；扩展统计按"MCV 优先、依赖兜底"拦截多列子句，依赖公式 P(b|a) = f·min(P(a),P(b))/P(a) + (1−f)·P(b)。

---

## 14.2 并行查询：单机版 scatter-gather

### 14.2.1 InitializeParallelDSM 全函数走读：把"我是谁"序列化给 worker

PG 的并行 worker 是 postmaster **新 fork 的进程**，不继承 leader 的任何私有内存。所以 leader 必须把"让 worker 表现得像我"所需的全部状态打包进 DSM。`src/backend/access/transam/parallel.c:67-81` 的 key 清单就是这份"人格拷贝"的目录：

```c
#define PARALLEL_KEY_FIXED					UINT64CONST(0xFFFFFFFFFFFF0001)
#define PARALLEL_KEY_ERROR_QUEUE			UINT64CONST(0xFFFFFFFFFFFF0002)
#define PARALLEL_KEY_LIBRARY				UINT64CONST(0xFFFFFFFFFFFF0003)
#define PARALLEL_KEY_GUC					UINT64CONST(0xFFFFFFFFFFFF0004)
#define PARALLEL_KEY_COMBO_CID				UINT64CONST(0xFFFFFFFFFFFF0005)
#define PARALLEL_KEY_TRANSACTION_SNAPSHOT	UINT64CONST(0xFFFFFFFFFFFF0006)
#define PARALLEL_KEY_ACTIVE_SNAPSHOT		UINT64CONST(0xFFFFFFFFFFFF0007)
#define PARALLEL_KEY_TRANSACTION_STATE		UINT64CONST(0xFFFFFFFFFFFF0008)
#define PARALLEL_KEY_ENTRYPOINT				UINT64CONST(0xFFFFFFFFFFFF0009)
#define PARALLEL_KEY_SESSION_DSM			UINT64CONST(0xFFFFFFFFFFFF000A)
#define PARALLEL_KEY_PENDING_SYNCS			UINT64CONST(0xFFFFFFFFFFFF000B)
#define PARALLEL_KEY_REINDEX_STATE			UINT64CONST(0xFFFFFFFFFFFF000C)
#define PARALLEL_KEY_RELMAPPER_STATE		UINT64CONST(0xFFFFFFFFFFFF000D)
#define PARALLEL_KEY_UNCOMMITTEDENUMS		UINT64CONST(0xFFFFFFFFFFFF000E)
#define PARALLEL_KEY_CLIENTCONNINFO			UINT64CONST(0xFFFFFFFFFFFF000F)
```

逐段走读 `InitializeParallelDSM()`（parallel.c:213）。

**开场**（parallel.c:231-232）：`GetTransactionSnapshot()` / `GetActiveSnapshot()` 先取两个快照——注意是在任何 DSM 分配之前取，保证序列化的快照就是 leader 当下用的快照。

**两个"临阵放弃"检查**：

- parallel.c:247-248：若当前处于不可中断状态（`!INTERRUPTS_CAN_BE_PROCESSED()`），直接把 `nworkers` 清零——leader 无法响应 worker 发来的中断，启动 worker 会死锁；
- parallel.c:254-267：拿不到 per-session DSM 段（存共享 record typmod 注册表）也清零 worker——worker 与 leader 对匿名 record 类型的 typmod 编号不一致的话，元组无法互相解读。

**估算趟**（parallel.c:276-314）：对每类状态先调 `EstimateXXXSpace()` 累加进 estimator，再一次性 `dsm_create`。为什么要两趟（先 Estimate 后 Serialize）而不是边序列化边扩容？因为 **DSM 段创建后不能像 palloc 那样 repalloc**——共享内存段的地址已经发给别人，只能一次算准。这是共享内存编程与堆编程的根本差异。

**创建 DSM 的降级路径**（parallel.c:328-341）：

```c
	segsize = shm_toc_estimate(&pcxt->estimator);
	if (pcxt->nworkers > 0)
		pcxt->seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);
	if (pcxt->seg != NULL)
		pcxt->toc = shm_toc_create(PARALLEL_MAGIC, dsm_segment_address(pcxt->seg), segsize);
	else
	{
		pcxt->nworkers = 0;
		pcxt->private_memory = MemoryContextAlloc(TopMemoryContext, segsize);
		pcxt->toc = shm_toc_create(PARALLEL_MAGIC, pcxt->private_memory, segsize);
	}
```

DSM 段数达到上限时**不报错**，退到私有内存 + 0 worker——"放弃并行"永远优于"查询失败"。这是并行代码贯穿始终的设计纪律（下文 LaunchParallelWorkers 还会看到一次）。TOC（table of contents）是段内的 key→指针目录，`shm_toc_insert` 登记、`shm_toc_lookup` 查询。

**固定状态**（parallel.c:343-363）：`FixedParallelState` 存 database OID、authenticated/session/current user、临时 namespace、leader 的 PGPROC/PID（worker 靠它加入 leader 的锁组）、事务/语句时间戳、序列化事务句柄等——都是定长值，不需要序列化器。

**逐项序列化**（parallel.c:384-456），每一项都值得问一句"漏了会怎样"：

- **LIBRARY**（parallel.c:384-387）：leader 已 load 的动态库列表。worker 必须 load 同样的 .so，否则：扩展注册的 hook 不生效、自定义类型的 in/out 函数找不到、计划树里指向扩展函数的 OID 无法解析——查询直接报"could not find function"；
- **GUC**（parallel.c:389-392）：全部非默认 GUC。`work_mem` 不一致 → worker 的 HashJoin 溢盘行为偏离计划假设；`enable_*` 不一致 → 无碍（worker 不再计划）但 `extra_float_digits` 这类影响输出的必须一致。特别地，与快照语义相关的 GUC（如 `default_transaction_isolation` 影响的行为）不一致会直接破坏一致性；
- **COMBO_CID**（parallel.c:394-397）：combo CID → (cmin, cmax) 的映射表。本事务先插入再更新同一行时元组头存的是压缩 combo 值，判断"这行对本事务的当前命令可见吗"必须展开映射。映射在 leader 私有内存——worker 没有它，就会把"本命令之前已删除"的行判成可见（或反之），**同一查询里 leader 和 worker 对同一行给出不同的可见性判断**，返回重复行或丢行；
- **TRANSACTION_SNAPSHOT**（parallel.c:403-409）：仅 `IsolationUsesXactSnapshot()`（REPEATABLE READ 及以上）时传——RC 隔离级每条语句取新快照，事务级快照无意义；RR 以上则 worker 必须继承事务快照，且注册到 leader 的快照引用上防止过早释放；
- **ACTIVE_SNAPSHOT**（parallel.c:411-414）：当前语句的执行快照。这是最不能漏的一项：快照决定 xmin/xmax 可见性边界，worker 快照比 leader 新哪怕一个 XID，就可能看到 leader 看不到的已提交行——Gather 汇出的结果集违反隔离级别，且**不确定性复现**（取决于并发提交时序）；
- **SESSION_DSM**（parallel.c:416-421）：共享 record typmod 表的句柄（见上）；
- **TRANSACTION_STATE**（parallel.c:423-426）：XID 栈、当前用户、子事务层级。worker 以"参与者"身份加入 leader 的事务：它能看到 leader 的 XID 视角，但**禁止写**——并行模式下分配 XID、写 WAL 的操作会报 "cannot ... during a parallel operation"。为什么禁止写？三个进程同时给一个事务分配子 XID/扩展 combo CID 映射，同步成本会吞掉并行收益，9.6 的设计决定是先做只读并行（commit 924bcf4f16 的提交信息明说了这个取舍）；
- **PENDING_SYNCS / REINDEX_STATE / RELMAPPER_STATE / UNCOMMITTEDENUMS / CLIENTCONNINFO**（parallel.c:428-456）：边角状态——本事务新建且尚未 fsync 的表、正在 REINDEX 的索引集合（worker 必须同样跳过它们）、relfilenode 映射的未提交变更、本事务新增的枚举值（worker 需要知道哪些枚举 OID 尚不可比较）、客户端连接信息（供日志/审计钩子使用）。

**错误队列**（parallel.c:468-482）：每 worker 一条 `PARALLEL_ERROR_QUEUE_SIZE`（16KB）的 shm_mq，receiver 设为 leader。worker 里 `ereport` 抛出的 ErrorResponse/NoticeResponse 被序列化后经此队列送回 leader 重新抛出——这就是客户端能看到 worker 内部错误文本的机制。错误路径独立于数据路径（元组队列在 execParallel.c 另建），保证数据队列满时错误仍能传递。

**入口点**（parallel.c:484-496）：以 `(library_name, function_name)` **字符串**而非函数指针传递——EXEC_BACKEND 构建（Windows）下各进程加载地址不同，函数指针跨进程无意义。注释（parallel.c:485-489）点明了这一点。

之后 `LaunchParallelWorkers()`（parallel.c:583）注册 bgworker（见 14.7 what-if 2 的失败路径），worker 入口 `ParallelWorkerMain()`（parallel.c:1301）按同样的 key 逐项 `shm_toc_lookup`（parallel.c:1364/1378/1417 起）恢复状态——**严格按依赖顺序**：先库（否则 GUC 的扩展变量没人认领）、再 GUC、再快照、再事务状态。

执行器层在这之上再叠一层自己的 TOC：`ExecInitParallelPlan()`（execParallel.c:653）序列化 `PARALLEL_KEY_PLANNEDSTMT`（execParallel.c:820-823——**worker 拿到的是整棵计划树的序列化副本**，各自反序列化后从中找到 Gather 之下的子树执行）、参数表、每 worker 一条 64KB 元组队列（`PARALLEL_TUPLE_QUEUE_SIZE`，execParallel.c:71；创建于 execParallel.c:622-642）和 instrumentation 汇总区（EXPLAIN ANALYZE 的 worker 行就从这来）。

### 14.2.2 执行器侧：ExecGather 与 gather_readnext 全函数走读

`Gather` 节点（`src/backend/executor/nodeGather.c`）是并行子树与串行上层的边界。`ExecGather()`（nodeGather.c:138）的首次调用惰性启动（nodeGather.c:146-151 注释：DSM 分配很贵，等真被执行才做——被 LIMIT 提前掐断的计划可能根本走不到这）：`ExecInitParallelPlan` → `LaunchParallelWorkers` → `ExecParallelCreateReaders` 建 reader 数组。nodeGather.c:213-215 决定 leader 是否亲自跑子计划：`nreaders == 0`（一个 worker 都没起来）或 `parallel_leader_participation` 开启且非 single_copy。

`gather_getnext()`（nodeGather.c:264）是"读队列优先，兜底本地执行"的循环：队列拿到元组就返回；拿不到且 `need_to_scan_locally`，leader 直接 `ExecProcNode(outerPlan)` 亲自算一条。

核心是 `gather_readnext()`（nodeGather.c:312），完整逻辑：

```c
static MinimalTuple
gather_readnext(GatherState *gatherstate)
{
	int			nvisited = 0;

	for (;;)
	{
		TupleQueueReader *reader;
		MinimalTuple tup;
		bool		readerdone;

		/* Check for async events, particularly messages from workers. */
		CHECK_FOR_INTERRUPTS();

		Assert(gatherstate->nextreader < gatherstate->nreaders);
		reader = gatherstate->reader[gatherstate->nextreader];
		tup = TupleQueueReaderNext(reader, true, &readerdone);

		if (readerdone)
		{
			Assert(!tup);
			--gatherstate->nreaders;
			if (gatherstate->nreaders == 0)
			{
				ExecShutdownGatherWorkers(gatherstate);
				return NULL;
			}
			memmove(&gatherstate->reader[gatherstate->nextreader],
					&gatherstate->reader[gatherstate->nextreader + 1],
					sizeof(TupleQueueReader *)
					* (gatherstate->nreaders - gatherstate->nextreader));
			if (gatherstate->nextreader >= gatherstate->nreaders)
				gatherstate->nextreader = 0;
			continue;
		}

		/* If we got a tuple, return it. */
		if (tup)
			return tup;

		gatherstate->nextreader++;
		if (gatherstate->nextreader >= gatherstate->nreaders)
			gatherstate->nextreader = 0;

		/* Have we visited every (surviving) TupleQueueReader? */
		nvisited++;
		if (nvisited >= gatherstate->nreaders)
		{
			if (gatherstate->need_to_scan_locally)
				return NULL;

			(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
							 WAIT_EVENT_EXECUTE_GATHER);
			ResetLatch(MyLatch);
			nvisited = 0;
		}
	}
}
```

这是一个完整的**多队列非阻塞轮询协议**，五个要点：

1. `TupleQueueReaderNext(reader, nowait=true, &readerdone)`（tqueue.c:176）非阻塞读：三态返回——有元组 / 暂时没有 / 该 worker 已发完（detach 队列即 EOF 信号，无需带外通知）；
2. 读完的 worker 用 memmove 从活跃数组剔除，`nreaders` 归零时关停全部 worker 并返回 NULL（上层收到最终 EOF）；
3. **黏性读**（nodeGather.c:363-368 注释）：拿到元组不换队列，下一次仍读同一个 reader，直到读空才轮转。历史上是逐 tuple 轮转，实测黏着读快得多——同一队列的连续读命中同一段共享内存 cache line，且摊薄 `shm_mq` 的 read 计数更新。代价是 Gather 输出顺序完全取决于"谁的队列先有货"——**乱序是设计结果**；
4. 轮询一整圈（`nvisited >= nreaders`）都空手：若 leader 还能本地执行则返回 NULL 让上层去跑本地子计划（**leader 不空等，把自己也当 worker 用**）；否则 `WaitLatch` 睡眠，等任一 worker 写入队列后 `SetLatch` 唤醒——避免 busy-poll 烧 CPU；
5. 顶部的 `CHECK_FOR_INTERRUPTS()` 兼作错误泵：worker 的错误经错误队列以中断形式送达 leader，在这里被重新抛出。

`GatherMerge`（nodeGatherMerge.c，PG 10，commit 355d3993c5）解决"子路径有序、汇聚后仍要有序"：每 worker 产出有序流，leader 用二叉堆做 N+1 路归并（`binaryheap_allocate(nreaders + 1, ...)`，nodeGatherMerge.c:430，+1 是 leader 自己的本地流）。`gather_merge_getnext()`（nodeGatherMerge.c:547）是教科书归并：弹堆顶 → 从其来源 reader 补一条 → 重入堆。它使 `Sort` 可以下推到 Gather 之下并行执行——与 Spark 的 sort-merge 落盘归并、Kafka 多分区按时间戳归并同构。

### 14.2.3 shm_mq：SPSC 无锁环形队列的正确性论证

worker → leader 的元组传输走 `src/backend/storage/ipc/shm_mq.c`（commit ec9037df26，2014-01-14，比并行查询本体早一年半——基础设施先行）。文件头自述 "single-reader, single-writer shared memory message queue"。核心结构（shm_mq.c:73-84）：

```c
struct shm_mq
{
	slock_t		mq_mutex;
	PGPROC	   *mq_receiver;
	PGPROC	   *mq_sender;
	pg_atomic_uint64 mq_bytes_read;
	pg_atomic_uint64 mq_bytes_written;
	Size		mq_ring_size;
	bool		mq_detached;
	uint8		mq_ring_offset;
	char		mq_ring[FLEXIBLE_ARRAY_MEMBER];
};
```

**无锁正确性论证**（shm_mq.c:31-72 的注释是完整证明，这里复述其骨架）：

1. **单方写者原则**：`mq_bytes_read` 只有 receiver 改，`mq_bytes_written` 只有 sender 改（shm_mq.c:36-37）。计数器**只增不减**（64 位，按 uint64 回绕语义取模环大小），于是每个变量都是"单写者多读者"——8 字节原子读写即可，不需要 CAS，更不需要锁。`shm_mq_inc_bytes_written`（shm_mq.c:1305）的注释甚至强调用"读+写"而非 `fetch_add`，**省掉总线锁**；
2. **区间所有权**：任意时刻 `[read, written)` 是未读数据，属于 receiver；`[written, read+ringsize)` 是空闲区，属于 sender（shm_mq.c:57-67）。sender 推进 written 是"移交所有权给 receiver"，receiver 推进 read 是"归还空间给 sender"——两个方向的移交互不冲突，这就是不需要互斥的本质原因；
3. **内存序**：所有权移交必须配合屏障，保证"数据写入先于所有权移交可见"。四处配对：
   - sender 写数据前 `pg_memory_barrier()`（shm_mq.c:1044，注释：将本次对 mq_ring 的写与之前对 mq_bytes_read 的读分开——读后写需要全屏障），配对 `shm_mq_inc_bytes_read` 中的 `pg_read_barrier()`（shm_mq.c:1283）；
   - sender 推进计数前 `pg_write_barrier()`（shm_mq.c:1312：先写 ring 再写 written），配对 receiver 读到 written 后、读数据前的 `pg_read_barrier()`（shm_mq.c:1118）。

   两对屏障合起来保证：receiver 看到 written 前进 ⇒ 必能看到对应数据；sender 看到 read 前进 ⇒ 必已完成消费。这是经典 SPSC ring buffer——与 LMAX Disruptor、io_uring 的 SQ/CQ 是同一个模式。

**批量推进优化**（shm_mq.c:113-117 注释 + `mqh_send_pending`）：sender 写入数据后并不立即推进共享计数，而是累积在本地 `mqh_send_pending`，攒到环大小的 1/4 或队列满时才一次性 `shm_mq_inc_bytes_written` + `SetLatch`（shm_mq.c:540-545、990-1004）——共享计数的每次更新都是跨核 cache line 失效，`SetLatch` 更是一次系统调用量级的开销，批量化把两者摊薄。对称地，receiver 用 `mqh_consume_pending` 批量推进 read（shm_mq.c:1101-1102、1149-1153）。

**发送主线**（`shm_mq_send` shm_mq.c:331 → `shm_mq_sendv` shm_mq.c:363 → `shm_mq_send_bytes` shm_mq.c:916）：先发 8 字节长度字，再发 payload（消息语义架在字节流上）；`shm_mq_send_bytes` 的循环三分支——队列满且对端未附着则等附着；满则冲刷 pending、`SetLatch` 叫醒 receiver、nowait 时返回 `SHM_MQ_WOULD_BLOCK` 否则 `WaitLatch` 睡眠；有空间则 `memcpy` 尽可能多（处理环回绕，一次最多写到环尾）。**接收主线**（`shm_mq_receive` shm_mq.c:574 → `shm_mq_receive_bytes` shm_mq.c:1081）对称；一个精妙细节：消息若在环中连续，**直接返回环内指针**不拷贝（shm_mq.c:106-111 注释），只有回绕的消息才拼接到本地缓冲——leader 消费元组通常是零拷贝的。另一个细节（shm_mq.c:1130-1143）：发现 `mq_detached` 后必须**先读屏障再重读一次 written**——sender 可能"推进计数、然后 detach"，直接返回 DETACHED 会丢掉最后一批数据。

拓扑上：N 个 worker = N 条独立 SPSC 队列，leader 逐条轮询。**没有 MPSC 争用**——这是"每 worker 一条队列"而不是"一条共享队列"的动机：多写者队列需要 CAS 竞争写位置，尾延迟不可控。

### 14.2.4 并行度决策与函数安全三级标记

**几个 worker？** `compute_parallel_worker()`（allpaths.c:4973）：表级 `parallel_workers` reloption 优先；否则按表大小对数配置（allpaths.c:5009-5018）：

```c
			heap_parallel_threshold = Max(min_parallel_table_scan_size, 1);
			while (heap_pages >= (BlockNumber) (heap_parallel_threshold * 3))
			{
				heap_parallel_workers++;
				heap_parallel_threshold *= 3;
				if (heap_parallel_threshold > INT_MAX / 3)
					break;		/* avoid overflow */
			}
```

底为 3 的对数：≥8MB（`min_parallel_table_scan_size` 默认值）1 个，≥24MB 2 个，≥72MB 3 个……索引页同理算一份取小，最后 `Min(parallel_workers, max_workers)`——`max_parallel_workers_per_gather` 默认 2（costsize.c:144）。注释自嘲 "This probably needs to be a good deal more sophisticated"——它完全不看谓词选择率、聚合复杂度，只看扫描量。对照 Spark：task 数 = 分区数，由数据布局决定，同样是"只看输入大小"的启发式，但 Spark 有 AQE 在运行时合并/拆分，PG 的并行度一旦定死就不再调整。

**能不能并行？** 每个函数在 `pg_proc.proparallel` 带三级标记（pg_proc.h:177-179）：

```c
#define PROPARALLEL_SAFE		's' /* can run in worker or leader */
#define PROPARALLEL_RESTRICTED	'r' /* can run in parallel leader only */
#define PROPARALLEL_UNSAFE		'u' /* banned while in parallel mode */
```

计划器用 `max_parallel_hazard()`（clauses.c:763）遍历整棵 Query 取"最坏标记"决定 `parallelModeOK`；对单个表达式用 `is_parallel_safe()`（clauses.c:782）判断能否推到 Gather 之下（`max_parallel_hazard_test`，clauses.c:823）。

**为什么必须三级而不是两级？** 因为"不能在 worker 跑"有两种不同的坏法：

- `restricted`（可在 leader、不可在 worker）：函数依赖 **leader 的私有会话状态**。例：`currval('seq')` 读的是本 backend 的序列缓存，worker 的缓存是空的，跑了直接报错 "currval of sequence ... is not yet defined"；`pg_backend_pid()` 在 worker 里返回 worker 的 PID——不报错但**答案错了**。planner 的处理：整条查询仍可并行，但含 restricted 函数的表达式保留在 Gather **之上**由 leader 求值；
- `unsafe`：函数一执行就破坏并行模式的全局约束，连 leader 在并行模式下都不能跑 → **整条查询禁用并行**。典型例子：一个内部执行 `INSERT` 的 PL/pgSQL 函数。假如把它误标成 `PARALLEL SAFE` 塞进 WHERE 里：worker 执行到 INSERT 需要分配 XID——worker 不允许分配 XID（事务状态是从 leader 只读继承的），当场 `ERROR: cannot assign XIDs during a parallel operation`，查询失败。更阴险的例子是 `setseed()`+`random()` 组合：`setseed` 改的是 backend 私有的随机数状态，三个进程各自 set 各自的 seed，后续 `random()` 序列与串行执行完全不同——不报错、结果悄悄不可复现。

自定义函数默认 unsafe——写扩展时忘了标 `PARALLEL SAFE` 是"为什么我的查询不并行"的高频答案。这套标记的哲学与 Spark 对 UDF 的态度形成对照：Spark 假定闭包可序列化、无副作用，出了问题算用户的；PG 假定函数有害，要求作者**显式自证清白**——数据库对正确性的默认立场从来是悲观的。

### 14.2.5 为什么是 scatter-gather 而不是 shuffle

用 Spark 词汇描述 PG 并行查询：**只有一种 exchange，即 worker→leader 的 Gather**。

| 能力 | Spark/MPP | PostgreSQL 并行 |
|---|---|---|
| 数据切分 | 按 key re-partition（exchange/shuffle） | 只按**物理块**切（Parallel Seq Scan 逐页分发） |
| 节点间交换 | 任意 stage 间 shuffle，worker↔worker | 唯一交换：worker → leader（Gather/GatherMerge），星形拓扑 |
| 并行 join | 两表按 join key 重分布 | Parallel Hash（共享哈希表，commit 1804284042）或内侧每 worker 重复执行 |
| 并行聚合 | shuffle 后按 key 并行 finalize | partial（worker）+ finalize（leader 单线程） |

**为什么不做 shuffle？** 三层原因，层层递进：

1. **进程模型成本**。worker↔worker 全连接需要 N² 条队列和每 worker 一个路由分发层；PG 的 worker 是重量级进程（各自的 relcache/catcache/快照管理），不是 Spark 的线程池 task——协调成本的基线完全不同；
2. **计划复杂度**。引入 re-partition 意味着 planner 要为每个可能的交换点枚举"分布属性"（distribution property），像 MPP 优化器（Greenplum 的 orca）那样把"数据按什么 key 分布"纳入 path 比较维度——planner 的搜索空间和代码复杂度翻倍。PG 9.6 团队的选择（见 commit 80558c1f5a 时代的邮件列表讨论）是先交付"能并行扫描和聚合"的 80% 收益；
3. **收益结构**。单机共享缓冲池意味着"数据移动"并不省 I/O（都在同一片 buffer pool），shuffle 省的只是"按 key 汇聚"的重复计算——两阶段聚合恰好能补上其中最常见的场景。

**两阶段聚合怎么补位**：worker 各自对手上数据做部分聚合，输出**转移状态**而非最终值（`AGGSPLIT_INITIAL_SERIAL`，标记于 planner.c:5988；路径构造在 `create_partial_grouping_paths`，planner.c:7606）；Gather 汇聚后 leader 做 `AGGSPLIT_FINAL_DESERIAL`（planner.c:7511）合并转移状态。`avg()` 的转移状态是 `(sum, count)`，合并即相加——这要求聚合函数定义了 `combinefunc`（pg_aggregate.aggcombinefn），没有 combine 函数的聚合（如 `array_agg` 早期）不能并行。

**与 Spark 推演同一条 `SELECT dept, avg(salary) FROM emp GROUP BY dept`**：

- Spark：Stage 1 各 task 对本分区 partialAgg（map 端 combine）→ **按 dept hash shuffle** → Stage 2 各 task 对分到的 dept 子集 finalAgg——**finalize 是并行的**，1 万个 dept 由 200 个 task 分摊；
- PG：3 个 worker 各自 partialAgg 本块区间 → Gather 到 leader → leader 对 3 路 partial 结果 finalAgg——**finalize 单线程**。dept 只有 20 个时毫无差别（每 worker 送 20 组上去，leader 合并 60 行）；dept 有 1000 万个（按用户 ID 分组）时，partial 几乎不缩减数据量，worker 把 1000 万组状态灌给 leader，leader 单线程 hash 合并成为瓶颈——**高基数 GROUP BY 是 PG 并行聚合的结构性短板**。同理受限的还有 DISTINCT、窗口函数按 partition key 汇聚等一切"按 key 重分布"需求。

理解这一点，就理解了 Citus/Greenplum 这类基于 PG 的 MPP 产品补的正是"跨节点 exchange"这一课；也理解了为什么 PG 的并行对"扫得多、出得少"（大表过滤、低基数聚合）收益最大。

### 小结

- 并行的本质成本是**状态克隆**：快照、GUC、combo CID、事务状态、库列表、record typmod……逐项序列化进 DSM，worker 才能"假装是 leader"；每一项都能构造出"漏传即坏"的反例；
- DSM 满、不可中断、worker 注册失败——所有故障路径都退化为"少并行/不并行"，绝不失败；
- 元组回传是每 worker 一条 SPSC 无锁环形队列：单方写者 + 区间所有权 + 两对屏障配对构成正确性证明；批量推进计数摊薄跨核同步；Gather 黏性轮询乱序汇聚，GatherMerge 堆归并保序；
- 并行度按表大小对数；函数 safe/restricted/unsafe 三级标记区分"哪里都能跑 / 只能 leader 跑 / 一跑就坏"；
- 架构选择是 scatter-gather：没有 shuffle，两阶段聚合补位，finalize 单线程是高基数分组的天花板。

---

## 14.3 JIT：把表达式编译成机器码

### 14.3.1 目录结构与 provider 机制

```
src/backend/jit/
├── jit.c                 -- 与 LLVM 无关的薄壳：按需 dlopen provider
└── llvm/
    ├── llvmjit.c         -- provider 实现：上下文、模块、符号解析、代码发射（ORC JIT）
    ├── llvmjit_expr.c    -- 表达式树 → LLVM IR（本节主角）
    ├── llvmjit_deform.c  -- 为具体元组描述符生成专用 tuple deform 函数
    ├── llvmjit_inline.cpp-- 跨函数内联（读内核 bitcode）
    ├── llvmjit_types.c   -- 用 clang 编译出内核类型布局的 bitcode"字典"
    └── llvmjit_wrap.cpp / llvmjit_error.cpp / SectionMemoryManager.cpp
```

内核本体**不链接 LLVM**。`jit.c` 只有回调表：`jit_provider` GUC（默认 "llvmjit"，jit.c:34），首次需要 JIT 时 `provider_init()`（jit.c:68）拼出 `$pkglibdir/llvmjit.so` 路径（jit.c:91）dlopen；失败置 `provider_failed_loading`（jit.c:81/97/108）**静默回退解释执行且不再重试**。`jit_compile_expr()`（jit.c:152）是转发入口。三个代价阈值 GUC 也定义在这（jit.c:40-42：100000 / 500000 / 500000）。这个"薄壳 + 可插拔 provider"让发行版把 JIT 拆成独立子包（LLVM 依赖 100MB+），也给未来更换后端留了接口。

### 14.3.2 llvm_compile_expr 主线：一个 EEOP 步骤如何变成 IR

回忆执行器篇：表达式编译产物 `ExprState` 是线性 opcode 数组（steps），解释器 `ExecInterpExpr` 用 computed goto 逐步执行。JIT 的接入点干净得惊人——`ExecReadyExpr()`（execExpr.c:902）：

```c
static void
ExecReadyExpr(ExprState *state)
{
	if (jit_compile_expr(state))
		return;

	ExecReadyInterpretedExpr(state);
}
```

编译成功则 `state->evalfunc` 指向机器码，失败落回解释器——**上层执行器完全无感知**。这个设计的前提是第 4 篇讲过的表达式重构（PG 10 把递归树求值改为线性 steps）：线性 opcode 数组既是解释器的输入，也是天然的编译 IR 前身——**为 JIT 铺路正是那次重构的隐藏动机**。

`llvm_compile_expr()`（llvmjit_expr.c:80）主线四步：

**1. 上下文与函数骨架**（llvmjit_expr.c:145-170）：每个查询的 EState 挂一个 `LLVMJitContext`（llvmjit_expr.c:146-152，同一查询的所有表达式编进同一模块，一次优化一次发射）；`LLVMAddFunction` 建一个签名等同 `ExecInterpExprStillValid` 的函数（llvmjit_expr.c:164-165）——参数就是 `(ExprState *, ExprContext *, bool *isnull)`，与解释器函数指针完全同型，这是"无感知替换"的 ABI 基础。

**2. 序幕：把常用指针提前加载**（llvmjit_expr.c:179-299）：用 `l_load_struct_gep` 把 `state->resvalue/resnull`、scan/inner/outer/result 槽的 `tts_values/tts_isnull` 数组指针等一次性载入 SSA 寄存器。结构体字段偏移来自 `FIELDNO_*` 宏——`llvmjit_types.c` 用 clang 把内核头编译成 bitcode，运行时从中提取与当前编译器 ABI 完全一致的结构布局，避免手写偏移与真实布局漂移。

**3. 每 step 一个基本块**（llvmjit_expr.c:302-307）：

```c
	/* allocate blocks for each op upfront, so we can do jumps easily */
	opblocks = palloc_array(LLVMBasicBlockRef, state->steps_len);
	for (int opno = 0; opno < state->steps_len; opno++)
		opblocks[opno] = l_bb_append_v(eval_fn, "b.op.%d.start", opno);
```

先建齐全部基本块再填充，任何 step 之间的跳转（QUAL 短路、CASE 分支、BOOL 短路）都是 `LLVMBuildBr(opblocks[目标])`——**解释器的"跳转表 + 间接跳转"变成 IR 里的静态控制流图**，LLVM 优化器因此能做跨 step 的常量传播和死代码消除，这是解释器 dispatch 永远做不到的。

**4. 逐 opcode 翻译**（llvmjit_expr.c:309-324 循环 + switch）。看两个代表：

**EEOP_QUAL**（WHERE 条件短路，llvmjit_expr.c:972-1007）：

```c
			case EEOP_QUAL:
				{
					LLVMValueRef v_resnull;
					LLVMValueRef v_resvalue;
					LLVMValueRef v_nullorfalse;
					LLVMBasicBlockRef b_qualfail;

					b_qualfail = l_bb_before_v(opblocks[opno + 1],
											   "op.%d.qualfail", opno);

					v_resvalue = l_load(b, TypeDatum, v_resvaluep, "");
					v_resnull = l_load(b, TypeStorageBool, v_resnullp, "");

					v_nullorfalse =
						LLVMBuildOr(b,
									LLVMBuildICmp(b, LLVMIntEQ, v_resnull,
												  l_sbool_const(1), ""),
									LLVMBuildICmp(b, LLVMIntEQ, v_resvalue,
												  l_datum_const(0), ""),
									"");

					LLVMBuildCondBr(b,
									v_nullorfalse,
									b_qualfail,
									opblocks[opno + 1]);

					/* build block handling NULL or false */
					LLVMPositionBuilderAtEnd(b, b_qualfail);
					/* set resnull to false */
					LLVMBuildStore(b, l_sbool_const(0), v_resnullp);
					/* set resvalue to false */
					LLVMBuildStore(b, l_datum_const(0), v_resvaluep);
					/* and jump out */
					LLVMBuildBr(b, opblocks[op->d.qualexpr.jumpdone]);
					break;
				}
```

加载上一 step 的结果值和 NULL 标志 → 构造 "NULL 或 false" 布尔 → 条件跳转：失败进 `b_qualfail`（置 false 后直跳 `jumpdone`，整个 qual 的出口）；通过则落到下一 step 的块。对比解释器同一语义：取 opcode → 查跳转表 → 间接跳转（分支预测器的噩梦）；JIT 后是**直连条件分支**，且 `jumpdone` 这类目标编号全部烘焙成了静态跳转。

**EEOP_FUNCEXPR 系列**（函数调用，llvmjit_expr.c:665-751）：非 strict 版本直接 `BuildV1Call`（llvmjit_expr.c:744-747）——生成对具体 `fmgr` 函数（如 `int4pl`）的**直接调用**，函数地址是编译期常量，省掉 `FunctionCallInvoke` 的间接跳转。strict 版本更能体现 JIT 的价值：解释器对 strict 函数用一个通用循环逐参数检查 NULL；JIT 为**每个参数**生成一个专用检查块（llvmjit_expr.c:702-739）——`b_checkargnulls[0..nargs-1]` 串成链，任一参数为 NULL 直接短路到 `opblocks[opno+1]`（结果已预置 NULL，llvmjit_expr.c:700），全非 NULL 才落入 `b_nonull` 调函数。参数个数、检查顺序全部展开成直线代码，没有循环没有计数器。

**deform 专用化**（`llvmjit_deform.c`，commit 32af96b2b1）：为具体 TupleDesc 生成专用解包函数——列数、每列定长/变长、对齐、NULL 位图判断全是编译期常量，逐列展开成直线代码。通用 `slot_deform_heap_tuple` 是"对任意表都对"的解释器，专用版是"只对这张表对"的硬编码。分析查询的时间大头常在 deform（宽表取前几列），这常是 JIT 最大单项收益——EXPLAIN (ANALYZE) 输出里 JIT 的 `Deforming` 计时单列一行就是这个原因。

### 14.3.3 inline：引用内核的 bitcode

把 steps 串成机器码后，`int4pl` 仍是 call 的话就吃不到常量折叠。`llvm_inline()`（llvmjit_inline.cpp:167）实现跨模块内联：构建期用 clang 把整个后端源码**额外编译一份 LLVM bitcode**装到 `$pkglibdir/bitcode/postgres/`（扩展也可装自己的）。运行时：

- 搜索路径默认 `$libdir/postgres`（llvmjit_inline.cpp:192），扩展函数按其模块名追加（llvmjit_inline.cpp:253）；
- 经汇总索引 `*.index.bc`（llvmjit_inline.cpp:812）定位函数所在 bitcode 文件，`snprintf(path, "%s/bitcode/%s", ...)` 打开（llvmjit_inline.cpp:492 附近），把函数体导入当前模块；
- 交给 LLVM 优化器后，`deform + int4lt + qual` 这样的热路径被折叠成几条机器指令——比较函数的调用边界消失，常量比较值直接进指令立即数。

### 14.3.4 代价联动：编译成本的经济学

是否 JIT 由**计划总代价**决定，`standard_planner()` 末尾（planner.c:698-720）：

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

		if (jit_expressions)
			result->jitFlags |= PGJIT_EXPR;
		if (jit_tuple_deforming)
			result->jitFlags |= PGJIT_DEFORM;
	}
```

三级阈值（100000 / 500000 / 500000）实现**分层投资**：先决定"编不编"（基线编译，-O0 量级），更大的查询才追加 -O3 优化和 inline——因为优化和内联本身比基线编译还贵几倍，只有执行时间更长的查询摊得平。

**为什么 PG 选"只编译表达式"而不是 HyPer 式整查询编译？** 收益分布 + 工程半径的双重考量：

- **收益分布**：Volcano 解释开销分两块——节点间的 `ExecProcNode` 递归调用，和节点内的表达式求值/元组解包。Andres Freund 在 JIT 立项时的测量（TPC-H 剖析）显示后者占大头：Q1 这类聚合重的查询 70%+ 时间在表达式和 deform。HyPer 的 data-centric 编译（把整条 pipeline 编成一个循环，消灭节点边界）能把两块都吃掉，但要求**推翻整个执行器**；PG 的渐进路线只动 `evalfunc` 函数指针，执行器一行不改，就吃到了大头收益。80% 收益、5% 侵入性——这是典型的 PG 式取舍；
- **工程半径**：HyPer 是从零设计的学术系统；PG 有百万行存量执行器代码和几十个 PlanState 类型。整查询编译还使 EXPLAIN、错误上下文、中断处理全部复杂化（编译后的大循环里怎么 CHECK_FOR_INTERRUPTS？）。表达式 JIT 保留了逐节点的可观测性。

**为什么 OLTP 常建议关 JIT？** 三个原因叠加：

1. **编译产物不缓存**：编译挂在执行态（EState）上，每次执行都重新编译，不跨查询、不跨 prepared statement 执行复用。一条 2ms 查询摊上 30ms 的 LLVM 编译是 15 倍净亏损；
2. **触发条件是估算代价**：统计失真时（典型：分区表的 Append 把所有分区的代价**加总**与阈值比较），廉价查询也会被误触发。生产事故报告里"升级到 PG 12 后 P99 多出几百毫秒"很多归因于此；
3. **量纲不对齐**：代价模型算的是"执行有多贵"，从不估"编译要花多久"——total_cost 100001 的查询和 10000000 的查询付出的编译成本几乎一样，前者大概率亏本。

摊销模型一句话：设编译成本 C、每行解释执行成本 i、编译后 j（j < i），JIT 盈利当且仅当处理行数 R > C/(i−j)。C 是常数（几十毫秒量级），所以 JIT 天然属于大 R 查询——分析型。OLTP 的 R 以个位数计，永远在盈亏线之下。PG 18 起社区活跃方向正是攻击 C：编译产物缓存复用（prepared statement 场景）、比 LLVM 轻量得多的 copy-and-patch 基线编译器——让"编译快但代码稍差"的档位存在，短查询才可能进入盈利区。落地进度以各版本 release notes 为准。

### 小结

- JIT 是可插拔 provider：jit.c 薄壳 dlopen `llvmjit.so`，失败静默回退且不再重试；
- `llvm_compile_expr` 按"每 step 一基本块"翻译：序幕预载指针、跳转静态化、strict 参数检查全展开、函数调用直连；deform 专用化常是最大单项收益；inline 靠构建期的内核 bitcode 副本；
- 只编译表达式是"80% 收益 / 5% 侵入"的渐进路线，与 HyPer 推翻执行器的路线相对；
- 触发与估算代价联动但不计编译成本、产物不缓存——摊销模型 R > C/(i−j) 注定它属于分析型查询，这就是"OLTP 关 JIT"的全部道理。

---

## 14.4 分区表：裁剪的三个时机

### 14.4.1 partbounds：边界的规范化表示

分区定义（`FOR VALUES ...`）在内存中规范化为 `PartitionBoundInfoData`（partbounds.h:79-96）：

```c
typedef struct PartitionBoundInfoData
{
	PartitionStrategy strategy; /* hash, list or range? */
	int			ndatums;		/* Length of the datums[] array */
	Datum	  **datums;
	PartitionRangeDatumKind **kind; /* The kind of each range bound datum;
									 * NULL for hash and list partitioned
									 * tables */
	Bitmapset  *interleaved_parts;	/* Partition indexes of partitions which
									 * may be interleaved. See above. This is
									 * only set for LIST partitioned tables */
	int			nindexes;		/* Length of the indexes[] array */
	int		   *indexes;		/* Partition indexes */
	int			null_index;		/* Index of the null-accepting partition; -1
								 * if there isn't one */
	int			default_index;	/* Index of the default partition; -1 if there
								 * isn't one */
} PartitionBoundInfoData;
```

由 `partition_bounds_create()`（partbounds.c:300）构建。`datums` 是**排好序**的边界数组，`indexes` 把边界映射到分区号，但三种策略的编码各不相同（partbounds.h:30-63 的注释值得逐句读）：

- **list**：datums 逐值排序，`indexes[i]` = 接受该值的分区，nindexes == ndatums；NULL 值单独走 `null_index`（NULL 不参与排序比较）；
- **range**：相邻分区共享的"上界 = 下界"只存一份，所以 ndatums 通常远小于 2×nparts；`indexes[i]` = 以 datums[i] 为上界的分区，**-1 表示空洞**（该区间没有分区）；末尾多一个 -1 代表"最后一个上界以上"，nindexes == ndatums + 1；`kind` 区分实际值 / MINVALUE / MAXVALUE；
- **hash**：datums 存 (modulus, remainder) 对；indexes 按"最大 modulus 的余数"索引，长度 = 所有分区 moduli 的最小公倍数式最大值，`indexes[hash % nindexes]` 直接命中分区或 -1。

于是"值落在哪个分区"统一为**排序数组上的二分**：list 用 `partition_list_bsearch`（partbounds.c:3600）、range 用 `partition_range_datum_bsearch`（partbounds.c:3688）、hash 直接取模查表。这个表示同时服务三个消费者：DDL 检查分区不重叠（`partition_bounds_equal`，partbounds.c:889 等一族比较函数）、优化器裁剪、执行期路由——**一份规范化，三处收益**。对比 Hive：分区就是 metastore 里的目录名字符串，没有任何"边界代数"，所以 Hive 只能做字符串等值/前缀式的目录过滤，无法回答"WHERE ts < x 命中哪些 range 分区"这种需要有序语义的问题（除非把语义写进查询引擎的常量折叠里）。

### 14.4.2 裁剪程序的生成与执行：gen_partprune_steps → get_matching_partitions

裁剪的难点在于**谓词里的值什么时候可知**：字面量在计划期可知，`$1` 外部参数在执行器启动期可知，子查询输出/nestloop 内侧参数每行都变。PG 的答案优雅在于**只生成一次"裁剪程序"，在不同时机对它求值**。

裁剪程序由 `gen_partprune_steps()`（partprune.c:744）生成，`target` 参数声明用途（partprune.c:735-738 注释）：`PARTTARGET_PLANNER` 只收集不含任何 Param 的子句；`PARTTARGET_INITIAL` 允许外部参数（PARAM_EXTERN）但排除 PARAM_EXEC；`PARTTARGET_EXEC` 全都要。一个细节（partprune.c:759-763）：若本表自身是分区且有 default 分区，把父表的 partition_qual 也并进子句——父层的约束可能帮助裁掉本层的 default 分区。实际的子句→步骤匹配在 `gen_partprune_steps_internal()`（partprune.c:990）：把 `partkey op 常量` 形态的子句按分区键列组织，产出两种步骤节点——`PartitionPruneStepOp`（用某操作符策略号 + 值对边界数组做一次二分）和 `PartitionPruneStepCombine`（对多个步骤的结果集求交/并，对应 AND/OR）。这就是一个微型查询计划：叶子是二分查找，内部节点是集合代数。

**计划期消费者** `prune_append_rel_partitions()`（partprune.c:780）全函数：

```c
Bitmapset *
prune_append_rel_partitions(RelOptInfo *rel)
{
	List	   *clauses = rel->baserestrictinfo;
	...
	if (rel->nparts == 0)
		return NULL;

	if (!enable_partition_pruning || clauses == NIL)
		return bms_add_range(NULL, 0, rel->nparts - 1);

	gen_partprune_steps(rel, clauses, PARTTARGET_PLANNER, &gcontext);
	if (gcontext.contradictory)
		return NULL;
	pruning_steps = gcontext.steps;

	if (pruning_steps == NIL)
		return bms_add_range(NULL, 0, rel->nparts - 1);

	/* Set up PartitionPruneContext */
	context.strategy = rel->part_scheme->strategy;
	...
	context.planstate = NULL;		/* 计划期没有执行态 */
	context.exprcontext = NULL;
	context.exprstates = NULL;

	/* Actual pruning happens here. */
	return get_matching_partitions(&context, pruning_steps);
}
```

要点：子句互相矛盾（`WHERE k=1 AND k=2`）时 `contradictory` 直接返回空集——一个分区都不扫；没有可用步骤则全保留。被裁掉的分区**根本不会进计划树**——EXPLAIN 里看不到它们的任何痕迹。

**求值器** `get_matching_partitions()`（partprune.c:846）：按 step_id 顺序执行每个步骤（Op 步骤 → `perform_pruning_base_step` 做二分，Combine 步骤 → 对前面结果做交/并，partprune.c:872-895），**最后一个步骤的结果就是答案**（partprune.c:902）——线性依赖的"寄存器机"。之后把边界偏移翻译回分区号（partprune.c:907-932）：range 的 -1 空洞意味着"查询范围触及无分区覆盖的区间"，若存在 default 分区则把它加入扫描集（partprune.c:914-928）——**default 分区是裁剪的天敌**：任何"范围没被显式分区完全覆盖"的不确定性都会把 default 拖进来。

### 14.4.3 三个时机：plan-time / initial / exec pruning

**阶段一：计划期**。如上，值全为常量时在 planner 里彻底解决。等价于 Hive 的静态分区裁剪（编译期过滤目录）。

**阶段二：执行器启动期（initial pruning）**。`$1` 这类外部参数在计划期未知，但在执行器启动时已经绑定。`make_partition_pruneinfo()`（partprune.c:225）在建计划时把 initial/exec 两套步骤打包成 `PartitionPruneInfo` 挂到 Append/MergeAppend 上。20devel 的执行流程（PG 18 起，commit d47cbf474e 重构）：`ExecDoInitialPruning()`（execPartition.c:1995）在 **ExecInitNode 之前**统一遍历 `estate->es_part_prune_infos`，对每个 pruneinfo 调 `CreatePartitionPruneState()`（execPartition.c:2145，调用点 execPartition.c:2008）并立即执行 initial 步骤，把存活子计划位图存进 `es_part_prune_results`；随后 Append 初始化时经 `ExecInitPartitionExecPruning()`（execPartition.c:2051）取回位图，**被裁掉的子计划连 ExecInitNode 都不做**——锁不加、快照不查、状态不建。EXPLAIN ANALYZE 显示为 `Subplans Removed: N`。重构的动机：老流程在 ExecInitNode 内部裁剪，导致"要初始化 Append 才知道能裁谁，但初始化本身要先锁全部分区"的先有鸡先有蛋问题，阻碍了泛型计划对锁开销的优化。

**阶段三：运行期（exec pruning）**。引用 PARAM_EXEC（nestloop 内侧参数、子查询输出）的步骤每次参数变化都要重算：Append 每次 rescan 时对 exec 步骤求值刷新存活位图。典型场景 `t JOIN part_table p ON p.key = t.x`：外侧每来一行，内侧 Append 只扫 x 命中的那一个分区——**逐行粒度的裁剪**，这是 Hive/Spark 的 DPP（dynamic partition pruning，物化广播侧后过滤目录）达不到的粒度（DPP 是 stage 粒度、一次性的）。

**所以然④：运行期裁剪到底救了谁——prepared statement 泛型计划的完整场景**。

```sql
PREPARE q(timestamptz) AS SELECT * FROM orders WHERE ts = $1;  -- orders 按月 range 分区，120 个分区
```

前 5 次执行 planner 走 **custom plan**：每次用实参重新计划，$1 是常量，计划期裁剪直接把计划裁到 1 个分区——完美但每次付计划成本。第 6 次起 plancache 比较成本，若切到 **generic plan**：$1 在计划期永远未知，**计划期裁剪彻底失效**，计划树里 Append 挂着全部 120 个子计划。若没有运行期裁剪（PG 11 之前的世界）：每次执行都实打实初始化并扫描 120 个分区（各自索引查一遍）——分区表 + prepared statement 的组合直接劝退。有了 initial pruning：$1 是 PARAM_EXTERN，启动期求值裁到 1 个分区，`Subplans Removed: 119`，执行成本几乎追平 custom plan，同时省掉了每次重计划。**运行期裁剪本质上是把"常量折叠"从编译期延展到了每个值可知的时机**——这句话是三阶段设计的全部精髓。遗留成本：泛型计划的 plan 树本身仍包含 120 个子计划的元数据（锁与 relcache 开销部分仍在，PG 18 的 d47cbf474e 正在继续压缩这块）。

### 14.4.4 写路径：ExecFindPartition 全函数走读

INSERT/COPY 进分区表时每行要找到叶子分区。`ExecFindPartition()`（execPartition.c:268）全函数骨架：

```c
	/* use per-tuple context here to avoid leaking memory */
	oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	if (rootResultRelInfo->ri_RelationDesc->rd_rel->relispartition)
		ExecPartitionCheck(rootResultRelInfo, slot, estate, true);

	/* start with the root partitioned table */
	dispatch = pd[0];
	while (dispatch != NULL)
	{
		int			partidx = -1;
		bool		is_leaf;

		CHECK_FOR_INTERRUPTS();
		rel = dispatch->reldesc;
		partdesc = dispatch->partdesc;

		ecxt->ecxt_scantuple = slot;
		FormPartitionKeyDatum(dispatch, slot, estate, values, isnull);

		if (partdesc->nparts == 0 ||
			(partidx = get_partition_for_tuple(dispatch, values, isnull)) < 0)
		{
			... ereport(ERROR, ... "no partition of relation \"%s\" found for row" ...);
		}

		is_leaf = partdesc->is_leaf[partidx];
		if (is_leaf)
		{
			if (likely(dispatch->indexes[partidx] >= 0))
				rri = proute->partitions[dispatch->indexes[partidx]];	/* 已缓存 */
			else
				... ExecLookupResultRelByOid / ExecInitPartitionInfo ...	/* 首次命中，惰性建 */
			dispatch = NULL;		/* 终止循环 */
		}
		else
		{
			/* 子分区表：取/建下一层 PartitionDispatch，继续下钻 */
			...
			dispatch = pd[dispatch->indexes[partidx]];
			/* 若本层 tuple 布局与上层不同，先做属性映射转换 */
			if (dispatch->tupslot)
				slot = execute_attr_map_slot(map, slot, myslot);
		}

		/* default 分区必须现场重查分区约束 */
		if (partidx == partdesc->boundinfo->default_index)
			ExecPartitionCheck(rri, slot, estate, true);
	}
```

逐段解说：

1. **逐层下钻**：`FormPartitionKeyDatum` 从元组算分区键值——分区键可以是表达式（`PARTITION BY RANGE (date_trunc('month', ts))`），所以必须走表达式求值机制，这也是需要 `ecxt_scantuple` 的原因（execPartition.c:308-316 注释）；多级分区（先按月 range、再按租户 hash）就是 while 多转几圈；
2. **`get_partition_for_tuple()`**（execPartition.c:1570）按策略分派：hash 算 `compute_partition_hash_value` 后 `indexes[hash % nindexes]` 直接命中；list/range 走二分。**带缓存**（execPartition.c:1579-1588 注释）：同一分区连续命中超过 `PARTITION_CACHED_FIND_THRESHOLD` 次后，先用缓存的边界快速验证——批量导入按时间有序到来时，几乎每行都命中上一行的分区，把二分降为一次比较。hash 不缓存（取模本来就 O(1)）；
3. **惰性初始化**：`ResultRelInfo`（含索引信息、触发器、约束）只在分区**首次接到行**时构建（execPartition.c:348-387）——万级分区的表若本批只写 3 个分区，只建 3 份状态；
4. **default 分区的特殊照顾**（execPartition.c:449-483）：路由进 default 分区的行必须现场重查分区约束——**default 的"负空间"边界会因并发 `CREATE TABLE ... PARTITION OF` 而收缩**，靠的是路由时重查 + DDL 侧对 default 分区加锁扫描已有行，两边夹住不变式"行永远落在约束成立的分区里"；
5. **找不到分区**报 `no partition of relation ... found for row`——与 Hive 动态分区"自动建目录"相反，PG 的分区集合是 DDL 定义的封闭集合，写入不会隐式创建分区（这是社区长期讨论的功能空缺，目前由 pg_partman 等外部工具补位）。

### 14.4.5 Partitionwise join/aggregate 与 Hive/Spark 对照

两个默认关闭的优化（costsize.c:161-162）：

- **分区智能连接** `try_partitionwise_join()`（joinrels.c:1611，PG 11，commit f49842d1ee）：两表分区方式相同且 join key = 分区键时，把大 join 分解为逐对分区的小 join 再 Append。每对 join 更小、哈希表更可能放进 work_mem——等价于 Spark 两个同 bucket 数的 bucketed 表 co-located join 免 shuffle；
- **分区智能聚合** `create_partitionwise_grouping_paths()`（planner.c:8383）：GROUP BY 含全部分区键时聚合完整下推到分区内（PARTITIONWISE_AGGREGATE_FULL）；否则分区内 partial + 上层 finalize。

为什么默认 off？计划期成本随分区数**乘性**放大：partitionwise join 要对每对分区枚举 join path，几百分区 × 复杂 join 的计划内存和耗时可能超过执行本身。Spark 没有这个问题是因为它的"计划"粒度粗（stage 级），不为每对 bucket 做代价枚举——精细计划是把双刃剑。

**与 Hive 分区对比总表**：

| | Hive | PostgreSQL |
|---|---|---|
| 分区元数据 | metastore 目录名（字符串） | 系统目录 + 规范化排序边界数组 |
| 静态裁剪 | 编译期按字面量过滤目录 | 阶段一（计划期），能力等价 |
| 动态裁剪 | DPP：stage 粒度、需物化广播侧 | 阶段二/三原生机制，参数/行粒度 |
| 分区实体 | 目录，无独立统计/索引 | 每分区是完整的表：独立统计、索引、存储参数 |
| 写入未知分区 | 动态分区自动建目录 | 报错（封闭集合） |
| 分区规模 | 十万级轻松 | 数千级要留意计划期开销 |

Hive 的分区是**文件系统布局的衍生物**，扩展性来自"不管理"；PG 的分区是**一等表对象**，能力来自"全管理"（每分区可独立 ANALYZE/VACUUM/建索引），代价是每分区的目录行、锁、relcache 都是真实开销。两者不是谁先进，而是"元数据密度"光谱的两端。

### 小结

- 边界规范化为排序 datums + indexes 映射，三种策略三种编码（range 有 -1 空洞、hash 按最大模数展开）；裁剪与路由都是二分/取模；
- 裁剪程序（Op 步骤 + Combine 步骤）只生成一次，按"值何时可知"在计划期/启动期/运行期三个时机求值；PG 18 把 initial pruning 提到 ExecInitNode 之前，被裁子计划连初始化都省掉；
- 泛型计划让计划期裁剪失效，initial pruning 把它救回来——运行期裁剪的本质是把常量折叠延展到每个值可知的时机；
- ExecFindPartition 逐层下钻 + 命中缓存 + 惰性建状态；default 分区在裁剪端拖累（不确定性都归它）、在路由端要重查约束；
- partitionwise 优化把大问题切成分区粒度，但计划成本乘性放大故默认关闭；对比 Hive：动态裁剪粒度 PG 完胜，分区规模上限 Hive 完胜。

---

## 14.5 不变式清单

每条给出"是什么、代码在哪守护、违反后果"。

1. **worker 与 leader 快照一致**：worker 的 active/transaction snapshot 必须与 leader 逐字节相同（parallel.c:403-414 序列化，ParallelWorkerMain 恢复）。违反：worker 与 leader 对同一元组可见性判断不同，Gather 结果丢行或多行，且随并发提交时序漂移、无法稳定复现——比崩溃难查一个量级。
2. **combo CID 映射完整**：worker 必须持有 leader 的全部 combo CID 映射（parallel.c:394-397）。违反：本事务内"先插后改"的行在 worker 中 cmin/cmax 解码错误，同一查询内部可见性自相矛盾。
3. **并行模式禁写**：worker 及并行模式下的 leader 不得分配 XID/写表（事务状态以只读参与者身份继承，parallel.c:423-426）。违反（假如没有这个检查）：多进程并发扩展同一事务的子事务栈与 combo CID 表，事务状态结构在无锁保护下被并发写坏；现实中被 `cannot assign XIDs during a parallel operation` 类错误挡住。
4. **shm_mq 单方写者**：`mq_bytes_read` 只有 receiver 写、`mq_bytes_written` 只有 sender 写（shm_mq.c:36-43）。违反：8 字节原子读写 + 屏障的整套正确性论证瓦解，需要退回互斥锁。
5. **shm_mq 屏障配对**：sender "先写 ring 再推 written"（write barrier，shm_mq.c:1312）配 receiver "先读 written 再读 ring"（read barrier，shm_mq.c:1118）；receiver "先读完 ring 再推 read"（read barrier，shm_mq.c:1283）配 sender 写 ring 前的全屏障（shm_mq.c:1044）。违反（弱内存序平台如 ARM 上去掉任一）：receiver 读到"计数已推进但数据未落地"的 ring 内容——元组二进制层面损坏。
6. **元组队列 EOF 协议**：worker 完成后 detach 队列，`TupleQueueReaderNext` 以 readerdone=true 上报，leader 从 reader 数组剔除（nodeGather.c:341-357）。违反（worker 异常退出未 detach）：由 bgworker 句柄 + `shm_mq_counterparty_gone` 兜底检测，否则 leader 会在 WaitLatch 上永久等待。
7. **样本物理有序**：`rows[]` 在统计计算前必须按 TID 排序（analyze.c:1379-1381）。违反：correlation 的 y 序列不再是物理序，相关性统计变成噪声，`cost_index` 的随机/顺序插值失真——索引扫描代价系统性错估。
8. **分区边界不重叠、default 唯一**：`partition_bounds_create` 排序时检查重叠，DDL 对新分区与 default 分区互斥加锁验证。违反：一行有两个合法归属，路由二分的结果依赖比较顺序——同一行两次 INSERT 可能进不同分区，唯一约束与裁剪同时失效。
9. **裁剪程序的时机纪律**：PARTTARGET_PLANNER 步骤不得引用任何 Param，PARTTARGET_INITIAL 不得引用 PARAM_EXEC（partprune.c:735-738）。违反：对未绑定的参数求值——直接段错误或读到垃圾值裁错分区（裁错 = 悄悄丢数据，比崩溃更糟）。
10. **pruneinfo 与计划节点对齐**：`ExecInitPartitionExecPruning` 校验 `bms_equal(relids, pruneinfo->relids)`（execPartition.c:2066-2069），不匹配直接 elog(ERROR)——防止 pruneinfo 列表与计划树节点错位后"用 A 表的裁剪结果裁 B 表"。

---

## 14.6 设计权衡与历史（git 考古）

按时间线（均为本仓库 `git log` 现场核实）：

| commit | 日期 | 内容 | 版本 |
|---|---|---|---|
| ec9037df26 | 2014-01-14 | shm_mq：SPSC 共享内存消息队列 | 9.4 |
| 924bcf4f16 | 2015-04-30 | 并行基础设施：DSM 状态序列化 + 错误传播 | 9.5/9.6 |
| 3bd909b220 | 2015-09-30 | Gather 执行节点 | 9.6 |
| 80558c1f5a | 2015-11-11 | 生成并行顺序扫描计划 | 9.6 |
| 45be99f8cd | 2016-01-20 | 并行 join | 9.6 |
| e06a38965b | 2016-03-21 | 并行聚合（partial/finalize 两阶段） | 9.6 |
| f0e44751d7 | 2016-12-07 | 声明式分区（PARTITION BY 语法、边界规范化、元组路由） | 10 |
| 355d3993c5 | 2017-03-09 | Gather Merge 节点 | 10 |
| 7b504eb282 | 2017-03-24 | 扩展统计：多列 ndistinct | 10 |
| 2686ee1b7c | 2017-04-05 | 扩展统计：函数依赖 | 10 |
| f49842d1ee | 2017-10-06 | partition-wise join | 11 |
| 1aba8e651a | 2017-11-09 | hash 分区 | 11 |
| 1804284042 | 2017-12-20 | Parallel Hash Join（共享哈希表） | 11 |
| 2a0faed9d7 / 432bb9e04d / cc415a56d0 / 32af96b2b1 | 2018-03-20~26 | JIT 四连：表达式编译、provider 基础设施、planner 集成、tuple deforming | 11 |
| 9fdb675fc5 | 2018-04-06 | Faster partition pruning（gen_partprune_steps 框架取代逐分区约束证明） | 11 |
| 499be013de | 2018-04-07 | 运行期分区裁剪（initial + exec pruning） | 11 |
| 7300a69950 | 2019-03-27 | 扩展统计：多列 MCV | 12 |
| 041b96802e | 2024-04-08 | ANALYZE 接入流式预读（read stream） | 17 |
| d47cbf474e | 2025-01-31 | initial pruning 移出 ExecInitNode（ExecDoInitialPruning） | 18 |

四条历史线索的解读：

1. **并行查询是五年工程**（2014-2018）：先队列（9.4）、再状态克隆（9.5）、再执行节点（9.6）、再逐个算子并行化（9.6-11 的 join/agg/hash join/index scan）。924bcf4f16 的提交信息明确列出四件事——启动协调、状态同步、**并行模式下禁止不安全操作**、错误传播——"先把不安全的都禁掉，再逐步解禁"是整条演进线的方法论；
2. **分区表两步走**：PG 10（f0e44751d7）只交付"语法 + 路由"，裁剪仍沿用祖传的逐分区约束证明（constraint exclusion，线性且慢）；PG 11 的 9fdb675fc5 才换成边界二分的裁剪框架，499be013de 紧接着（隔一天！）把框架推广到运行期。**先立数据结构，后收性能**；ExecDoInitialPruning（d47cbf474e）说明这条线 8 年后仍在演进；
3. **JIT 一周落地四个 commit**（2018-03-20 到 03-26），但其真正前置是 PG 10 的表达式求值线性化重构——大特性的关键铺垫常在一两个版本之前就埋好；
4. **扩展统计三连跳**（PG 10 ndistinct/dependencies → PG 12 MCV）：从"最便宜的修正"到"最精确的修正"逐版本递进，每一步都保持"没建扩展统计的用户零成本"。

---

## 14.7 what-if 推演

### 推演 1：统计陈旧 + 分区裁剪失效叠加的慢查询

**场景**：`events` 按天 range 分区保留 90 天，每天新建明日分区并写入当日数据。查询 `SELECT * FROM events WHERE ts > now() - interval '1 hour' AND user_id = 42`。凌晨切日后 autovacuum 尚未 ANALYZE 今日新分区。

**推演链**：① `now() - interval '1 hour'` 是 stable 表达式，计划期不可折叠——计划期裁剪裁不掉任何分区，靠 initial pruning 在启动期裁到今日分区；② 今日分区统计为空（`relpages=0, reltuples=-1` 的新表约定），planner 对它按默认启发式估行数；早晨该分区实际已有千万行但统计仍是"接近空表"——`user_id = 42` 的选择率被乐观估计，planner 可能选 `Seq Scan`（"表很小，扫一遍便宜"）而不是 user_id 索引；③ 若查询还带 `ORDER BY ts LIMIT 100`，低估行数会诱导 planner 选"按 ts 索引倒序扫、边扫边过滤 user_id"的计划——在 user_id=42 的行很稀疏时这是灾难（扫几百万行才凑够 100 行）；④ 叠加效应：分区表的统计只在**分区级**收集，`events` 父表的查询计划依赖每个存活分区自己的统计——90 个老分区统计精准、1 个新分区统计真空，恰恰所有新数据查询都打在统计真空的那个分区上。**教训**：按时间分区的表，新分区上线后应立即手动 `ANALYZE`（或用 `autovacuum_analyze_scale_factor` 的分区级 reloption 调激进）；这类"统计冷启动"是分区表特有的运维成本，Hive 没有这个问题只是因为它根本不做代价优化。

### 推演 2：并行 worker 启动失败时计划如何退化

**场景**：计划要 4 个 worker，但实例的 `max_worker_processes` 池已被逻辑复制槽和其他并行查询占满。

**推演链**：① `LaunchParallelWorkers()`（parallel.c:583）循环调 `RegisterDynamicBackgroundWorker`，第一个注册失败后置 `any_registrations_failed = true`，**后续 slot 不再尝试**（parallel.c:639-651 注释：既然池满了，再试大概率也失败），并 detach 掉为未启动 worker 预留的错误队列——否则 leader 会等一个永远不来的 worker；② `nworkers_launched` 可能是 0..4 之间任何值。**计划不变、不重规划**：Gather 的 reader 数组按实际启动数建（nodeGather.c:194-209），0 个 worker 时 `need_to_scan_locally = true`，leader 单进程执行整棵并行子树——**并行计划天然是"1 到 N 进程都能跑"的弹性计划**，Parallel Seq Scan 的块分发器对"只有 leader 一个消费者"同样工作；③ 性能后果：4 worker 的计划被 planner 按 `parallel_divisor ≈ 4.x` 摊薄了代价估计，实际单进程跑意味着真实耗时约是估计的 4 倍以上——**计划的最优性假设被运行时环境击穿，但正确性无损**；④ 观测：EXPLAIN ANALYZE 显示 `Workers Planned: 4, Workers Launched: 0`；PG 17 起 `pg_stat_database.parallel_workers_to_launch/launched`（对应 nodeGather.c:190-191 的计数）可全局监控这种"并行饥饿"。**教训**：`max_worker_processes`（进程池总量，改动要重启）> `max_parallel_workers`（并行查询可用量）> `max_parallel_workers_per_gather` 三层配额要留梯度，否则高峰期所有并行计划集体退化为串行，吞吐雪崩没有任何报错。

### 推演 3：JIT 编译比查询本身还慢的翻车与自保

**场景**：`orders` 按月分区 36 个月，`PREPARE q AS SELECT count(*) FROM orders WHERE status = $1` 走泛型计划。

**推演链**：① 泛型计划下 Append 挂 36 个子计划，planner 把 36 个分区的扫描代价**加总**为 top_plan->total_cost ≈ 120000，越过 `jit_above_cost = 100000` → jitFlags 置 PGJIT_PERFORM（planner.c:698-703）；② 执行时 initial pruning 并不存在（status 不是分区键），确实要扫 36 个分区——但每个分区只有几万行，总执行 80ms；③ JIT 编译发生在执行器初始化阶段：36 个子计划的表达式（每分区的 qual + 聚合）全部收进同一 LLVM 模块编译，几百个函数的 IR 生成 + 基线编译花 150ms——**编译 150ms + 执行 80ms**，JIT 让查询慢了近 3 倍；④ 更糟的是 prepared statement 每次执行都重编（产物挂在 EState 上，随执行态销毁）；⑤ 若 total_cost 再高些越过 500000，还会追加 -O3 和 inline，编译成本再翻几倍——代价估算越离谱（比如统计陈旧高估了行数），JIT 赔得越多。**自保参数**：`jit_above_cost` 调到与实例查询画像匹配的量级（OLTP 实例直接 `jit = off`）；EXPLAIN (ANALYZE) 的 `JIT: Timing` 一栏是诊断入口（Generation/Inlining/Optimization/Emission 分项计时）；分区多的实例尤其注意"代价加总触发 JIT"这个已知陷阱。**根因回顾**：三个结构性缺陷叠加——阈值比较的是估算代价、代价模型不含编译成本、产物不缓存（见 14.3.4）。

---

## 自测问题（10 题带详解）

1. **为什么 ANALYZE 默认只采 30000 行就敢给亿行大表做统计？采样分几个阶段，各用什么算法？**
   详解：直方图桶边界是分位数估计，误差随样本量 r 以 exp(-2rε²) 速率收敛，与表大小无关；表大小只在联合失败概率的 ln(n) 里出现（Chaudhuri et al. SIGMOD 1998，analyze.c:1980-1997 注释，r = 4k·ln(2n/γ)/f²，300 = 305.82 取整）。两阶段：块级 Knuth Algorithm S（块数已知，顺序概率入选 + 跳跃合并优化，sampling.c:64）、行级 Vitter Algorithm Z 水库采样（行数未知，`reservoir_get_next_S` 一次算出跳过多少行，t 大时期望 O(1) 随机数，sampling.c:147）。两阶段交织执行，样本最后按 TID 回排（analyze.c:1379）。

2. **stadistinct = -0.5 是什么意思？Duj1 估计器在哪两类分布上会失准，方向各是什么？**
   详解：负数是比例：distinct ≈ 0.5 × 当前行数，随表增长外推（估计超过总行数 10% 时转比例存储，analyze.c:2716）。失准场景：重尾分布（少数热值 + 千万级长尾）→ 样本 f1 偏小 → **低估**，连锁导致 HashAgg 内存爆炸；低频均匀分布（每值出现 N/d 次但 d ≪ N）→ 样本几乎全是 singleton，f1≈n → **高估**至接近 N。这是采样估基数的固有局限（有下界定理），故 ndistinct 是唯一提供 `ALTER TABLE ... SET (n_distinct=...)` 人工覆盖的统计量。

3. **函数依赖统计的修正公式是什么？它在什么输入下反而比独立假设更错？**
   详解：P(b|a) = f·min(P(a),P(b))/P(a) + (1−f)·P(b)（dependencies.c:1106-1120），degree=1 且 P(a)≤P(b) 时 P(a,b)=P(a)。反例：**不相容的常量组合**——`city='北京' AND province='广东省'`，真实选择率 0，依赖修正给出 ≈P(city) 高估；因为 degree 是全表平均，无法感知具体值对是否共现。联合 MCV 按值查询没有此问题，所以 `statext_clauselist_selectivity`（extended_stats.c:2024）先 MCV 后依赖。

4. **InitializeParallelDSM 为什么必须序列化 combo CID 映射和两个快照？为什么要先 Estimate 后 Serialize 两趟？**
   详解：快照决定可见性边界，worker 快照与 leader 有偏差就会返回不一致行集（ACTIVE_SNAPSHOT 必传，parallel.c:411-414；TRANSACTION_SNAPSHOT 仅 RR 以上，parallel.c:403）；combo CID 是本事务"先插后改"元组的 cmin/cmax 压缩映射，在 leader 私有内存，worker 缺了它对本事务修改过的行做出错误可见性判断（parallel.c:394-397）。两趟是因为 DSM 段创建后无法扩容——必须先用 estimator 算准总大小再一次 `dsm_create`（parallel.c:328），这与堆内存的 repalloc 习惯根本不同。

5. **gather_readnext 为什么"黏着"同一个队列读而不是逐 tuple 轮转？Gather 的输出为什么乱序？**
   详解：nodeGather.c:363-368 注释——黏着读直到队列空才轮转，连续读同一段共享内存的 cache 局部性好得多，还摊薄了 read 计数的批量推进。输出顺序取决于哪个 worker 的队列先有数据，本质是"完成顺序"而非"数据顺序"——需要保序时 planner 生成 GatherMerge（每 worker 有序流 + leader 二叉堆 N 路归并，nodeGatherMerge.c:430/547）。

6. **shm_mq 为什么不需要锁？写出 sender 和 receiver 各自需要的屏障及其配对。**
   详解：单方写者——`mq_bytes_written` 只 sender 写、`mq_bytes_read` 只 receiver 写（shm_mq.c:36-43），单写者变量用 8 字节原子读写即可。屏障两对：sender 先写 ring 数据、`pg_write_barrier()`（shm_mq.c:1312）后推 written ↔ receiver 读到 written 后 `pg_read_barrier()`（shm_mq.c:1118）再读数据；receiver 读完数据 `pg_read_barrier()`（shm_mq.c:1283）后推 read ↔ sender 读 read 后 `pg_memory_barrier()`（shm_mq.c:1044，读后写需全屏障）再写 ring。任何一条在弱内存序平台缺失都会读到未落地数据。

7. **一个内部执行 INSERT 的函数被误标为 PARALLEL SAFE 会怎样？restricted 和 unsafe 的处置差别是什么？**
   详解：worker 执行到 INSERT 需要分配 XID，而 worker 以只读参与者身份继承 leader 事务，当场报 `cannot assign XIDs during a parallel operation`。restricted（如 `currval`，依赖 leader 私有会话状态）：查询仍并行，但该表达式保留在 Gather 之上由 leader 求值；unsafe：`max_parallel_hazard()`（clauses.c:763）遍历发现后整条查询禁并行。自定义函数默认 unsafe——宁可不并行也不冒险。

8. **PG 并行为什么做不了高效的高基数 GROUP BY？用 Spark 对照说明，并指出内核的补位机制及其瓶颈。**
   详解：worker 间无 re-partition exchange，唯一数据通路是 worker→leader 的 Gather（星形拓扑）。Spark：partial → 按 key shuffle → **并行** finalize；PG：worker partial（AGGSPLIT_INITIAL_SERIAL，planner.c:5988）→ Gather → **leader 单线程** finalize（AGGSPLIT_FINAL_DESERIAL）。组数少时 partial 大幅缩减数据量、瓶颈不存在；组数百万级时 partial 不缩减，leader 合并成为串行瓶颈。不做 shuffle 是进程模型成本、planner 复杂度与收益结构的三重权衡——补这一课的正是 Citus/Greenplum。

9. **JIT 的盈利条件用摊销模型写出来；解释 PG 为什么先编译表达式而不是整个执行器。**
   详解：编译成本 C、每行解释成本 i、编译后 j，盈利当且仅当行数 R > C/(i−j)——C 是几十毫秒级常数，所以只有大 R（分析型）查询盈利；三级阈值（planner.c:698-712，100000/500000/500000）是分层投资：更贵的 -O3/inline 留给更大的查询。选表达式而非整执行器：剖析显示表达式求值 + tuple deform 占解释开销大头，替换 `ExprState->evalfunc` 函数指针即可接入（execExpr.c:902），执行器零改动拿 80% 收益；HyPer 式整查询编译收益更高但要求推翻百万行存量执行器并牺牲逐节点可观测性。

10. **`PREPARE q(date) AS ... WHERE ts = $1` 查按月分区表，custom plan 和 generic plan 下裁剪各发生在哪个阶段？`JOIN ... ON p.key = t.x` 呢？**
    详解：custom plan——$1 以实参代入，计划期裁剪（`prune_append_rel_partitions`，partprune.c:780），被裁分区不进计划树；generic plan——$1 计划期未知，计划树含全部分区，靠启动期 initial pruning（`ExecDoInitialPruning`，execPartition.c:1995，PG 18 起在 ExecInitNode 之前执行，被裁子计划连初始化都不做），EXPLAIN 显示 `Subplans Removed`。join 参数是 PARAM_EXEC、逐行变化——exec pruning：Append 每次 rescan 用 `ExecInitPartitionExecPruning`（execPartition.c:2051）建立的状态重算存活位图，逐行只扫命中分区。三阶段共用 `gen_partprune_steps`（partprune.c:744）生成的同一套裁剪程序，差别只在求值时机（PARTTARGET_PLANNER/INITIAL/EXEC）。

---

## 延伸阅读

- `src/backend/access/transam/README.parallel`：并行模式的完整设计文档（状态同步、禁写规则、错误传播）；
- `src/backend/statistics/README`、`README.dependencies`、`README.mcv`：扩展统计的算法说明；
- `src/backend/jit/README`：JIT 的架构与 bitcode inline 机制；
- `src/backend/partitioning/`、`src/backend/executor/execPartition.c` 头部注释：分区裁剪与路由的导览；
- Vitter, "Random sampling with a reservoir", ACM TOMS 1985；Chaudhuri et al., "Random sampling for histogram construction", SIGMOD 1998；Haas & Stokes, IBM RJ 10025（ndistinct 估计器）；Neumann, "Efficiently Compiling Efficient Query Plans for Modern Hardware", VLDB 2011（HyPer 编译路线，与 PG 渐进路线对照）。
