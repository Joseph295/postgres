# T8 内核 Trick 大全

> 基于 PostgreSQL 20devel（master 分支）源码现场核实，"文件:行号"指本仓库当前工作副本；
> 若代码变动，可用引文中的函数名/宏名重新定位。本篇不重复各子系统整体机制（见第 01–15 章），
> 只聚焦技巧本身，每个 trick 按统一结构剖析：**解决什么问题 → 朴素做法的缺陷 →
> trick 的做法（真实代码）→ 正确性论证 → 代价与适用边界 → 在别处的复用**。

## 目录

| 分组 | 编号 | Trick |
|------|------|-------|
| 数据结构 | 1 | ItemId：页内行指针间接层 |
| | 2 | ctid 版本链与 HOT 重定向 |
| | 3 | NodeTag 手工多态 + 自动生成 copy/equal/out/read |
| | 4 | Bitmapset：把关系集合压成位图（兼谈柔性数组成员） |
| | 5 | FSM 页内隐式二叉堆 |
| 并发 | 6 | Hint bits："可再生"的事务状态缓存 |
| | 7 | xmax 藏行锁：锁存在数据行里 |
| | 8 | Fastpath 锁的 Dekker 式两阶段检查 |
| | 9 | LWLock：一个 32 位原子字打包全部状态 |
| | 10 | sinval 的 lock-free hasMessages 旗标 |
| | 11 | 组提交 leader/follower（ProcArray/CLOG 组更新） |
| | 12 | ProcArray dense arrays：把热字段镜像成紧凑数组 |
| | 13 | SetLatch 与 self-pipe：信号安全的进程唤醒 |
| 内存 | 14 | MemoryContext 区域回收与 per-tuple 重置 |
| | 15 | Chunk header 的 4-bit 方法 ID：free(p) 如何知道 p 属于谁 |
| 磁盘与 I/O | 16 | FPW：检查点后"第一次修改才整页" |
| | 17 | WAL 段复用不清零 + prev-link/CRC 判定终点 |
| | 18 | Ring buffer：大表扫描不污染缓冲池 |
| | 19 | Backend fsync 请求转发给 checkpointer |
| 协议与兼容 | 20 | Magic block：加载时 ABI 自检 |
| | 21 | kwlist.h X-macro + 编译期完美哈希 |
| | 22 | errcontext 回调栈：错误信息的"逆向注入" |
| 编译器 | 23 | Computed goto：解释器直接线索化 |
| | 24 | likely/unlikely 与 StaticAssert：给编译器递情报 |
| | 25 | C 语言手工虚函数表（TableAmRoutine/FdwRoutine） |

文末：**共同思想提炼**（六条设计母题）与**自测题（7 题带详解）**。

---

# 一、数据结构

## Trick 1：ItemId——页内行指针间接层

**它解决什么问题。** 外部（索引、其他行的 ctid）需要长期、稳定地引用页内的一行；但页内空间
管理又要求能自由移动元组（碎片整理、pruning）。两者直接矛盾。

**朴素做法及其缺陷。** 让 TID 直接是"页号 + 页内字节偏移"。那么页内一旦做碎片整理（把存活
元组搬到页尾压实），所有指向被移动元组的索引项全部失效，必须同步改写索引——每次整理一个堆页
要连带写多个索引页，代价不可接受。

**trick 的做法。** 页头之后放一个行指针数组，TID 只引用"页号 + 数组下标"，真实偏移藏在
数组元素里。每个元素被压进 4 字节（`src/include/storage/itemid.h:25`）：

```c
typedef struct ItemIdData
{
    unsigned    lp_off:15,      /* offset to tuple (from start of page) */
                lp_flags:2,     /* state of line pointer, see below */
                lp_len:15;      /* byte length of tuple */
} ItemIdData;
```

`lp_flags` 的四个状态（itemid.h:38–41）：`LP_UNUSED`/`LP_NORMAL`/`LP_REDIRECT`/`LP_DEAD`。

**为什么是对的。** 8 KB 页内偏移和长度都 < 2^15，两个 15 位字段无损；碎片整理只改 `lp_off`，
数组下标（即 OffsetNumber）不变，因此所有外部引用（索引项、ctid 链、EPQ 重查）继续有效。
`LP_DEAD` 让"行已死但槽位还被索引引用"成为可表达状态：堆元组存储可以立即回收，而槽位要等
索引项清理后（VACUUM 第二阶段）才置 `LP_UNUSED` 复用——这正是"引用还在，就不能复用编号"
的正确性条件的机器表达。

**代价与适用边界。** 每行 4 字节固定开销；读一行要两跳（先读槽再读元组）。槽位数组只能在
页尾方向增长，`lp_off:15` 把页大小上限钉死在 32 KB（`BLCKSZ` 最大值正是 32768）。

**在别处的复用。** 索引页同样用 ItemId 数组（nbtree、GiST 等复用 `PageAddItem` 一套
bufpage.c 例程）；`LP_REDIRECT` 是 HOT（Trick 2）的地基；`LP_DEAD` 在 nbtree 里被复用为
kill_prior_tuple 的"索引项已死"标记。

## Trick 2：ctid 版本链与 HOT 重定向

**它解决什么问题。** MVCC 下 UPDATE 产生新版本，读者拿着旧版本的 TID 必须能找到新版本
（EPQ 语义）；同时希望"未改索引列的 UPDATE 不插新索引项"。

**朴素做法及其缺陷。** 每次 UPDATE 都向所有索引插入指向新版本的项。索引膨胀速度与更新频率
成正比，即使一次 UPDATE 只改了一个非索引列，N 个索引也要 N 次插入 + 未来 N 次清理。

**trick 的做法。** 两层：①每个元组头的 `t_ctid` 指向"自己的下一个版本"，形成页内/跨页
版本单链表；②若新旧版本在同一页且索引列未变（HOT 更新），索引项保持指向旧位置，读者沿链
前进。旧版本被 prune 掉后，其槽位改为 `LP_REDIRECT`，`lp_off` 字段被重用为"下一个槽号"
（itemid.h:76–79）。索引扫描侧的链遍历在
`heap_hot_search_buffer()`（`src/backend/access/heap/heapam_indexscan.c:90`）：

```c
        if (!ItemIdIsNormal(lp))
        {
            /* We should only see a redirect at start of chain */
            if (ItemIdIsRedirected(lp) && at_chain_start)
            {
                /* Follow the redirect */
                offnum = ItemIdGetRedirect(lp);      /* heapam_indexscan.c:133-135 */
                at_chain_start = false;
                continue;
            }
            /* else must be end of chain */
            break;
        }
```

**为什么是对的。** 链的完整性靠"下一版本的 xmin == 上一版本的 xmax"这一不变式校验
（heapam_indexscan.c:161–166：`TransactionIdEquals(prev_xmax, HeapTupleHeaderGetXmin(...))`
不匹配即视为断链）——因为槽位可能被复用，单靠槽号会指到无关元组，xmin/xmax 配对是防
ABA 的逻辑签名。HOT 的前提"索引列未变"保证索引项 → 链头 → 沿链找可见版本的搜索结果与
逐版本插索引项完全等价；任一 MVCC 快照在链上至多看到一个可见版本，所以顺链首个可见者即答案。

**代价与适用边界。** 跨页 UPDATE（页内放不下或改了索引列）退化为普通更新，仍需全索引插入；
链遍历使读放大随版本数线性增长，依赖 pruning 及时截短；`LP_REDIRECT` 槽本身是无法回收的
4 字节残留，直到整页所有引用它的索引项消亡。

**在别处的复用。** EPQ（`EvalPlanQual`）沿 `t_ctid` 追最新版本实现 READ COMMITTED 的
半一致读；`heap_index_delete_tuples` 的 bottom-up deletion 同样借道 HOT 链判定索引项可删；
详见第 08 章（MVCC）与第 10 章（pruning）。

## Trick 3：NodeTag 手工多态 + 自动生成 copy/equal/out/read

**它解决什么问题。** 解析树/计划树有数百种节点类型，需要运行时类型识别（RTTI）、深拷贝、
相等比较、序列化/反序列化——而实现语言是 C。

**朴素做法及其缺陷。** 为每种节点手写 copy/equal/out/read 四套函数。数百节点 × 每节点十几个
字段 × 四套 = 上万行纯机械代码，历史上（PG 15 之前）真是手写的，新增字段忘记同步四处是
最常见的 bug 来源之一。

**trick 的做法。** 第一层是手工多态：每个节点第一个成员都是 `NodeTag type`，于是任何节点
指针都可安全地按 `Node *` 读 tag（`src/include/nodes/nodes.h:137,162`）：

```c
#define nodeTag(nodeptr)        (((const Node*)(nodeptr))->type)
#define IsA(nodeptr,_type_)     (nodeTag(nodeptr) == T_##_type_)
```

构造统一走 `makeNode`（nodes.h:148–160）：`newNode(size, tag)` 即 `palloc0` + 打 tag。
第二层是元编程：`src/backend/nodes/gen_node_support.pl` 在构建期解析所有节点头文件的
C 结构定义，自动生成四套函数（gen_node_support.pl:633–636 打开
`copyfuncs.funcs.c`、`equalfuncs.funcs.c` 等输出文件）。不宜按默认规则处理的字段用
空宏标注属性（nodes.h:124 `#define pg_node_attr(...)` ——对编译器是空操作，对脚本是元数据），
例如 `src/include/nodes/pathnodes.h:175`：

```c
    ParamListInfo boundParams pg_node_attr(read_write_ignore);
```

**为什么是对的。** C 标准保证指向结构体的指针可与指向其首成员的指针互转（C11 6.7.2.1p15），
所以"首成员必为 NodeTag"这一约定使 `nodeTag()` 对任何节点都是良定义的读取；这正是单继承
对象布局的手工版。生成器读的是编译器要编译的同一份结构定义，四套函数与结构体定义
**建构性地**一致——字段增删后忘记同步的错误类别被整体消灭，而非靠 review 防守。

**代价与适用边界。** 无编译器检查的"继承"：把 `Node *` 强转成错误类型只有 `Assert(IsA(...))`
在 debug 构建兜底。生成规则只覆盖常规字段类型，特殊语义仍需 `pg_node_attr` 人工标注，
标错（如该拷贝的标了 ignore）生成的代码照样错。

**在别处的复用。** 同一 NodeTag 机制被扩展 API 借用作 ABI 检查：`ExtensibleNode`、
`TableAmRoutine`（Trick 25）、`FdwRoutine` 都以 `T_Xxx` tag 开头供 `IsA` 校验；
`src/backend/catalog/genbki.pl`、`gen_keywordlist.pl`（Trick 21）是同一"构建期从单一
事实源生成代码"思想的姊妹实现。

## Trick 4：Bitmapset——把关系集合压成位图（兼谈柔性数组成员）

**它解决什么问题。** 优化器里"关系编号集合"无处不在（`RelOptInfo->relids`、join 搜索的
已连接集合、参数化路径的依赖集），需要海量的并/交/子集判定，且集合要作为哈希键参与查找。

**朴素做法及其缺陷。** 用 `List *` 存整数。判断子集是 O(n·m) 的双重遍历；求并要排序去重；
比较两个集合是否相等更是麻烦；内存上每个元素一个 ListCell。join 搜索是指数级空间上的枚举，
集合运算在最内层，这个常数因子直接决定规划耗时。

**trick 的做法。** 定长字为单位的位图（`src/include/nodes/bitmapset.h:49`）：

```c
typedef struct Bitmapset
{
    pg_node_attr(custom_copy_equal, special_read_write, no_query_jumble)
    NodeTag     type;
    int         nwords;         /* number of words in array */
    bitmapword  words[FLEXIBLE_ARRAY_MEMBER];   /* really [nwords] */
} Bitmapset;
```

成员定位是两次位运算（`src/backend/nodes/bitmapset.c:48–49`）：

```c
#define WORDNUM(x)  ((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)   ((x) % BITS_PER_BITMAPWORD)
```

于是 `bms_is_subset` 是逐字 `(a & ~b) == 0`，`bms_union` 是逐字 `|`，一条指令处理 64 个
成员。注意 `words[FLEXIBLE_ARRAY_MEMBER]`：结构体与变长数组一次 `palloc` 连续分配
（bitmapset.c:217 `bms_make_singleton` 中 `palloc0(BITMAPSET_SIZE(wordnum + 1))`），
这个 C99 柔性数组成员惯用法（`src/include/c.h:617`）贯穿全库——varlena（c.h:837）、
`HeapTupleHeaderData.t_bits`、`PGAlignedBlock` 之外几乎所有"头 + 变长体"结构都用它，
省掉一次间接指针和一次独立分配。

**为什么是对的。** 关系编号是小而稠密的整数（rtindex 从 1 递增），位图对这种域是信息论意义
上近乎最优的编码；集合运算与按位布尔代数同构（∪↔OR、∩↔AND、⊆↔`a&~b=0`），逐字运算的
正确性由布尔代数逐位独立性保证。PG 17 起 Bitmapset 还维持"最高位字非零"的规范形式不变式，
使"内容相等 ⇔ 逐字节相等"，可直接作哈希键。

**代价与适用边界。** 只适合小而稠密的整数域：成员值上界决定内存（一个成员值 10 万的集合
要 12 KB）。稀疏大域该用哈希表或 radix tree（如 TidStore）。多数集合操作返回新分配对象，
调用方需注意别名与释放约定（近年改为多数场景允许原地重用以减开销）。

**在别处的复用。** 优化器全域（`relids`、`param_info->ppi_req_outer`、EquivalenceClass
成员集）；`hba.c` 解析、分区剪枝的存活分区集、`RelOptInfo->eclass_indexes` 等索引集合。

## Trick 5：FSM 页内隐式二叉堆

**它解决什么问题。** 插入一行要快速找到"任意一个空闲空间 ≥ x 的堆页"，且这张"空闲空间地图"
自身要能容忍崩溃后不一致。

**朴素做法及其缺陷。** 线性扫描每页的空闲字节记录：O(表大小)。或维护精确的按空闲量排序的
全局结构：每次插入/删除都要 WAL 化维护，写放大巨大，还成为全局竞争点。

**trick 的做法。** 每个 FSM 页是一棵**隐式完全二叉树**（数组下标编码父子关系，零指针），
叶子存每个堆页的空闲档位（1 字节，粒度 BLCKSZ/256），内部节点存子树最大值
（`src/backend/storage/freespace/fsmpage.c:29–31`）：

```c
#define leftchild(x)    (2 * (x) + 1)
#define rightchild(x)   (2 * (x) + 2)
#define parentof(x)     (((x) - 1) / 2)
```

查找从根下降，谁的档位够就往谁走（fsmpage.c:245–260）：

```c
    while (nodeno < NonLeafNodesPerPage)
    {
        int         childnodeno = leftchild(nodeno);

        if (childnodeno < NodesPerPage &&
            fsmpage->fp_nodes[childnodeno] >= minvalue)
        {
            nodeno = childnodeno;
            continue;
        }
        childnodeno++;          /* point to right child */
        ...
```

页与页之间再叠三层同构的树（freespace.c 的高层寻址），整个 FSM 是"树的树"。
FSM 更新**不写 WAL**。

**为什么是对的。** 内部节点 = max(子树) 这一不变式保证：根 ≥ x ⇒ 必存在叶 ≥ x，下降每步
都保持"当前子树里有解"，故 O(log n) 必达。FSM 允许不写 WAL 的根本原因是它只是**建议**：
拿到候选页后插入代码会真正加锁验证空间，FSM 过时最多导致一次徒劳的访问，随后就地修正。
崩溃留下的"父承诺子不兑现"的撕裂页也被就地发现并重建（fsmpage.c:263 起的
"Fix the corruption and restart" 分支）——数据结构自愈，而非依赖恢复流程。

**代价与适用边界。** 1 字节档位有量化误差（约 32 字节粒度）；"建议性"意味着并发下会有
多进程撞同一页，故搜索起点带 `fp_next_slot` 轮转扰动（fsm_internals.h:27 注释）。
不适用于需要精确空间承诺的场景。

**在别处的复用。** Visibility Map 是同一"每堆页几个 bit 的旁路小地图"思路（无树结构）；
"允许过时 + 使用时验证"的建议性缓存模式再现于 relcache（使用时凭 sinval 失效）与
hint bits（Trick 6）。隐式二叉堆同样出现在 `binaryheap.c`（merge append、异步 append 用）。

---

# 二、并发

## Trick 6：Hint bits——"可再生"的事务状态缓存

**它解决什么问题。** 判可见性要知道 xmin/xmax 是否已提交。查 CLOG（含 SLRU 缓存与锁）
对每行每次都做太贵，热点表一次顺序扫描可能触发上亿次 CLOG 查询。

**朴素做法及其缺陷。** 方案 A：每次都查 CLOG——慢且 CLOG 成为全局热点，还使 CLOG 永远
不能截断。方案 B：提交时回头把本事务碰过的所有行标成"已提交"——提交延迟与写行数成正比，
且要记住全部脏行位置，代价荒谬。

**trick 的做法。** 第一次真正查完 CLOG 后，把结论作为 hint bits（`HEAP_XMIN_COMMITTED` 等
infomask 位）写回元组头，**且不写 WAL**。核心在
`SetHintBitsExt()`（`src/backend/access/heap/heapam_visibility.c:142`）：

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

**为什么是对的。** 关键论证是**可再生性 + 单调性**：hint bit 丢了（崩溃时脏页未落盘）无非
下次重查 CLOG，结论不变——因为"xid 已提交/已中止"是单调事实，一旦为真永远为真，缓存永不
需要失效协议。所以它可以不写 WAL、可以在只持共享锁时设置（打位是对齐字节的原子写，PG 20
里进一步经 `BufferSetHintBits16`/`BufferBeginSetHintBits` 与异步 I/O 互斥，避免在页面写出
期间修改导致校验和撕裂，见 heapam_visibility.c:83–99 的状态机注释）。上面引文是唯一的
微妙点：异步提交的事务 commit 记录可能尚未刷盘，若 hint 先落盘而崩溃回放又没有该提交，
就出现"标记已提交实际未提交"的假 hint——因此仅当 commit LSN 已刷盘、或页面 LSN 本身
≥ commit LSN（页面落盘必先刷 WAL 到页 LSN，形成传递性 interlock）时才允许设置。

**代价与适用边界。** "不写 WAL"在开启数据校验和/`wal_log_hints` 时被打破：hint-bit-only
的脏页若在检查点后首次写出，必须发 `XLOG_FPI_FOR_HINT` 全页镜像
（`src/backend/access/transam/xloginsert.c:1134` `XLogSaveBufferForHint`，:1172 处
`XLogInsert(RM_XLOG_ID, XLOG_FPI_FOR_HINT)`）——否则部分写会撕裂校验和保护的页。这是
"免费缓存"与"页完整性可校验"之间的经典折中：买校验和，就要为 hint 补交 WAL 税。
另一代价：大量首读会造成"读操作产生写"（读放大转写放大），迁移/恢复后首扫尤其明显。

**在别处的复用。** 索引的 `LP_DEAD`（kill_prior_tuple）是同构技巧：扫描时顺手标记已死
索引项，不 WAL 化、丢了可再生；`pg_database.datfrozenxid` 之类的前推、以及 relcache
的 `rd_isvalid` 都属"可再生断言缓存"家族。

## Trick 7：xmax 藏行锁——锁存在数据行里

**它解决什么问题。** SELECT FOR UPDATE / 外键校验需要行级锁，行数可达上亿，而共享内存
锁表容量固定。

**朴素做法及其缺陷。** 每行锁在共享内存锁表里占一项。一条 `SELECT ... FOR UPDATE` 扫过
一千万行就需要一千万个锁表项——内存不可控。DB2/早期 Sybase 走"锁升级"路（行锁攒多了升
表锁），代价是并发度骤降与意外死锁。

**trick 的做法。** 行锁**不进锁表**，直接记录在元组头的 `t_xmax` + infomask 里：xmax 字段
写上锁持有者的 xid，再用 `HEAP_XMAX_LOCK_ONLY` 等位区分"这是锁不是删除"
（`src/include/access/htup_details.h:197,229`）：

```c
#define HEAP_XMAX_LOCK_ONLY     0x0080  /* xmax, if valid, is only a locker */
...
static inline bool
HEAP_XMAX_IS_LOCKED_ONLY(uint16 infomask)
{
    return (infomask & HEAP_XMAX_LOCK_ONLY) ||
        (infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK;
}
```

多个事务同时锁一行时，xmax 放不下多个 xid，就升级为 MultiXactId
（`HEAP_XMAX_IS_MULTI`，htup_details.h:209）——一个指向"成员 xid 列表"的间接编号，
列表存在专门的 SLRU 里。等待冲突行锁时才短暂借用锁表：等待者对**持锁者的 xid** 调
`XactLockTableWait`，即锁表里锁的是"事务"这个粒度恒定的对象，而非行。

**为什么是对的。** 行锁的语义是"直到持有者事务结束"，而 MVCC 本来就要求每行携带 xmax 并
让读者判断 xmax 事务的状态——锁信息与版本信息共享同一判定机器：`xmax = xid + LOCK_ONLY`
对可见性判定是"没有删除"（HeapTupleSatisfies* 都先查 `HEAP_XMAX_IS_LOCKED_ONLY`），对
写冲突检测是"有人持锁"。释放无需任何动作：事务结束后，别人看到 xmax 事务已完结即视同
锁已消失——锁的生命周期被**寄生**在事务生命周期上，所以不需要显式释放路径，也就没有
崩溃泄漏问题。内存占用 O(被锁行数) 变 O(0)（写在本来就存在的行头里）。

**代价与适用边界。** 锁状态持久化在数据页上：加行锁会弄脏页、写 WAL（为了崩溃后 MultiXact
成员可查）。MultiXact 是著名的复杂度与 wraparound 火药桶（成员 SLRU 需独立冻结）。
判断"锁是否仍被持有"要查事务状态，比查锁表贵。且无法枚举"谁锁了哪些行"（pg_locks 里
看不到未冲突的行锁）——可观测性换扩展性。

**在别处的复用。** 元组头 infomask 里同样"寄生"的还有 `HEAP_KEYS_UPDATED`、
`HEAP_HOT_UPDATED` 等版本链元数据；"把状态寄生在必然存在的载体上以免独立生命周期管理"
的思路再现于 hint bits（Trick 6）与 `LP_DEAD`。

## Trick 8：Fastpath 锁的 Dekker 式两阶段检查

**它解决什么问题。** 每条 SQL 都要对涉及的表拿 AccessShareLock 等弱锁。弱锁之间从不冲突，
却都要进分区哈希锁表，高并发短查询下锁表分区锁成为首要瓶颈。

**朴素做法及其缺陷。** 所有锁一律进共享锁表：为从不冲突的锁付全额共享内存同步成本。
或者弱锁完全不记录：DDL（强锁）来时无法发现冲突，正确性崩塌。

**trick 的做法。** 弱锁记在**每 backend 私有的 fastpath 槽**里（仅受该 backend 自己的
`fpInfoLock` 保护）；另设一个全局计数数组 `FastPathStrongRelationLocks->count[1024]`
记录"每个哈希分区上强锁的存在性"。弱锁获取路径
（`src/backend/storage/lmgr/lock.c:992–1004`）：

```c
            /*
             * LWLockAcquire acts as a memory sequencing point, so it's safe
             * to assume that any strong locker whose increment to
             * FastPathStrongRelationLocks->counts becomes visible after we
             * test it has yet to begin to transfer fast-path locks.
             */
            LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE);
            if (FastPathStrongRelationLocks->count[fasthashcode] != 0)
                acquired = false;               /* 走慢路径 */
            else
                acquired = FastPathGrantRelationLock(...);
            LWLockRelease(&MyProc->fpInfoLock);
```

强锁获取方对称地先递增 count（lock.c:4498 附近 "Bump strong lock count, to make sure any
fast-path lock requests won't ..."），**然后**逐个扫描所有 backend 的 fastpath 槽，把已存在
的弱锁"搬运"进共享锁表，搬运时逐个持对方的 `fpInfoLock`。

**为什么是对的。** 这是 Dekker 算法的结构：双方各自"先写自己的意图旗标，再读对方的"。
弱锁方：写槽位（在持 fpInfoLock 下）→ 读 count；强锁方：写 count → 读所有槽位（逐个持
fpInfoLock）。两个进程的"写→读"对如果都不重排，则不可能双方都读到对方"不存在"：
至少一方会看见另一方。这里不用显式 `pg_memory_barrier`，而是**借用 LWLockAcquire 自带的
全栅栏语义**（引文注释言明）。于是三种交错都正确：弱锁方读到 count≠0 → 主动走慢路径，
在共享锁表相遇；强锁方扫描时看到槽位 → 搬运后在共享锁表冲突检测；弱锁写槽发生在强锁方
持同一 fpInfoLock 扫描之后 → 但那时 count 已非零，弱锁方必然读到。不存在两边都
"隐身"的第四种交错。

**代价与适用边界。** 仅限"按关系 OID 的弱 DML 锁"（`EligibleForRelationFastPath`）；
每 backend 槽组有限（默认 16 个，PG 18 起可扩），溢出退慢路径；强锁变得更贵（要扫全体
backend）——正确的方向性取舍：DDL 罕见，SELECT 无处不在。`pg_locks` 视图必须额外汇集
各 backend 私有槽，观测复杂度上升。

**在别处的复用。** 同样的"先立旗、后检查 + 借既有临界区当栅栏"出现在
`SetLatch`/`WaitEventSetWait` 的 `maybe_sleeping` 协议（Trick 13）和 sinval 的
hasMessages（Trick 10）。谓词锁（SSI）的 `PredicateLockPromotion` 阈值则是对照组：
它选择了传统的粒度升级路线。

## Trick 9：LWLock——一个 32 位原子字打包全部状态

**它解决什么问题。** LWLock 是内核里数量最大（每个缓冲区 descriptor 一个 content lock）、
频率最高的同步原语，它的快路径每省一条原子指令都是全局收益；同时结构体必须小（数万个
实例常驻共享内存）。

**朴素做法及其缺陷。** 经典实现：spinlock 保护 {独占标志, 共享计数, 等待队列}。每次加解锁
都要 spinlock 的一对原子操作 + 临界区，无竞争路径也要两次原子往返；spinlock 本身在高核数
下退化严重（PG 9.5 之前正是如此，多核只读负载被 buffer mapping lock 压垮）。

**trick 的做法。** 把独占位、共享持有者计数、"有等待者"、"队列自锁"全部塞进**一个**
`pg_atomic_uint32 state`（`src/include/storage/lwlock.h:44`；位分配见
`src/backend/storage/lmgr/lwlock.c:96–108`）：

```c
#define LW_FLAG_HAS_WAITERS         ((uint32) 1 << 31)
#define LW_FLAG_WAKE_IN_PROGRESS    ((uint32) 1 << 30)
#define LW_FLAG_LOCKED              ((uint32) 1 << 29)
...
#define LW_VAL_EXCLUSIVE            (MAX_BACKENDS + 1)
#define LW_VAL_SHARED               1
#define LW_LOCK_MASK                (MAX_BACKENDS | LW_VAL_EXCLUSIVE)
```

无竞争加锁是单次 CAS（`LWLockAttemptLock`，lwlock.c:764，节选 :783–795）：

```c
        if (mode == LW_EXCLUSIVE)
        {
            lock_free = (old_state & LW_LOCK_MASK) == 0;
            if (lock_free)
                desired_state += LW_VAL_EXCLUSIVE;
        }
        else
        {
            lock_free = (old_state & LW_VAL_EXCLUSIVE) == 0;
            if (lock_free)
                desired_state += LW_VAL_SHARED;
        }
```

精妙处在编码本身：共享计数占低 18 位（≤ `MAX_BACKENDS` = 2^18−1），独占"位"取值
`MAX_BACKENDS + 1`——恰好是共享计数域之上的第一个位。于是"锁空闲" = `(state & LW_LOCK_MASK) == 0`
一次测试同时覆盖两种模式；共享加锁是 `+1`，独占加锁也是"加一个常数"，CAS 循环两种模式共用。
等待队列自身的 spinlock 也被吸收为 state 里的 `LW_FLAG_LOCKED` 位。

**为什么是对的。** 所有状态在同一个字里，意味着"读取-判断-修改"由一次 CAS 原子完成，
不存在两个字段分别更新造成的中间态窗口。位域互不重叠由编译期断言保证
（lwlock.c:117 `StaticAssertDecl((LW_VAL_EXCLUSIVE & LW_FLAG_MASK) == 0, ...)`）；
共享计数不会溢出进独占位，因为持有者至多 MAX_BACKENDS 个，而 MAX_BACKENDS 本身被
定义为 2^18−1 并有断言钉死。释放-唤醒竞态（释放者没看见新等待者/等待者没看见释放）
用经典的"入队后再重试一次获取"（双重检查）弥合。

**代价与适用边界。** CAS 循环在极端竞争下有活锁倾向（比排队 spinlock 公平性差）；
32 位塞满后没有扩展余地（PG 18 把 MAX_BACKENDS 提升到 2^18−1 时就动过这套编码）；
无死锁检测、不可重入——这些被明确接受为 LWLock 的契约。

**在别处的复用。** buffer descriptor 的 `state`（refcount + usagecount + 标志位打包进一个
原子 u32/u64，bufmgr）是同一手法的另一次独立应用；`procArrayGroupFirst` 等无锁链表头
（Trick 11）同样依赖"单字 CAS 表达复合状态"。

## Trick 10：sinval 的 lock-free hasMessages 旗标

**它解决什么问题。** 每个事务开始都要问一句"有没有新的目录失效消息？"绝大多数时候答案是
"没有"。这条**否定路径**的成本乘以每秒事务数，就是它的全局成本。

**朴素做法及其缺陷。** 读共享的全局消息计数 `maxMsgNum` 与自己的 `nextMsgNum` 比较。
但读写双方都要经同一把自旋锁/读写锁（历史实现如此），使"通常什么都不做"的检查在所有
CPU 之间弹跳同一条缓存行，成为可测量的扩展性瓶颈。

**trick 的做法。** 给每个 backend 一个**各自缓存行里的**私有旗标 `hasMessages`
（`src/backend/storage/ipc/sinvaladt.c:146`）。写者广播完消息后逐个置 true
（sinvaladt.c:435–441，在持 SInvalWriteLock 下）；读者的快路径完全无锁
（sinvaladt.c:496–510）：

```c
    if (!stateP->hasMessages)
        return 0;                       /* 快路径：无锁，无原子操作 */

    LWLockAcquire(SInvalReadLock, LW_SHARED);
    /*
     * We must reset hasMessages before determining how many messages we're
     * going to read.  That way, if new messages arrive after we have
     * determined how many we're reading, the flag will get reset and we'll
     * notice those messages part-way through. ...
     */
    stateP->hasMessages = false;
    /* 然后才读 maxMsgNum，取走消息 */
```

**为什么是对的。** 危险场景只有一个：读者错过消息（读到 false 但其实有新消息）。逐条论证：
①写者的顺序是"写消息数组 → 置 hasMessages=true"，且置旗后释放 SInvalWriteLock 构成
释放栅栏（sinvaladt.c:430 注释言明借锁释放当栅栏）；②读者的顺序是"清旗 → 读 maxMsgNum"
——**先清后读**是全部要害：若清旗之后又有新消息到达，写者会再次置 true，下一次检查必然
看见；若把顺序颠倒成"读完再清"，就会把清旗动作盖到新消息的旗标上，永久丢失。③读者快路径
读到 false 的时刻，逻辑上等价于"失效消息再晚到一纳秒"——由于读者尚未开始使用任何受保护
的缓存条目和锁，这个线性化点的选择是合法的（sinvaladt.c:490 附近注释："it is not
guaranteed ... 但等价于消息稍晚到达"）。旗标是 bool 单字节写，天然免撕裂。

**代价与适用边界。** 假阳性代价保留（置了旗但消息其实早被读过——无害，多走一次慢路径）；
每 backend 一个旗标占一条缓存行的思想要求 ProcState 布局配合；只适用于"错过通知可以由
下一次通知补救"的单调场景，不能直接搬到需要精确一次投递的队列。

**在别处的复用。** `latch->maybe_sleeping`/`is_set` 协议（Trick 13）是同一"先改自己
状态、后检查对方状态、用屏障钉序"家族；ProcSignal 的 `pss_signalFlags` 也是
每进程旗标 + 慢路径确认的结构。

## Trick 11：组提交 leader/follower——ProcArray/CLOG 组更新

**它解决什么问题。** 事务提交尾声要在 ProcArray 里清掉自己的 xid（需独占 ProcArrayLock），
要更新 CLOG 页（需 CLOG 控制锁）。高并发小事务下，所有提交者在这两把锁上排队，锁本身的
交接开销（唤醒、缓存行转移）超过临界区实际工作。

**朴素做法及其缺陷。** 每个提交者各自 `LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE)`；
拿不到就睡。N 个并发提交 = N 次锁交接，每次交接都是一轮调度延迟，吞吐随并发不升反降。

**trick 的做法。** 拿不到锁的提交者不睡等锁，而是把自己 CAS 进一个无锁单链表；链表**第一个**
成员成为 leader，代表全组干活
（`ProcArrayGroupClearXid`，`src/backend/storage/ipc/procarray.c:784`，节选 :795–806）：

```c
    proc->procArrayGroupMember = true;
    proc->procArrayGroupMemberXid = latestXid;
    nextidx = pg_atomic_read_u32(&procglobal->procArrayGroupFirst);
    while (true)
    {
        pg_atomic_write_u32(&proc->procArrayGroupNext, nextidx);

        if (pg_atomic_compare_exchange_u32(&procglobal->procArrayGroupFirst,
                                           &nextidx,
                                           (uint32) pgprocno))
            break;
    }
```

`nextidx != INVALID_PROC_NUMBER` 的人是 follower，睡在自己的信号量上等结果
（procarray.c:814 起）；leader 拿一次 ProcArrayLock，`pg_atomic_exchange_u32` 原子摘下
整条链（procarray.c:846），替全组清 xid，最后逐个唤醒。CLOG 侧的
`TransactionGroupUpdateXidStatus`（`src/backend/access/transam/clog.c:450`）结构完全同构。

**为什么是对的。** ①无锁入队的 ABA 与丢失问题：CAS 到表头的 push 是标准 Treiber 栈的
入队半边，唯一的 pop 是 leader 的 `exchange(&first, INVALID)` 整链摘取——不存在
"摘中间节点"的操作，所以没有 ABA 窗口。②不会有无 leader 的组：第一个成功 CAS 空表头的
进程读到的 `nextidx` 必为 INVALID，因此**恰好**他自知是 leader（procarray.c:808–812 注释：
"It is impossible to have followers without a leader..."）。③follower 的等待不检查锁而
检查 `proc->procArrayGroupMember`（leader 清它），醒于伪唤醒也只会重新睡（:824 循环）。
④语义等价性：清 xid 各成员互不相关、且都必须在持 ProcArrayLock 下进行，leader 批量执行与
各自执行的可串行化结果相同；latestXid 的全局推进取组内最大值，与逐个推进的最终值一致。
锁交接次数从 O(N) 降到 O(组数)，临界区内的缓存行也只被 leader 一个 CPU 触碰。

**代价与适用边界。** follower 的延迟略升（多一次信号量往返）；只适合"操作可由他人代做、
结果只有全局副作用"的临界区（清 xid、改 CLOG 位），不适合需要返回复杂私有结果的操作；
代码复杂度和唤醒风暴（leader 逐个 SetLatch）是实打实的成本。

**在别处的复用。** 两处实例（ProcArray、CLOG）共享 PGPROC 里的一套字段模式；WAL 写盘的
`LWLockAcquireOrWait` + "醒来发现别人已把我的 WAL 刷了就直接返回"（XLogFlush 的
group-flush 行为）是同一思想在 I/O 上的版本。

## Trick 12：ProcArray dense arrays——把热字段镜像成紧凑数组

**它解决什么问题。** 拍快照要在持锁下扫过全部活跃 backend 的 xid/xmin。PGPROC 是每个约
八百多字节的大结构体，散布在共享内存各处；扫 N 个 backend 就要摸 N 条互不相邻的缓存行，
快照成本随连接数线性放大（PG 13 及以前的著名瓶颈）。

**朴素做法及其缺陷。** 遍历 `procArray->pgprocnos[]` 逐个解引用 `allProcs[procno].xid`。
每个 xid 独占一条缓存行（旁边全是无关字段），预取器无从发力；连接数 5000 时拍一次快照
就是 5000 次随机内存访问。

**trick 的做法。** 把快照需要的少数热字段从 PGPROC **镜像**到按 `pgxactoff` 紧密排列的
平行数组 `ProcGlobal->xids[]`（等，见 `src/include/storage/proc.h:400–408` 注释），
真相双写，读路径只扫紧凑数组。维护镜像的代价花在进程进出 ProcArray 时
（`src/backend/storage/ipc/procarray.c:512–524`）：

```c
    memmove(&ProcGlobal->xids[index + 1],
            &ProcGlobal->xids[index],
            movecount * sizeof(*ProcGlobal->xids));
    ...
    proc->pgxactoff = index;
    ProcGlobal->xids[index] = proc->xid;
```

数组始终保持无洞（成员退出时 memmove 压实，后续成员的 `pgxactoff` 集体前移，
procarray.c:530–539）。

**为什么是对的。** 一致性不变式是 `allProcs[pgprocnos[i]].pgxactoff == i` 且
`ProcGlobal->xids[i]` 恒等于对应 PGPROC 的 `xid`（procarray.c:501 的 Assert 现场校验）。
双写会不会被读到中间态？镜像字段的修改被限定在持 ProcArrayLock（或 XidGenLock）下进行
（proc.h:405–408 注释言明并发规则），而快照读取也持 ProcArrayLock 共享模式——锁界定了
可见性边界，数组重排（memmove）只发生在独占持锁下。性能上这是把 O(N) 次缓存行未命中
变成 O(N/16) 次顺序预取命中（4 字节 xid × 16 = 一条缓存行），并让 SIMD 化成为可能。

**代价与适用边界。** 双写使每处修改 xid 的代码都要记得同步镜像（封装在
`ProcArrayInstallImportedXmin` 等少数入口里）；进程退出的 memmove 是 O(N)——用低频
操作的线性代价换高频操作的缓存友好，连接数极高且进出频繁的场景（无连接池的短连接风暴）
会把代价暴露出来。

**在别处的复用。** 列式存储对行式存储的优势是同一原理的宏观版本；执行器的
`TupleTableSlot` tts_values/tts_isnull 平行数组、JIT 元组解构也是"按访问模式重排布局"。
这项优化（PG 14, commit 941697c3c）与 Trick 11 组合，把 ProcArrayLock 从最热锁的位置
拉了下来。

## Trick 13：SetLatch 与 self-pipe——信号安全的进程唤醒

**它解决什么问题。** 进程要同时等待"套接字可读**或**别的进程叫我"。信号处理函数里几乎
什么都不能安全地做（async-signal-safety），而 `select/poll` 又只能等 fd——信号打断
`select` 存在经典的丢失唤醒竞态：信号在"检查条件"与"进入 select"之间到达就永久睡死。

**朴素做法及其缺陷。** `pg_usleep` 轮询：延迟与功耗二选一。或依赖 `select` 被 EINTR
打断：信号若恰在进入 select 前送达，进程带着"已有事件"睡进 select，直到下一次超时——
丢失唤醒。

**trick 的做法。** Latch 抽象：等待方把"自己可能在睡"的状态先写出去，唤醒方
`SetLatch`（`src/backend/storage/ipc/latch.c:290`，节选 :298–313）：

```c
    /*
     * The memory barrier has to be placed here to ensure that any flag
     * variables possibly changed by this process have been flushed to main
     * memory, before we check/set is_set.
     */
    pg_memory_barrier();

    /* Quick exit if already set */
    if (latch->is_set)
        return;

    latch->is_set = true;

    pg_memory_barrier();
    if (!latch->maybe_sleeping)
        return;
```

若对方可能在睡，则向其发 SIGURG；信号处理函数只做一件 async-signal-safe 的事：向
**self-pipe** 写一个字节（或在 Linux/macOS 上直接用 signalfd/kqueue 的 EVFILT_SIGNAL 收编
信号，见 waiteventset.c）。于是"信号"被转换成"fd 可读"，与套接字事件统一进同一个
epoll/kqueue 等待集合；等待方醒来后排干管道、重查 `is_set`。

**为什么是对的。** 丢失唤醒的反证：等待方的顺序是"置 maybe_sleeping → 屏障 → 检查
is_set → 睡"（latch.c:384 附近注释），唤醒方是"置 is_set → 屏障 → 检查 maybe_sleeping"。
两条"写A→读B"路径被屏障钉序后，与 Trick 8/10 相同的 Dekker 论证给出：不可能同时发生
"唤醒方没看见 maybe_sleeping"且"等待方没看见 is_set"。即使二者都看见了（信号照发、
等待方也自己发现了 is_set），后果只是一次多余的管道字节——self-pipe 的写入是幂等的
（排干即可），信号合并也无妨，因为唤醒语义是电平（is_set）而非边沿（信号本身）。
信号处理函数只执行 write(2)——POSIX 明列的 async-signal-safe 函数——绕开了
"handler 里不能碰锁/malloc"的全部雷区。

**代价与适用边界。** 每次唤醒至少一次系统调用；latch 是电平语义，使用者必须遵守
"循环底部等待、醒后重查条件"的纪律（latch.c:333 起注释强调）；多路事件下存在惊群式
冗余唤醒。self-pipe 每进程消耗两个 fd（有 signalfd/kqueue 的平台已免除）。

**在别处的复用。** 整个后台进程体系（walwriter、bgwriter、logical worker）的主循环都建立
在 `WaitLatch` 上；`ConditionVariable` 在 latch 之上又叠了一层"先入队再检查"的同款
两阶段协议；并行查询的 shm_mq 用 latch 做生产者-消费者唤醒。

---

# 三、内存

## Trick 14：MemoryContext 区域回收与 per-tuple 重置

**它解决什么问题。** C 没有 GC。查询执行会产生海量生命周期各异的小分配，逐个 `free` 既
容易漏（长命进程的泄漏是致命的）又慢；出错路径（elog 长跳转）更是不可能人工清理。

**朴素做法及其缺陷。** malloc/free 配对纪律 + 错误路径手工清理。在有 `PG_TRY`/longjmp 的
代码库里，任何一处 ERROR 都可能跳过 free；review 无法穷举所有抛错点。引用计数或全局 GC
则与 C 生态和性能目标不合。

**trick 的做法。** 分配挂靠在层级化的 MemoryContext 上，回收按**区域**整体进行：
`MemoryContextReset` 一次归还全部 chunk（`src/backend/utils/mmgr/aset.c:23` 注释：
"blocks ... given back at once on AllocSetReset()"）。生命周期绑定到语义边界：
TopMemoryContext（进程）、TopTransactionContext（事务）、per-query、per-tuple。执行器
对"每行临时垃圾"的处理是教科书式用法（`src/include/executor/executor.h:659–675`）：

```c
#define ResetExprContext(econtext) \
    MemoryContextReset((econtext)->ecxt_per_tuple_memory)
...
#define ResetPerTupleExprContext(estate) \
    ResetExprContext(GetPerTupleExprContext(estate))
```

每处理一行前重置 per-tuple context，行内表达式求值随便分配、从不释放。

**为什么是对的。** 正确性论证是**生命周期包含关系的形式化**：只要遵守"对象分配在生命周期
≥ 自身的 context 里"这一条纪律，那么 context 销毁时其中不存在仍被需要的对象——无泄漏由
"context 必在语义边界被销毁"保证（事务结束销毁 TopTransactionContext 是无条件的，连
错误路径也经过 AbortTransaction 走到）；无悬垂由分配纪律保证。错误恢复因此简化为
"回到上层边界，把下层 context 整体销毁"，与 longjmp 天然契合。性能上，重置比逐 chunk
free 快得多（aset 保留首块内存直接复用），per-tuple 分配退化为指针递增。

**代价与适用边界。** 纪律仍靠人：跨行存活的对象误放 per-tuple context 就是悬垂（典型 bug
形态）；整体回收意味着峰值内存 ≥ 精确释放方案；不适合生命周期交错、无法层级化的对象
（此时代码只能退回手工 `pfree`，MemoryContext 也支持）。

**在别处的复用。** 全库皆是：relcache 每条目一个 context、plancache、
`MessageContext`（每条前端消息）、autovacuum 每表一个……"区域 + 语义边界回收"也正是
Apache APR pools、Arena allocator 的思想；PostgreSQL 是把它与错误处理（elog/PG_TRY）
深度耦合的最完整工业实现之一。详见第 11 章 §内存管理。

## Trick 15：Chunk header 的 4-bit 方法 ID——free(p) 如何知道 p 属于谁

**它解决什么问题。** `pfree(p)`/`repalloc(p)` 只拿到裸指针，但 chunk 可能来自 aset、
generation、slab、bump 等不同分配器，各自的元数据布局不同。必须从指针本身反查"你是谁家的"。

**朴素做法及其缺陷。** 老实现：每个 chunk 头存 8 字节的 `MemoryContext` 反向指针。每个
小分配都背 8 字节税（分配 8 字节实付 16+）；且 `pfree` 要先解引用 context 指针再取
methods 表，两跳。

**trick 的做法。** chunk 前只放一个 `uint64 hdrmask`，用**位手术**塞进四样东西
（`src/include/utils/memutils_memorychunk.h:24–37`）：

```
 * 1.   4-bits to indicate the MemoryContextMethodID as defined by
 *      MEMORY_CONTEXT_METHODID_MASK
 * 2.   1-bit to denote an "external" chunk (see below)
 * 3.   30-bits reserved for the MemoryContext to use for anything it
 *      requires.  Most MemoryContexts likely want to store the size of the
 *      chunk here.
 * 4.   30-bits for the number of bytes that must be subtracted from the chunk
 *      to obtain the address of the block that the chunk is stored on.
```

注意 4+1+30+30=65 > 64：第 3 段最高位与第 4 段最低位**共享同一个 bit**——因为块偏移必然
MAXALIGN 对齐，其最低位恒为 0，可以借出（memutils_memorychunk.h:38–44 注释明言）。
`pfree` 的分派（`src/backend/utils/mmgr/mcxt.c:205–214`）：

```c
#define MCXT_METHOD(pointer, method) \
    mcxt_methods[GetMemoryChunkMethodID(pointer)].method

static inline MemoryContextMethodID
GetMemoryChunkMethodID(const void *pointer)
{
    uint64      header;
    ...
    header = *((const uint64 *) ((const char *) pointer - sizeof(uint64)));
    return (MemoryContextMethodID) (header & MEMORY_CONTEXT_METHODID_MASK);
}
```

4 bit ID 直接索引静态虚表 `mcxt_methods[]`（mcxt.c:64）。

**为什么是对的。** 每个分配器写 chunk 头时都必须把自己的 ID 编进低 4 位（由
`MemoryChunkSetHdrMask` 统一封装），于是"指针 − 8 字节处低 4 位"是所有分配器共同维护的
契约，读取无需知道来源。65 bit 塞 64 bit 的合法性：块偏移是两个 MAXALIGN 地址之差，
必为 MAXALIGN（≥8）的倍数，最低位数学上恒 0，读取时掩掉即可无损重建
（memutils_memorychunk.h:151–152 `HdrMaskBlockOffset` 掩最低位）。大分配（30 bit 装不下
尺寸）设 external 位逃逸到"块头存全量元数据"的旧方案——快路径优化 + 慢路径兜底的标准
分层。ID 表还故意把少数编码留给"永远不该出现"的哨兵（`mcxt_methods` 里的 BOGUS 条目），
使野指针大概率撞上明确报错而非静默损坏。

**代价与适用边界。** 至多 16 种分配器（当前用 4 种 + 保留位）；30 bit 限制单 chunk 相对
块首偏移 < 1 GB；对野指针的防御是概率性的（低 4 位恰好合法就误分派——Valgrind 集成
弥补）。头从 8 字节起步看似没省，但 aset 借第 3 段 30 bit 存了尺寸，省掉了旧头里的
另一个字段，小 chunk 头净省 8 字节。

**在别处的复用。** ItemId 的 15+2+15（Trick 1）、LWLock state（Trick 9）、buffer
descriptor state、`t_infomask` 都是同族"位打包 + 对齐借位"；利用对齐低位藏 tag 的手法
与 nbtree ItemPointer 里藏 posting 信息、指针 tagging（如 Lisp 实现）一脉相承。

---

# 四、磁盘与 I/O

## Trick 16：FPW——检查点后"第一次修改才整页"

**它解决什么问题。** 8 KB 页写入磁盘不是原子的（扇区 512B/4K），崩溃可能留下"前半新后半旧"
的撕裂页；而 WAL 记录多是逻辑增量（"在偏移 x 插入元组"），基页若已撕裂，重放无从谈起。

**朴素做法及其缺陷。** 方案 A：每条 WAL 记录都带整页镜像——WAL 体积膨胀数十倍。
方案 B：依赖文件系统/硬件原子写（如双写缓冲、COW 文件系统）——可移植性差，PostgreSQL
要在任意 POSIX 文件系统上正确。

**trick 的做法。** 只有**检查点之后第一次**修改某页时，WAL 记录才携带整页镜像（FPI）；
判据是"页面 LSN ≤ 最近检查点的 REDO 点"（`src/backend/access/transam/xloginsert.c:686–699`）：

```c
        else
        {
            /*
             * We assume page LSN is first data on *every* page that can be
             * passed to XLogInsert, whether it has the standard page layout
             * or not.
             */
            XLogRecPtr  page_lsn = PageGetLSN(regbuf->page);

            needs_backup = (page_lsn <= RedoRecPtr);
            if (!needs_backup)
            {
                if (!XLogRecPtrIsValid(*fpw_lsn) || page_lsn < *fpw_lsn)
                    *fpw_lsn = page_lsn;
            }
        }
```

重放时遇到 FPI 直接整页覆盖，无视页面现状。

**为什么是对的。** 归纳论证：崩溃恢复从最近检查点的 REDO 点开始。对任何在恢复中会被
重放的页 P——(1) 若 P 在检查点后被修改过，则重放序列中它的**第一条**记录必是 FPI
（因为第一次修改时页 LSN 还 ≤ RedoRecPtr），整页覆盖使 P 进入确定状态，此后的增量记录
作用在确定基础上；(2) 若 P 在检查点后从未被修改，则它不可能撕裂——撕裂只能发生在写出
期间，而检查点完成语义保证检查点前的脏页已完整落盘。页 LSN 本身可能位于撕裂页中？无妨：
LSN 判断只影响**是否多拍一张 FPI**（保守方向安全），而恢复时 FPI 覆盖根本不读旧页。
`RedoRecPtr` 在插入时可能是陈旧值，故拿到 WAL 插入锁后还有二次核对（`fpw_lsn` 回传
再检查），保证不漏拍。

**代价与适用边界。** 检查点后的首轮写负载 WAL 猛增（checkpoint 后吞吐锯齿是 PostgreSQL
的特征曲线）；`full_page_writes=off` 只在硬件保证原子写时合法。FPI 同时被复用为撕裂之外
的用途——见 hint bits 的 `XLOG_FPI_FOR_HINT`（Trick 6）。

**在别处的复用。** pg_rewind 依赖 FPI 语义收集改动页；备库回放、PITR 与在线备份
（backup 期间强制 FPW）都建立在"FPI = 页的重置点"之上。对照：InnoDB 用双写缓冲解同一
问题——两份顺序写换 WAL 里不带整页；PostgreSQL 用 WAL 内嵌换少一次独立刷盘。详见第 09 章。

## Trick 17：WAL 段复用不清零 + prev-link/CRC 判定终点

**它解决什么问题。** 两个问题共用一个解：①不断创建/删除 16 MB 段文件浪费（元数据操作 +
重新分配磁盘块 + 每次都写 16 MB 零填充）；②崩溃恢复时如何知道 WAL 读到哪里算"末尾"——
WAL 没有也不能有"末尾指针"（维护它本身就要一次额外同步写）。

**朴素做法及其缺陷。** 每段用完删除、需要时新建并清零：分配开销大且清零本身是 16 MB 写。
用一个持久化的"当前写到哪"指针：每条记录提交都要更新该指针并 fsync，等于 WAL 写两遍。

**trick 的做法。** 旧段直接**重命名**成未来的段名继续用，内容不清零
（`RemoveXlogFile`，`src/backend/access/transam/xlog.c:4059`，经
`InstallXLogFileSegment`，xlog.c:3613 完成改名；`wal_recycle` 开关见 xlog.c:135）。
段里因此充满上一轮的**合法旧 WAL 记录**。终点判定不靠任何指针，靠记录自证失效
（`src/backend/access/transam/xlogreader.c:1175–1188, 1205–1218`）：

```c
        /*
         * Record's prev-link should exactly match our previous location. This
         * check guards against torn WAL pages where a stale but valid-looking
         * WAL record starts on a sector boundary.
         */
        if (record->xl_prev != PrevRecPtr)
        { ... return false; }
...
    /* in ValidXLogRecord: */
    COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
    COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
    if (!EQ_CRC32C(record->xl_crc, crc))
```

三重判据：页头的 `xlp_pageaddr` 必须等于该页应有的 LSN（旧段的页自带旧地址，改名不改
内容，一读即穿帮）；记录的 `xl_prev` 必须精确等于前一条记录的地址；CRC 必须匹配。
任一不符即认定"到头了"。

**为什么是对的。** 需要论证的是**不早停也不晚停**。不晚停（不误读旧记录为新）：旧记录
是为旧 LSN 位置生成的，其所在页的 `xlp_pageaddr` 是旧地址；即使攻击性地假设某旧页恰好
落在地址相同的位置（不可能，因为段被改名到了新地址区间），还有 `xl_prev` 链式校验——
一条旧记录的 prev 指向旧位置，与恢复扫描维护的 `PrevRecPtr` 精确相等的概率需要旧新两轮
写入在字节级重合，最后 CRC 再兜底。特别地，引文注释点出最阴险的场景：撕裂页上扇区边界处
残留的旧记录"看起来完全合法"——唯有 prev-link 的**精确匹配**（而非仅"小于自身"）能
识别它。不早停：真正的最后一条完整记录必然通过全部校验（它就是按这些规则写出的）。
于是"WAL 末尾"成为一个**内在可判定**属性，无需外部元数据，也就无需为维护元数据付出
额外的同步写。

**代价与适用边界。** 判定是概率性的（CRC32 碰撞 + 字节级巧合），工程上视为不可能但
理论上非零；被复用的段在归档/传输时含垃圾尾巴（压缩可缓解）；`pg_waldump` 等工具必须
实现同样的判界逻辑。恢复中一旦某条记录损坏，其后的合法记录也不可达——这是"链式自证"
的固有属性，也正是语义所要求的（prev 断裂即末尾）。

**在别处的复用。** 两阶段提交状态文件、复制槽状态文件都用"CRC 自证完整性"；
"重命名即原子安装"手法（先写临时文件再 rename）遍布 xlog.c、slot.c、
`write_relcache_init_file`。

## Trick 18：Ring buffer——大表扫描不污染缓冲池

**它解决什么问题。** 一次大于 shared_buffers 的顺序扫描，若按常规策略使用缓冲池，会把
全部热数据逐出，换进一堆只用一次的页——扫完后整个系统冷启动。

**朴素做法及其缺陷。** 依赖 LRU/clock 自身：clock 的 usage_count 机制对"快速扫过、每页
恰好命中一次"的访问模式没有抵抗力，每页都会被装入并把别人挤走。绕过缓冲池直接读
（direct I/O）则失去与并发写者的一致性协调（别人改了页你读不到）。

**trick 的做法。** 为批量操作分配一个小小的**私有环形缓冲区**，本策略的所有读取在环内
自我循环复用（`src/backend/storage/buffer/freelist.c:70–100` 定义
`BufferAccessStrategyData`，:98 `GetBufferFromRing`）。环大小按操作类型定
（`GetAccessStrategy`，freelist.c:426，BAS_BULKREAD 基准 256 KB，freelist.c:459）：

```c
        case BAS_BULKREAD:
            ...
                ring_size_kb = 256;
```

`StrategyGetBuffer` 优先从环里取（freelist.c:198）；环内的页若被别人拉高了引用，
`GetBufferFromRing` 会放弃复用它（等于把它"让渡"给主缓冲池）。VACUUM、COPY、
seqscan 大表、`CREATE TABLE AS` 各有专属环型号（BAS_VACUUM 等）。

**为什么是对的。** 正确性零风险：环只是**受害者选择策略**的改变，页的内容管理、锁、pin、
脏页写出全部仍走共享缓冲池的正规通道，与并发访问者的一致性不受任何影响——这是它比
direct I/O 高明之处。性能论证：顺序扫描的复用距离 = 表大小，任何小于表的缓存都注定
零命中（LRU 的经典反例），所以给它多于"流水线深度"的缓存是纯浪费；256 KB 恰好覆盖
"上层还 pin 着前几页 + 同步扫描队友的落差"（freelist.c:433 注释提示与
SYNC_SCAN_REPORT_INTERVAL 联动）。而 BULKREAD 遇到脏页时不自己刷盘而是
`StrategyRejectBuffer` 换一个（freelist.c:584 附近注释）——避免让读操作背上写 WAL 的
延迟。

**代价与适用边界。** 反复扫同一张中等大小表的工作负载会被误伤（表本可整个驻留缓冲池，
却被环限制成每次都读盘）——所以判定阈值是"表 > shared_buffers/4"才启用 BULKREAD 环；
VACUUM 的环太小则会被 WAL flush 频率反噬（PG 17 起 `vacuum_buffer_usage_limit` 可调）。

**在别处的复用。** 同一策略对象被 COPY（BAS_BULKWRITE）、VACUUM（BAS_VACUUM）、
ANALYZE 共用；操作系统页缓存的 `posix_fadvise(DONTNEED)`、数据库界的
"scan-resistant buffer pool"（如 Oracle 的 CACHE/NOCACHE、MySQL 的 midpoint insertion）
都是同题异解——PostgreSQL 的版本胜在完全局部化：不改全局替换算法，只给特定访问者
戴上小环。

## Trick 19：Backend fsync 请求转发给 checkpointer

**它解决什么问题。** 检查点必须保证"此前所有脏页已持久化"。脏页由任意 backend/bgwriter
写出（write），但 write ≠ 持久化，必须有人对每个被写过的文件 fsync；谁来记住
"哪些文件写过"、谁来执行 fsync？

**朴素做法及其缺陷。** 每个 backend 写出页后立刻 `fsync`：把最贵的同步 I/O 放进前台
查询路径，一次驱逐脏页的查询延迟暴涨；同一文件被多个进程反复 fsync，重复劳动。
或者 O_SYNC 打开数据文件：每次 write 都同步，更慢。

**trick 的做法。** backend 写出页后只把 (文件, 类型) 的**待办事项**发给 checkpointer，
fsync 延迟到检查点时批量去重执行
（`RegisterSyncRequest`，`src/backend/storage/sync/sync.c:581`，节选 :594–606）：

```c
    if (pendingOps != NULL)
    {
        /* standalone backend or startup process: fsync state is local */
        RememberSyncRequest(ftag, type);
        return true;
    }

    for (;;)
    {
        ...
        ret = ForwardSyncRequest(ftag, type);
```

`ForwardSyncRequest`（`src/backend/postmaster/checkpointer.c:1213`）把请求塞进共享内存
队列；队列满时先尝试**原地去重压缩**（`CompactCheckpointerRequestQueue`，
checkpointer.c:1268——同一文件的重复请求合并），仍满则唤醒 checkpointer 吸收。

**为什么是对的。** 正确性的核心是**顺序**：请求在 write 之后、检查点完成之前必达
checkpointer——`RegisterSyncRequest` 在队列满时循环重试（sync.c:606 起 for 循环 +
`WaitLatch` 10ms 退避），**绝不静默丢弃**；而 checkpointer 在决定检查点 REDO 点之后、
写 checkpoint 记录之前处理完全部已吸收请求并逐文件 fsync。于是"检查点声称完成 ⇒ 此前
写出的每个文件都被 fsync 过"成立。去重的合法性：fsync 是幂等的文件级操作，N 个对同一
文件的请求合并为 1 次执行语义不变——这正是批量摊销的价值：每秒上千次页写出坍缩为检查点
时每文件一次 fsync。错误处理侧有著名的 fsync-gate 加固：fsync 失败即 PANIC（不可重试，
因为内核可能已丢弃脏页标记）。

**代价与适用边界。** 检查点承担全部 fsync 尖峰（由 checkpoint_completion_target 平滑）；
共享队列是定长的，极端写风暴下 backend 会在转发上排队；startup 进程和单用户模式没有
checkpointer，走本地 `pendingOps` 路径（引文首段）——同一 API 两种实现的整洁降级。

**在别处的复用。** 同型的"前台只记账、后台批量执行"：统计系统（pgstat 快照/刷写）、
WAL 归档（.ready 文件 + archiver）、`RequestCheckpoint` 本身。bgwriter 与 backend 驱逐
共享同一 `RegisterSyncRequest` 通道，保证"谁写出都不漏记"。

---

# 五、协议与兼容

## Trick 20：Magic block——加载时 ABI 自检

**它解决什么问题。** 扩展 .so 是针对某个 PostgreSQL 版本、某组编译选项（对齐、块大小、
函数调用接口版本）编译的。装错版本的 .so 不会链接失败——符号都在——但结构体布局不同，
运行起来是静默内存损坏。

**朴素做法及其缺陷。** 靠文件名/目录约定（如装在 `lib/postgresql/16/`）：约定挡不住
手工拷贝。靠 SONAME/符号版本：无法覆盖"同版本不同编译选项"（`--with-blocksize=32` 的
服务器加载默认块大小编译的扩展照样炸）。

**trick 的做法。** 每个扩展模块**编译期**把关键 ABI 参数烙进一个静态结构，通过约定名字的
导出函数暴露（`src/include/fmgr.h:503–527`）：

```c
#define PG_MODULE_MAGIC_DATA(...) \
{ \
    .len = sizeof(Pg_magic_struct), \
    .version = PG_VERSION_NUM / 100, \
    .funcmaxargs = FUNC_MAX_ARGS, \
    .indexmaxkeys = INDEX_MAX_KEYS, \
    .namedatalen = NAMEDATALEN, \
    .float8byval = FLOAT8PASSBYVAL, \
    .abi_extra = FMGR_ABI_EXTRA, \
    __VA_ARGS__ \
}
```

`PG_MODULE_MAGIC` 宏（fmgr.h:522）展开为函数 `Pg_magic_func`，返回指向该静态数据的指针。
服务器 `dlopen` 后第一件事就是找这个函数并逐字段核对
（`src/backend/utils/fmgr/dfmgr.c:257–270`）：

```c
        magic_func = (PGModuleMagicFunction)
            dlsym(file_scanner->handle, PG_MAGIC_FUNCTION_NAME_STRING);
        if (magic_func)
        {
            const Pg_magic_struct *magic_data_ptr = (*magic_func) ();

            if (magic_data_ptr->len != sizeof(Pg_magic_struct) || ...)
```

不符即 `incompatible_module_error`（dfmgr.c:72）拒绝加载；没有 magic 函数直接拒载。

**为什么是对的。** 关键在于**数据由被检方的编译器在被检方的编译环境里生成**：
`PG_VERSION_NUM`、`NAMEDATALEN` 等值在扩展编译时被常量折叠进 .so 的只读数据段，任何
"编译环境与运行环境不一致"都会体现在字段差异上，无法伪造也无法遗漏（不写 PG_MODULE_MAGIC
就根本装不上）。先比 `len` 再比内容的次序也有讲究：结构体本身在版本间增长过
（如 `abi_extra` 是后加的），len 不等就不能按新布局解读旧数据——自描述长度是跨版本比较
自身的引导。`abi_extra`（默认 "PostgreSQL"）给 fork 发行版留了标记位：EDB/Greenplum 等
改 ABI 的分支可写入自己的串，防止与社区版二进制互装。

**代价与适用边界。** 只校验声明过的少数参数，结构体布局差异若不反映在这些参数上仍漏网
（靠 `PG_VERSION_NUM/100` 的 major 版本档拦截大部分）；对非 C 扩展（PL 写的函数）不适用
（那是另一套 `CREATE EXTENSION` 版本机制的职责）。

**在别处的复用。** 控制文件 `pg_control` 的 `CATALOG_VERSION_NO`/`PG_CONTROL_VERSION`
是同一思想对数据目录的应用；`NodeTag` 校验（`IsA(routine, TableAmRoutine)`，Trick 25）
是运行时的轻量同款；libpq 协议版本协商是网络维度的对应物。

## Trick 21：kwlist.h X-macro + 编译期完美哈希

**它解决什么问题。** 词法分析器对每个标识符都要问"是不是关键字"（500 多个），这是解析
路径上最热的查询之一；同时关键字表被**多个消费者**以不同形态需要：语法分析要 token 号、
psql 补全要名字、ecpg 要自己的表、文档要生成对照表。

**朴素做法及其缺陷。** 一份手写的排序数组 + 二分查找：O(log n) 次字符串比较（≈9 次
strcmp）；多个消费者各自维护副本，加一个关键字要改 N 处，漂移是时间问题。运行时构建
哈希表：启动成本 + 表要放共享内存或每进程重建，且哈希冲突链使最坏情况不可控。

**trick 的做法。** 两层。第一层 X-macro：`src/include/parser/kwlist.h` 只有一列
`PG_KEYWORD(...)` 调用，**故意不定义**该宏（kwlist.h:7 注释），由包含者按需定义
（kwlist.h:28）：

```c
PG_KEYWORD("abort", ABORT_P, UNRESERVED_KEYWORD, BARE_LABEL)
```

gram.y 里定义成产出 %token，kwlookup 里产出名字池，psql 里产出补全表——一份事实源,
N 种展开。第二层：构建期 Perl 脚本对关键字集合生成**最小完美哈希函数**
（`src/tools/gen_keywordlist.pl:165` 调 `PerfectHash::generate_hash_function`，算法在
`src/tools/PerfectHash.pm`——两个乘法散列查表相加的 CHM 变体）。查找退化为"一次哈希 +
一次串比较"（`src/common/kwlookup.c:52–70`）：

```c
    if (len > keywords->max_kw_len)
        return -1;              /* 超长先拒，省掉哈希 */
    /*
     * Compute the hash function. ... Since it's a perfect hash, we need only
     * match to the specific keyword it identifies.
     */
    h = keywords->hash(str, len);

    /* An out-of-range result implies no match */
    if (h < 0 || h >= keywords->num_keywords)
        return -1;
```

后接**恰好一次**逐字符比较（kwlookup.c:70–83，顺带做 ASCII-only 小写折叠，注释点名
不用 tolower 以避开土耳其语区陷阱）。

**为什么是对的。** 完美哈希的定义即"对已知集合无冲突"，因此候选槽唯一，单次比较即可
确认或否决——最坏情况从二分的 9 次比较降为 1 次哈希 + 1 次比较，且无冲突链、无最坏
退化。构建期生成的合法性：关键字集合在编译期闭合（这正是完美哈希的适用前提），若集合
变化，构建系统重新生成函数，正确性由"生成器验证哈希对全集无碰撞"保证（PerfectHash.pm
生成失败会换随机种子重试）。X-macro 层保证 token 号、名字、分类三者永远同步——错位
（名字表第 i 项对不上 token 第 i 项）这类 bug 被构造性消灭。名字池用"一大块字符串 +
偏移数组"而非指针数组（`ScanKeywordList` 结构），省重定位且只读段可跨进程共享。

**代价与适用边界。** 集合必须编译期已知——用户自定义词无缘；生成的哈希函数对**集合外**
输入返回任意值，所以 out-of-range 检查与最终串比较**不可省略**（引文两道闸）；构建链
引入 Perl 依赖。

**在别处的复用。** 同一 PerfectHash.pm 还生成 Unicode 规范化查找表
（`generate-unicode_norm_table.pl`）；X-macro 模式再现于
`src/include/parser/kwlist.h` 的姊妹们——`pl_unreserved_kwlist.h`、ecpg 的
`c_kwlist.h`，以及 `errcodes.txt`→多目标生成、`wait_event_names.txt`→文档+代码双生成。

## Trick 22：errcontext 回调栈——错误信息的"逆向注入"

**它解决什么问题。** 底层代码抛错时（如 `date` 类型解析失败），用户需要知道**高层语境**：
是 COPY 的第几行？逻辑复制哪个表？PL/pgSQL 哪个函数第几行？但底层函数根本不知道也不该
知道这些。

**朴素做法及其缺陷。** 把上下文一路作参数传下去：每层 API 都要为"可能出错时的描述信息"
增加参数，污染全部签名。或在每个调用点 `PG_TRY/PG_CATCH` 捕获后重抛加工：TRY 块有
成本（sigsetjmp），且重抛丢失/复制错误状态的坑极多。

**trick 的做法。** 高层在进入某段工作前，把"如何描述我"注册成回调压入线程局部（进程局部）
链表 `error_context_stack`（`src/include/utils/elog.h:311–318`）：

```c
typedef struct ErrorContextCallback
{
    struct ErrorContextCallback *previous;
    void        (*callback) (void *arg);
    void       *arg;
} ErrorContextCallback;

extern PGDLLIMPORT ErrorContextCallback *error_context_stack;
```

节点是**栈上分配**的（局部变量），退出时把 `error_context_stack` 指回 `previous` 即弹栈。
真正抛错时，`errfinish` 才回头逐层调用（`src/backend/utils/error/elog.c:519–522`）：

```c
    for (econtext = error_context_stack;
         econtext != NULL;
         econtext = econtext->previous)
        econtext->callback(econtext->arg);
```

每个回调用 `errcontext()` 追加一行，如 `COPY t, line 42, column d: "foo"`。

**为什么是对的。** 成本模型是核心论证：错误是异常路径，正常路径上下文注册只花两三条
指令（写两个指针字段 + 改一个全局指针），**描述信息的格式化被推迟到确实出错时**——
这是惰性求值对错误处理的应用。栈上分配节点为何安全？因为 C 的调用栈纪律保证：注册者
的栈帧存活期 ⊇ 其中所有下层调用的执行期，而回调只可能在下层调用抛错时（栈帧仍活着）
被执行；elog 的 longjmp 飞越这些栈帧之前，回调已经跑完。longjmp 后栈里的节点作废，
但 `PG_TRY`/事务中止路径会把 `error_context_stack` 恢复为进入时的保存值
（elog.h:391–411 的 PG_TRY 宏组正是保存/恢复它），不会留悬垂。递归错误（回调自己抛错）
由 errstart 的递归深度检查斩断（elog.c:513 注释）。

**代价与适用边界。** 回调在**错误发生时刻**执行，能看到的必须是仍然有效的数据——arg
指向的对象若在抛错前已被释放就是悬垂（所以惯例是指向调用者栈帧或长命结构）；回调里
不能做可能再抛错的重活。全局链表意味着它按"动态作用域"工作，异步事件（信号处理里
的 elog）看到的栈可能语义错位——所以信号处理器里几乎不允许 elog。

**在别处的复用。** COPY（`CopyFromErrorCallback`）、PL/pgSQL 语句级回调、逻辑复制 apply
worker、并行 worker 把错误传回 leader 时也重放 context 行。同一"进程全局链表头 +
栈上节点"的手法还实现了资源清理（`before_shmem_exit` 队列）与 `PG_ENSURE_ERROR_CLEANUP`。

---

# 六、编译器

## Trick 23：Computed goto——解释器直接线索化

**它解决什么问题。** 表达式解释器 `ExecInterpExpr` 是 OLTP 的最内层循环之一：取下一个
opcode、跳到对应实现、执行、再取下一个。分派本身的 CPU 成本（分支预测失败）可占解释
执行的一小半。

**朴素做法及其缺陷。** `while (...) switch (op->opcode)`：编译器生成**单一**间接跳转
（跳转表），CPU 的间接分支预测器只有一个预测槽位对付所有 opcode 序列——不同 opcode
交错出现时预测命中率极低，每次失败十几个周期。

**trick 的做法。** 用 GCC/Clang 的标签地址扩展（`&&label` + `goto *ptr`），让**每个**
opcode 实现的结尾各自内联一个"取下一跳转"的间接 goto
（`src/backend/executor/execExprInterp.c:89–121`）：

```c
#ifdef HAVE_COMPUTED_GOTO
#define EEO_USE_COMPUTED_GOTO
#endif
...
#if defined(EEO_USE_COMPUTED_GOTO)
...
#define EEO_CASE(name)      CASE_##name:
#define EEO_DISPATCH()      goto *((void *) op->opcode)
#define EEO_OPCODE(opcode)  ((intptr_t) dispatch_table[opcode])
#else                           /* !EEO_USE_COMPUTED_GOTO */
#define EEO_SWITCH()        starteval: switch ((ExprEvalOp) op->opcode)
```

更进一步：表达式初始化时把步骤数组里的 opcode **就地替换**为标签地址
（`ExecReadyInterpretedExpr` 用 `EEO_OPCODE` 查 `dispatch_table`，execExprInterp.c:114），
运行时分派连查表都省了，直接 `goto *op->opcode`。因此还需要
`reverse_dispatch_table`（execExprInterp.c:117）在调试/EXPLAIN 时把地址翻译回枚举值。

**为什么是对的。** 语义等价显然（每个跳转都落到与 switch case 相同的代码）；性能论证
来自分支预测微架构：N 个分布在各 case 尾部的间接跳转拥有 N 个独立的预测器条目，每个
条目学习的是"opcode X 之后常跟什么"的**条件分布**而非全局分布——对表达式这种高度重复
的 opcode 序列，命中率大幅提升（解释器领域实测 15–40% 加速的经典技术，源自 Forth 的
direct threading）。正确性上的微妙点被工程化处理：标签地址是**函数内**局部量，所以
`dispatch_table` 用"哨兵调用"导出（首次以 state==NULL 调用 `ExecInterpExpr` 仅为取表，
execExprInterp.c:113 注释）；不支持扩展的编译器优雅退化为 switch（宏对同一份 case 代码
双重展开）。

**代价与适用边界。** 非标准 C（GNU 扩展），必须保留 switch 后备；把 opcode 换成指针后
每步 8 字节、调试可读性下降（需反查表）；JIT（llvmjit_expr）出现后，解释器分派开销的
上限被进一步绕过——computed goto 是"没有 JIT 或不值得 JIT 时"的最优解释档。

**在别处的复用。** 本仓库中该技术专用于表达式解释器；同族的"分派表 = 函数指针/标签数组"
思想以温和形式出现在 `mcxt_methods[]`（Trick 15）与各 AM 虚表（Trick 25）。CPython 3.11+、
V8 的字节码解释器采用同一技术，可互为参照。

## Trick 24：likely/unlikely 与 StaticAssert——给编译器递情报

**它解决什么问题。** 两类信息编译器自己推不出来：①哪个分支是热路径（错误检查几乎永不
成立，但编译器默认五五开，可能把冷路径排进热指令流）；②代码正确性所依赖的平台/布局
假设（结构体大小、枚举个数、位域不重叠），违背时希望**编译失败**而非运行时炸。

**朴素做法及其缺陷。** ①靠编译器启发式或 PGO：启发式常错，PGO 对发行版构建流程要求高。
②运行时 `Assert`：只有 debug 构建、只有跑到那行才查；靠注释提醒："when changing X,
also change Y" 的注释从不被工具执行。

**trick 的做法。** 分支概率注解（`src/include/c.h:492–497`）：

```c
#ifdef __GNUC__
#define likely(x)   __builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)   ((x) != 0)
#define unlikely(x) ((x) != 0)
#endif
```

典型用法如 `if (unlikely(!OidIsValid(...))) elog(ERROR, ...)`。编译期断言
（c.h:1067–1075，现代版直接落到 C11 `static_assert`）：

```c
#define StaticAssertDecl(condition, errmessage) \
    static_assert(condition, errmessage)

#define StaticAssertStmt(condition, errmessage) \
    do { static_assert(condition, errmessage); } while(0)
```

实战样例即 Trick 9 引过的 lwlock.c:117——位域编码的互斥性被钉进编译期：

```c
StaticAssertDecl((LW_VAL_EXCLUSIVE & LW_FLAG_MASK) == 0,
                 "LW_VAL_EXCLUSIVE and LW_FLAG_MASK overlap");
```

同族还有 `pg_attribute_always_inline`/`pg_noinline`（强制热函数内联/把冷路径挪出函数体，
见 c.h 的 attribute 宏群）。

**为什么是对的。** `__builtin_expect` 影响的是基本块布局与静态预测提示：热路径变成
直落（fall-through）序列，冷路径（elog 调用及其参数准备）被搬到函数尾部甚至
`.text.unlikely` 段——I-cache 里塞满有用指令。语义严格不变（表达式值原样返回），
所以标错方向只是变慢、不会变错——这是它可以放心大规模使用的原因。StaticAssert 的价值
在于**把不变式从文档态升为构建门禁**：条件是编译期常量表达式，违背即编译错误，重构者
在第一时间、在自己机器上收到反馈，而不是在客户的大端机上收到崩溃报告。零运行时成本。

**代价与适用边界。** likely/unlikely 是程序员断言，负载变化后可能过时（谁会想到某系统里
"错误"路径成了常态？）；只该用于**极端**偏斜的分支（错误检查、Assert、一次性初始化），
五五开的分支标了反而有害。StaticAssert 只能表达编译期可算的条件，涉及运行时值的不变式
仍归 `Assert`。

**在别处的复用。** 全库高频：`palloc` 的尺寸检查、fastpath 锁判定、执行器逐行循环里的
NULL 检查都带 unlikely；StaticAssert 守卫着 `sizeof(ItemIdData)==4` 一类布局契约、
枚举与查找表的长度同步（如 `pgstat` 的 kind 表）。它们与 `pg_attribute_packed`、
`pg_attribute_aligned` 共同构成"以宏为界面的编译器方言隔离层"——c.h 把非标准扩展
封装成可降级的统一拼写，这本身也是个兼容性 trick。

## Trick 25：C 语言手工虚函数表（TableAmRoutine/FdwRoutine）

**它解决什么问题。** 存储引擎（表访问方法）、外部数据源（FDW）、索引 AM 需要可插拔：
核心代码按统一接口调用，具体实现来自运行时加载的扩展 .so。C 没有 interface/虚函数。

**朴素做法及其缺陷。** 巨型 switch（`switch (am_type) { case HEAP: ... }`）：新增实现要
改核心代码，扩展无从注入。散装函数指针参数：接口几十个回调，逐个传递不可维护，也无法
整体校验完备性。

**trick 的做法。** 接口 = 一个装满函数指针的 const 结构体，首字段是 NodeTag
（`src/include/access/tableam.h:321–326`）：

```c
typedef struct TableAmRoutine
{
    /* this must be set to T_TableAmRoutine */
    NodeTag     type;
    ...
    TableScanDesc (*scan_begin) (Relation rel, ...);        /* tableam.h:360 */
    void        (*tuple_insert) (Relation rel, ...);        /* tableam.h:546 */
```

实现方（heapam_handler.c 或扩展）定义一个静态常量实例填满所有槽，通过 handler 函数
（返回 `internal` 的 SQL 函数）交给核心；核心统一从
`GetTableAmRoutine()`（`src/backend/access/table/tableamapi.c:27`）取用并**逐槽校验**：

```c
    routine = (TableAmRoutine *) DatumGetPointer(datum);

    if (routine == NULL || !IsA(routine, TableAmRoutine))
        elog(ERROR, "table access method handler %u did not return a TableAmRoutine struct",
             amhandler);
    /* 之后是一长串 Assert(routine->scan_begin != NULL) 式的完备性检查 */
```

调用点是 `rel->rd_tableam->tuple_insert(...)` ——与 C++ 虚调用同构（对象携带 vtable 指针，
经它间接调用），但 vtable 是**手写、显式、每关系一个指针**。

**为什么是对的。** 这精确复刻了虚函数表的机器级实现（C++ 编译器生成的也就是"隐藏的
函数指针表 + 对象内表指针"），因此获得同等的多态能力；而**显式化**带来三点 C++ 版本
没有的好处：①NodeTag 首字段 + `IsA` 检查提供了跨编译单元、跨 .so 的廉价类型/ABI 校验
（handler 返回的东西哪怕是野指针，先撞 tag 检查，配合 Trick 20 的 magic block 构成
两级防线）；②加载时一次性验证所有必选回调非空（tableamapi.c 的 Assert 群），把
"实现不完整"从第一次调用时的段错误提前为创建时的明确报错；③静态 const 实例天然进
只读段、进程间共享、无构造顺序问题。间接调用的成本被刻意控制：`rd_tableam` 缓存在
relcache 条目里，取表一次、调用多次。

**代价与适用边界。** 没有编译器强制的签名检查（槽位赋了签名不符的函数只有警告级别
保护）；没有继承/默认方法——可选回调靠 NULL 检查约定（如 `scan_analyze_next_block`
可为 NULL）；接口演进即 ABI 断裂，每个大版本扩展都要重编（由 magic block 拦截）。

**在别处的复用。** 同一模式家族：`FdwRoutine`（foreign/fdwapi.h）、索引 AM 的
`IndexAmRoutine`（amapi.h）、`TupleTableSlotOps`（tuptable.h——执行器元组槽的多态）、
`MemoryContextMethods`（Trick 15，用 4-bit ID 代替对象内指针的变体）、
`CustomExecMethods`（自定义扫描）。第 15 章（扩展机制）与 T1（存储引擎设计空间）
从 API 设计角度展开了同一主题。

---

# 七、共同思想提炼

回看这 25 个 trick，反复出现的是六条设计母题：

**母题一：加一层间接（indirection buys freedom）。**
"计算机科学的一切问题都可以用加一层间接解决"在本仓库有最密集的示范：ItemId 让元组
可移动而引用稳定（Trick 1）；HOT redirect 让索引项不追版本（Trick 2）；MultiXactId 让
4 字节 xmax 容纳任意多锁持有者（Trick 7）；虚函数表让调用方与实现解耦（Trick 25）；
FSM/VM 作为主数据的旁路小地图（Trick 5）。间接层的共同代价是多一跳访问，共同收益是
**把"变"隔离在层内**。

**母题二：惰性——把工作推迟到不得不做、且可能永远不必做的时刻。**
hint bits 等第一次真正需要时才查 CLOG（Trick 6）；FPW 只在检查点后首次修改时拍照
（Trick 16）；errcontext 只在真出错时才格式化描述（Trick 22）；fsync 推迟到检查点批量
执行（Trick 19）；VACUUM 把删除的清理无限推迟到后台。惰性的正确性前提总是同一句话：
**推迟不改变可观测语义，且存在兜底时刻保证工作最终完成或确证不需要**。

**母题三：可再生状态不需要持久化协议。**
凡是"丢了可以重算、且重算结果恒同"的状态，就可以不写 WAL、不加失效协议、甚至容忍
撕裂：hint bits（Trick 6）、FSM/VM 的建议值与自愈（Trick 5）、relcache init file
（丢了重建）、`LP_DEAD` 索引提示。识别出哪些状态是可再生的、然后**拒绝为它们支付
持久化成本**，是 PostgreSQL 性能工程里回报最高的一类决策。反例衬托：一旦校验和让
hint 页"不可撕裂"，就得补交 FPI_FOR_HINT 的税——可再生性被打破，成本立刻回来。

**母题四：批量与摊销——把 N 次同步点合并为 1 次。**
组提交 leader 替全组清 xid（Trick 11）；fsync 请求去重后每文件一次（Trick 19）；
MemoryContext 整体重置代替逐 chunk free（Trick 14）；WAL 段整段复用代替逐段创建
（Trick 17）。摊销的合法性论证千篇一律但不可省略：**被合并的操作幂等或可交换，且合并
执行点仍在正确性要求的时序边界之内**。

**母题五：编译期/构建期换运行期。**
完美哈希在构建期消灭运行期冲突处理（Trick 21）；gen_node_support.pl 在构建期消灭手写
同步错误（Trick 3）；StaticAssert 把不变式检查移到编译期（Trick 24）；computed goto 的
标签地址在表达式编译期就替换进步骤数组（Trick 23）；magic block 把 ABI 参数在扩展编译期
固化（Trick 20）。共同前提：**信息在早期阶段已经闭合**（关键字集合、节点定义、位域布局
都是编译期常量）。

**母题六：为热路径设计，让冷路径买单（fast path / slow path 分离）。**
fastpath 锁让无冲突的弱锁绕开锁表，DDL 付出扫描全体 backend 的代价（Trick 8）；
sinval 的 hasMessages 让"没消息"零成本，代价是写者逐 backend 置旗（Trick 10）；
LWLock 单 CAS 快路径 + 复杂的排队慢路径（Trick 9）；chunk header 4-bit 快分派 +
external 慢路径（Trick 15）；ring buffer 保护常规负载、让批量扫描自我设限（Trick 18）。
方向选择的依据永远是频率的不对称性——**优化谁，取决于谁被执行得多，而不是谁更重要**。

这六条母题彼此复合：一个成熟的 trick 往往同时命中两三条（hint bits = 惰性 + 可再生 +
热路径；组提交 = 批量 + 热路径；完美哈希 = 构建期 + 热路径）。读新代码时，先问
"它在哪条母题上下注"，通常能最快抓住设计意图。

---

# 八、自测题（7 题带详解）

**Q1. 为什么 hint bits 可以不写 WAL，而 `PD_ALL_VISIBLE` 页标志位的设置却必须写 WAL
（或随其他记录连带保护）？两者不都是"派生信息"吗？**

<details><summary>详解</summary>

判据是母题三的可再生性条件："丢了可重算，且**错了会被就地纠正**"。hint bits 满足：
它只是 CLOG 的缓存，读者发现没有 hint 就查 CLOG，不存在"hint 缺失导致错误结果"的路径；
且设置方向单调（事务终态不变）。而 `PD_ALL_VISIBLE` 与 visibility map 位联动，被
index-only scan **信任为真**——VM 位说"全可见"，扫描就不回堆验证。一个错误置位的
VM 位会直接返回错误数据，没有"使用时验证"的兜底，所以它的置位必须经 WAL 保护以保证
崩溃后堆页与 VM 页一致（清除方向反而可以宽松，因为"全可见位丢失"只导致多余的堆访问，
是安全方向）。一般化的结论：**建议性（advisory）状态可免 WAL，权威性（authoritative）
状态不可**；判断一个位是哪种，看有没有代码在不验证的情况下信任它。
</details>

**Q2. Trick 8（fastpath 锁）中，弱锁方为什么必须在**持有 fpInfoLock 之后**才检查
`FastPathStrongRelationLocks->count`？把检查提前到拿锁之前（省一次可能白拿的锁）会
出什么错？**

<details><summary>详解</summary>

Dekker 论证依赖"写自己的旗标 → 栅栏 → 读对方的旗标"的顺序。弱锁方的"旗标"是
fastpath 槽位内容，但槽位写入发生在检查 count **之后**——顺序看似反了。真正的写读序
是由 fpInfoLock 建立的：强锁方扫描槽位时要逐个拿同一把 fpInfoLock，所以"弱锁方在
临界区内先查 count 再写槽"与"强锁方先加 count 再进临界区扫槽"之间，任何交错里
两把锁的获取顺序都强制了可见性：若弱锁方的临界区先行，强锁方随后扫描必见其槽位；
若强锁方 count 递增先于弱锁方进临界区，则 LWLockAcquire 的获取栅栏保证弱锁方必读到
新 count（lock.c:995 注释正是此意）。若把 count 检查移到 fpInfoLock 之外，检查与
写槽之间就出现了无栅栏窗口：强锁方可能在这个窗口里完成"加 count + 扫描"，既没看到
槽位（还没写），弱锁方也没看到 count（已查过）——两边同时隐身，DDL 与 DML 并发穿行。
教训：借既有锁当栅栏时，被保护的读写必须**真的在临界区内**。
</details>

**Q3. Trick 17 中，恢复逻辑对"随机起点"（randAccess）只要求 `xl_prev < RecPtr`，而
顺序读取时要求 `xl_prev == PrevRecPtr` 精确相等（xlogreader.c:1158–1188）。为什么两种
场合标准不同？都用宽松标准会怎样？**

<details><summary>详解</summary>

顺序读取时读者知道前一条记录的确切地址，精确匹配是最强判据，能识破"撕裂页上扇区边界
处残留的旧记录"——旧记录内部一致（CRC 对）、位置合法，唯独它的 prev 指向**旧轮次**的
前驱，与本轮扫描维护的 PrevRecPtr 不等，立即穿帮。随机起点（比如从检查点记录地址直接
跳入）没有前驱知识，只能退而求其次做单调性检查（WAL 地址严格递增，prev 必小于自身）；
这是安全的，因为随机起点本身来自可信源（pg_control 里的检查点指针，有独立 CRC），
不承担"探测末尾"的职责。若顺序读取也放宽为 `<`：复用段中的旧记录 prev 同样满足小于
关系，恢复会把上一轮的旧 WAL 当新记录重放——数据损坏。这题的要点是：**同一字段在
不同信任上下文里承担不同强度的证明义务**。
</details>

**Q4. Trick 11 的组提交里，follower 把自己 CAS 进链表后发现自己不是第一个，于是睡等。
如果 leader 在 follower 完成入队后、睡下之前就完成了全组清理并发出唤醒，follower 会
不会错过唤醒而永久睡死？**

<details><summary>详解</summary>

不会，有两道防线。第一，唤醒媒介是 PGPROC 信号量（`PGSemaphoreUnlock`），信号量有
计数：先 unlock 后 lock 也能立刻通过——唤醒不会像信号那样"发早了就丢"。第二，
follower 醒来后并不假设"醒 = 完成"，而是循环检查 `proc->procArrayGroupMember`
（procarray.c:824 起的 for 循环：伪唤醒或提前唤醒都会重新睡），该标志由 leader 在
完成清理后清除，构成真正的完成信号；标志的写读在信号量操作蕴含的栅栏下有序。这体现
了唤醒协议的通用纪律（同 Trick 13 的 latch）：**唤醒机制只负责"可能有进展"，完成与否
永远由状态变量说了算**；任何"以唤醒本身为完成信号"的设计都有丢失唤醒或伪唤醒竞态。
</details>

**Q5. Trick 15 说 hdrmask 在 64 bit 里塞了 65 bit 信息。写出这件事成立的充要条件，并
说明如果未来某个分配器想让 chunk 不按 MAXALIGN 对齐，会破坏什么。**

<details><summary>详解</summary>

第 3 段（30 bit 值域）最高位与第 4 段（30 bit 块偏移）最低位共享一个物理 bit。成立的
充要条件：**块偏移的最低位恒为 0**，即 chunk 地址与块地址都 MAXALIGN 对齐（MAXALIGN ≥ 8，
故偏移为 8 的倍数，最低位恒 0，甚至最低三位恒 0——当前只借用了一位）。读取方
`HdrMaskBlockOffset` 掩掉最低位重建偏移；写入方把第 3 段的最高位直接写在共享 bit 上。
若某分配器让 chunk 4 字节对齐：偏移可能是 4 的奇数倍，最低位为 1，与第 3 段最高位的
值叠加后两段互相污染——块偏移读出会多算或少算 1，`GetMemoryChunkContext` 找错块头，
后果是内存管理器级别的损坏。事实上 `GetMemoryChunkMethodID`（mcxt.c:224）的第一道
Assert 就是 `pointer == MAXALIGN(pointer)`——对齐不是风格约定，而是编码空间的
承重墙。这题的一般教训：**位借用技巧的每一位都对应一条必须被断言钉死的对齐/取值不变式**。
</details>

**Q6. Trick 6 的 LSN interlock：`XLogNeedsFlush(commitLSN) && BufferGetLSNAtomic(buffer)
< commitLSN` 时拒绝设置 hint。请解释为什么"页 LSN ≥ commitLSN"时即使 commit 记录
尚未刷盘也可以放心设置 hint。**

<details><summary>详解</summary>

危险场景是：hint（标记"xid 已提交"）随脏页落盘，随后崩溃，而 commit 记录没进 WAL——
恢复后该事务实际是中止的，磁盘上却留着"已提交"的 hint，未来的读者会看到幽灵数据。
所以安全条件是"**hint 落盘 ⇒ commit 记录已落盘**"。现在看页 LSN ≥ commitLSN 的情形：
WAL-before-data 规则（bufmgr 在写出页前必调 `XLogFlush(PageGetLSN(page))`）保证任何页
在落盘前，其页 LSN 之前的全部 WAL 已刷盘；页 LSN ≥ commitLSN 意味着刷到页 LSN 必然
连带把 commit 记录刷了。于是所需蕴含式**传递性成立**：hint 落盘 → 页已落盘 → 页 LSN
前的 WAL 已刷 → commit 记录已刷。反之页 LSN < commitLSN 时不存在这条传递链（页可以
先落盘），且设置 hint 又不允许推高页 LSN（可能只持共享锁，heapam_visibility.c:118–121
注释言明），只好放弃、留待未来重试——反正 hint 是惰性可再生的（母题二、三），放弃的
代价只是下次多查一遍 CLOG。
</details>

**Q7. 对照 Trick 3（自动生成 copy/equal）与 Trick 21（X-macro）：两者都是"单一事实源 +
生成"，为什么节点系统用外部 Perl 脚本解析 C 头文件，而关键字表用 C 预处理器的 X-macro
就够了？给出选择判据。**

<details><summary>详解</summary>

判据是**生成目标与源信息的复杂度**。关键字表的"记录"是四元组常量，每个消费者要的只是
"对每条记录展开一次宏"——恰好是 C 预处理器能表达的全部能力，X-macro 零外部依赖、
增量编译友好，够用即最优。节点系统的源信息是**C 类型系统本身**：字段的类型（`List *`
要深拷贝、`Bitmapset *` 有专用拷贝、`char *` 要 strdup、数组要按长度字段循环）、
注释属性（`pg_node_attr(read_write_ignore)`）都决定生成逻辑，而预处理器不能反射
结构体定义——它看不见字段类型，也做不了按类型分派。因此必须上升到能解析 C 声明的
外部工具（gen_node_support.pl 自带一个够用的 C 结构解析器）。推论：当"事实源"是
数据时用 X-macro；当"事实源"是**代码结构**时才值得引入构建期解析器；两者之间还有
第三档——kwlist 的完美哈希也用了外部脚本，但那是因为目标（哈希函数系数）需要搜索
计算，超出宏的计算能力。选工具的尺度：用能力恰好覆盖需求的最低档机制。
</details>

---

## 附：与其他章节的交叉引用

- Trick 1/2/6/7：第 06 章（页面布局）、第 08 章（MVCC 与锁）、第 10 章（pruning/VACUUM）；
  Trick 8–13：第 08 章 §锁管理器、第 12 章（IPC 与后台进程）；Trick 14/15：第 11 章 §内存管理。
- Trick 16/17/19：第 09 章、T5（持久化与恢复设计空间）；Trick 18：第 06 章 §缓冲池；
  Trick 20/25：第 15 章、T1；Trick 21/22：第 02 章、第 01 章 §错误处理；Trick 23/24：第 04 章、第 14 章 §JIT。