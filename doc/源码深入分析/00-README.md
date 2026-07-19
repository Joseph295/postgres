# PostgreSQL 源码深入分析系列 · 总索引

> 本系列是《PostgreSQL源码学习指南》各章的**逐段/逐行展开**：指南是地图，本系列是走读实录。
> 全系列 15 章均为 v2 深度版（总计约 2.4 万行），统一执行以下标准：
> - **完整走读**：每章 3–7 个核心函数从头到尾分段走读（省略处注明内容与原因），代码摘录与源码逐字一致；
> - **所以然三问**：每个机制回答"解决什么问题（含反面场景推演）/ 为何不用替代方案 / 正确性依赖什么不变式"；
> - **不变式清单**：每章列出关键不变式及违反后果；**what-if 推演**：崩溃/并发交错的场景推演；
> - **设计史考古**：每章引用 3–20 个经 git log 核实的设计演进 commit；
> - 结构统一：本章导读 → 前置知识 → 正文（各节小结）→ 不变式/设计史 → 自测 8–10 题带详解。
> 所有 `文件:行号` 引用基于本仓库 20devel 源码核实；行号会随版本漂移，函数名与文件路径长期稳定。
> 配套实操见《PostgreSQL动手实验手册》。

## 文档清单与阅读顺序

| 序号 | 文档 | 对应指南章节 | 核心源码 |
|---|---|---|---|
| 01 | 进程模型与命令循环 | 第 1–2 章 | postmaster.c, launch_backend.c, tcop/postgres.c |
| 02 | 解析器与重写器 | 第 3 章 | scan.l, gram.y, analyze.c, rewriteHandler.c |
| 03 | 查询优化器 | 第 4 章 | planner.c, allpaths.c, joinrels.c, costsize.c, selfuncs.c |
| 04 | 执行器 | 第 5 章 | execMain.c, execProcnode.c, nodeXxx.c, execExpr*.c |
| 05 | 写路径与事务提交 | 第 6 章 | heapam.c, xloginsert.c, xact.c |
| 06 | 存储管理与缓冲池 | 第 7 章 | bufmgr.c, freelist.c, smgr/md.c, freespace.c, visibilitymap.c |
| 07 | 访问方法与索引 | 第 8 章 | tableam.h, nbtree/, gin/, gist/, brin/ |
| 08 | 事务、MVCC 与锁 | 第 9 章 | xact.c, procarray.c, heapam_visibility.c, lmgr/ |
| 09 | WAL 与崩溃恢复 | 第 10 章 | xlog.c, xloginsert.c, xlogrecovery.c |
| 10 | VACUUM 与 autovacuum | 第 11 章 | vacuumlazy.c, pruneheap.c, autovacuum.c |
| 11 | 系统目录、缓存与内存管理 | 第 12–13 章 | catalog/, syscache.c, relcache.c, inval.c, mcxt.c, aset.c |
| 12 | 进程间通信与后台进程 | 第 14 章 | shmem.c, waiteventset.c, checkpointer.c, bgworker.c |
| 13 | 复制与高可用 | 第 15 章 | walsender.c, walreceiver.c, slot.c, logical/ |
| 14 | 统计、并行、JIT 与分区 | 第 16 章 | analyze.c, parallel.c, nodeGather.c, jit/, partprune.c |
| 15 | 扩展机制 | 第 17 章 | extension.c, foreign/, contrib/pg_stat_statements, contrib/postgres_fdw |

## 与学习路线图的对应

- **阶段 1**（走通执行路径）：读 01–04；
- **阶段 2**（存储与事务内核）：读 05–10（全路线图的重心）；
- **阶段 3**（优化器与执行器进阶）：重读 03–04 + 读 14；
- **阶段 4**（写扩展）：读 15，参考 11–12；
- **阶段 5–6**（社区与纵深）：13 与你选定的纵深方向对应篇目。

## 使用建议

1. **对照源码读**：左边开文档，右边开对应源文件；每个代码摘录都值得跳进源码看上下文；
2. **先做自测**：每篇文末的自测问题也可用作读前摸底——都会答就跳读；
3. **配合实验**：读完一篇，做《动手实验手册》里对应编号的实验，把"看懂"升级为"验证过"；
4. **行号漂移时**：`grep -n "^函数名" 文件路径` 重新定位。
