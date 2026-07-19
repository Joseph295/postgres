# 研究资料 · PostgreSQL Table AM 接口与生态调查（截至 2026 年 7 月）

> 本文是 T1《存储引擎设计空间》的支撑调研，独立成篇保存。共执行 12 轮 WebSearch、8 次 WebFetch 核实；论断均附来源，无法完全核实处标注"不确定"。

## 一、接口演化（PG 12 → PG 18）

**PG 12（2019）**：引入 `TableAmRoutine` 可插拔表存储接口（约 38 个回调），heap 为唯一内置实现。接口本质是"围绕 heap 设计后部分泛化的元组形抽象"，而非真正的存储层抽象（评价来源：Christophe Pettus《A Field Guide to Alternative Storage Engines for PostgreSQL》，2026-05-08，thebuild.com）。

**PG 14（2021）**：新增 TID Range Scan 执行节点及配套 table AM 回调（`scan_set_tidrange`/`scan_getnextslot_tidrange`）。**PG 15（2022）**：未发现对 table AM API 的显著改动（不确定，未逐条核对 tableam.h 历史）。

**PG 16（2023）**：Andres Freund 重构 relation extension——批量扩展页与扩展锁管理下沉到 bufmgr（`ExtendBufferedRel*`），table AM 不再自行处理扩展锁，COPY 等场景显著提速（设计线程 2022-10、commit 2023-04，pgsql-hackers）。

**PG 17（2024）**：引入 streaming read（vectored ReadBuffer）API，顺序扫描与 ANALYZE 改走 read stream，table AM 的 analyze 接口签名随之变化（pganalyze 2024；commitfest #4875）；修复 TID Range Scan 缺失 EvalPlanQual recheck 的问题。

**Korotkov 2024 年初的 table AM 批量提交与 revert 事件**：Alexander Korotkov 在 2024-03 CommitFest 末为 OrioleDB 需求集中提交一批 "Table AM Interface Enhancements"（线程始于 2023-11，约 2000 行）。Matthias van de Meent 批评文档不足、目标不清、担忧 table AM 绕过用户静默操纵索引（2024-03-03）。因"仓促、缺乏共识"且 Andres Freund 事后审查提出质量问题，Korotkov 于 2024-04 陆续 revert：
- 2024-04-11 revert "Custom reloptions for table AM"（撤销 9bd99f4c26、422041542f）；
- 2024-04-16 revert "Generalize relation analyze in table AM interface"（撤销 27bc1772fc、dd1f6b0c17，注明 "per review by Andres Freund"）。
Korotkov 本人承认"为赶 feature freeze 而仓促"。据 Postgres Professional 的综述，`tuple_insert()` 可返回不同 slot、`rd_amcache` 可存复杂结构等较小改动保留在了 PG 17（保留清单未逐一对照 git，细节不确定）。pganalyze 对 17 beta1 的总结确认这批改动"过于侵入性，决定推迟重做"（2024-05-25）。

**PG 18（2025-09 发布）**：落地 AIO 子系统（io_method=worker/io_uring），read stream 覆盖更多扫描路径，为非 heap AM 提供更好的 I/O 基础设施。EDB 称 PG 18 的 index AM API 可插拔性有提升（具体 commit 未逐一核实，不确定）。OrioleDB 所需核心补丁并未如其 2024–2025 年期望全部并入 PG 18。

## 二、非 heap 表 AM 生态现状（2026 年中）

| 项目 | 是否真 TAM | 现状 | 是否需 patch PG |
|---|---|---|---|
| **Citus columnar** | 是 | 随 Citus 维护，2025 年有 PG 18 兼容修复；append-only，不支持 UPDATE/DELETE；Azure 托管可用 | 否 |
| **TimescaleDB hypercore TAM** | 曾是 | **已退场**：2.18（2025 年初）引入实验性 hypercore TAM；2.21.0（2025-07-08）宣布弃用，原文 "did not show the signals we hoped for"；2.22.0（2025-09-02）移除，残留使用会阻塞升级。公司 2025 年更名 TigerData（月份不确定）。columnstore 能力回归其自有（非 TAM）压缩架构 | 否 |
| **OrioleDB** | 是（undo-log MVCC、index-organized） | 最新 **beta16，2026-06-18**，补丁集基于 PG 16.13/17.9/18.4，新增 SERIALIZABLE 支持与 amcheck 集成；**仍是 beta、仍需 patched PostgreSQL、未 GA**。团队隶属 Supabase，平台上以 Public Alpha 提供（"2024 被 Supabase 收购"一说时点不确定）。是最可能暴露 TAM API 边界的项目 | 是 |
| **pg_mooncake** | v0.1 是（DuckDB 列存 TAM），v0.2 转向 | v0.1.2（2025-02-12）后，v0.2 架构转为 moonlink 实时镜像 Postgres 表到 Iceberg 列存，弱化自建 TAM。**Mooncake Labs 已于 2025-10 被 Databricks 收购**，并入 Lakebase/Neon 体系；收购后独立开源扩展的前景**不确定** | 否 |
| **ParadeDB** | 否（pg_search 走 **index AM**，BM25 索引写入 PG 块存储、参与 WAL/复制） | pg_search 活跃（pgxn 0.22.x，2025）；pg_analytics 2025-03 归档停维，分析能力并入 pg_search | 否 |
| **pg_duckdb** | 否（查询加速层/FDW 式，非主存储 TAM） | DuckDB 官方 + MotherDuck/Hydra 合作，2025 年持续活跃 | 否 |
| **Hydra columnar** | 是（Citus columnar 分叉） | 2025-02 仍有更新，但公司重心转向基于 pg_duckdb 的 serverless 分析，columnar 维护强度存疑（不确定） | 否 |

## 三、社区对接口局限的讨论

核心论点四条（最系统的陈述：Korotkov《Why PostgreSQL needs a better API for alternative table engines?》，2025-03-24，orioledb.com；及 Pettus field guide 2026-05-08）：

1. **TID 6 字节假设**：行标识硬编码为 32 位块号 + 16 位偏移（48 bit 可用），index AM 存的就是这种 TID；index-organized table 或以主键定位行的引擎（OrioleDB）无处安放超过 48 bit 的标识。官方文档明确承认此约束。
2. **无法自定义行标识 / 索引更新"全有或全无"**：系统假设一个行版本要么被所有索引索引、要么都不；排除了 WARM 类优化。
3. **MVCC/undo 语义绑定 heap**：可见性判断、hint bits、VACUUM 假设渗入执行器与索引路径；undo-based MVCC 需要 index AM 支持 "retail index tuple deletion"（单条索引项删除），而现接口只有批量删除——Korotkov 与 Freund 等人的讨论中反复出现（"Table AM Interface Enhancements" 线程 2023-11 起；奠基讨论见 Andres Freund "Pluggable Storage - Andres's take"，2018–2019）。
4. **index AM 与 table AM 耦合**：van de Meent 指出 table AM 不应静默改写索引行为，自定义 reloptions、索引通知等应等 index AM API 重设计后再做——这正是 2024 年 revert 的深层原因。

会议侧：Korotkov 在 pgconf.dev 2024 做扩展 table AM API 报告；PGConf NYC 2025 有 Table/Index AM 专题。Mark Dilger 在 index AM API 扩展性方向的具体线程未直接检索确认（不确定）。

## 四、截至 2026 年中的状态判断

1. **TAM 生态"苏醒但未成熟"**（Pettus "Table Access Methods Wake Up"）：heap 仍是唯一生产级通用 OLTP 存储；TimescaleDB 这一资金最充足的玩家已在 2025-09 主动放弃 TAM 路线，是重要的反面信号。
2. **OrioleDB 是最接近"第二个真正表引擎"的项目**：beta16（2026-06-18）已支持 PG 18 与 SERIALIZABLE，但仍需 patched PG、未 GA；其补丁并入主线的进度落后于 2024–2025 年的乐观预期，核心 API 改动经 2024 年 revert 事件后走上"更慢但更审慎"的路径。
3. **分析类方案普遍绕开 TAM**：pg_duckdb、moonlink/Iceberg 镜像（pg_mooncake v0.2、Databricks Lakebase）选择加速层/镜像架构而非实现完整 TAM，侧面印证当前 TAM API 对列存 + 索引 + MVCC 组合的支撑不足。
4. **主线的实质进展在基础设施层**：PG 16 relation extension、PG 17 streaming read、PG 18 AIO 为任何 AM 提供了更好的 I/O 底座；但 TID 泛化、retail index deletion、index/table AM 解耦等结构性问题在 PG 18 仍未解决，预计是 PG 19+ 的议题（推断，不确定）。
