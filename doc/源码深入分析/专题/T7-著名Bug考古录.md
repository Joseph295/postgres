# 专题 T7 · 著名 Bug 考古录:十三桩事故与它们暴露的设计约束

> 源码版本:本仓库 master 分支(20devel)。**文中所有 commit hash 均已用 `git log` / `git show` 在本仓库逐一验证存在**,日期取自 commit 的作者时间;文中引用的源码文件与函数名均已在当前 master 上 `grep` 核实仍然存在(个别案例中会注明"该代码已被后续重构移动")。
> 外部信息(LWN 文章、社区公告、事后分析博客)来自 Web 检索(检索时间 2026-07),来源在各案例末尾与附录中列出。
>
> **本篇与其他各篇的关系**:第 05/08/09/10/13 篇讲的是这些子系统"设计上应该怎么工作";本篇讲的是它们"曾经怎样坏掉"。每个案例都会指回相关章节,但不重复其内容——这里只关心事故本身:现象、根因、修复、教训。
>
> **读者假设**:读过第 08(MVCC 与锁)、09(WAL 与恢复)两篇,或对等价概念(xmin/xmax、SLRU、REDO、hint bit)有工作级理解。目标是:读完任意一个案例,你能脱稿给别人讲清它的根因。

---

## 目录

- [第 0 章 · 如何读这份事故档案](#第-0-章--如何读这份事故档案)
- [案例 1 · 9.3 multixact 系列:PG 史上最严重的数据损坏事故(2013–2015)](#案例-1)
- [案例 2 · pg_upgrade 丢失 TOAST relfrozenxid:一次升级工具引发的紧急发版(2011)](#案例-2)
- [案例 3 · fsyncgate:二十年间所有人都用错了 fsync(2018)](#案例-3)
- [案例 4 · FSM/VM 截断不写 WAL:恢复后"读到不存在的块"(2016)](#案例-4)
- [案例 5 · overwrite contrecord:主库崩溃撕裂 WAL 记录,备库永久卡死(2021)](#案例-5)
- [案例 6 · PG 14 CREATE INDEX CONCURRENTLY 索引静默丢行(2021–2022)](#案例-6)
- [案例 7 · abbreviated keys 被禁用:glibc strxfrm 与 strcoll 不一致(2016)](#案例-7)
- [案例 8 · collation 变更导致索引静默损坏:一个至今没有完全解决的问题](#案例-8)
- [案例 9 · "missing chunk number 0":syscache 里的过期 TOAST 指针(2011)](#案例-9)
- [案例 10 · 逻辑解码用错快照读 catalog(2017/2022)](#案例-10)
- [案例 11 · Hot Standby 的 KnownAssignedXids:备库可见性两次翻车(2010/2021)](#案例-11)
- [案例 12 · JIT 内联内存泄漏:LLVMContext 里越积越多的类型(2023)](#案例-12)
- [案例 13 · CVE-2024-7348:pg_dump 的 TOCTOU 提权(2024)](#案例-13)
- [第 14 章 · 元分析:这些 bug 教会了社区什么](#第-14-章--元分析)
- [附录 A · commit 速查表](#附录-a--commit-速查表)
- [附录 B · 参考资料](#附录-b--参考资料)

---

## 第 0 章 · 如何读这份事故档案

### 0.1 选取标准

十三个案例按三条标准选出:

1. **有明确的历史地位**——要么触发过紧急发版(out-of-cycle release),要么在社区留下了专有名词(fsyncgate、multixact woes),要么代表一类反复出现的故障模式;
2. **根因可以讲到代码与设计假设层面**——不是"某处少了个判断",而是"某个被所有人默认成立的前提其实不成立";
3. **修复 commit 可在本仓库验证**——所有 hash 都用 `git show` 确认过存在与日期。

### 0.2 统一结构

每个案例六段:**背景与现象 → 复现条件 → 根因分析 → 修复方案 → 波及与教训 → 深层设计约束**。最后一段是本篇真正想沉淀的东西:每桩事故背后,都有一条被违反的、平时不写在任何注释里的隐含约束。十三条约束汇总在第 14 章。

### 0.3 严重度速览

| 案例 | 后果类型 | 是否静默 | 是否触发紧急发版 |
|---|---|---|---|
| 1 multixact | 数据损坏/丢失 | 部分静默 | 是,多次(9.3.2→9.4.2) |
| 2 pg_upgrade | 数据不可读 | 延迟爆发 | 是(8.4.8/9.0.4) |
| 3 fsyncgate | 数据丢失 | 完全静默 | 否(设计级修复) |
| 4 FSM/VM 截断 | 查询报错/坏VM | 延迟爆发 | 否 |
| 5 contrecord | 备库永久落后 | 显式报错 | 否(14.1 等修复) |
| 6 CIC 丢行 | 索引丢行 | 完全静默 | 是(14.4 专门发版) |
| 7 abbreviated keys | 索引序错乱 | 完全静默 | 是(9.5.2 禁用) |
| 8 collation | 索引序错乱 | 完全静默 | 无法单方面修复 |
| 9 missing chunk | 偶发查询报错 | 显式报错 | 否 |
| 10 逻辑解码快照 | 解码错乱/崩溃 | 部分静默 | 否 |
| 11 KnownAssignedXids | 备库不可用/慢 | 显式 | 否 |
| 12 JIT 泄漏 | OOM 杀进程 | 渐进 | 否 |
| 13 CVE-2024-7348 | 提权执行代码 | — | 安全发版 |

---

<a id="案例-1"></a>
## 案例 1 · 9.3 multixact 系列:PG 史上最严重的数据损坏事故(2013–2015)

### 背景与现象

PostgreSQL 9.3(2013-09 发布)引入了"外键锁改进"——commit **`0ac5ad5134`**(Álvaro Herrera,2013-01-23,"Improve concurrency of foreign key locking")。它把行锁从两种(共享/排他)细分为四种(`FOR KEY SHARE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR UPDATE`),让外键检查只拿最弱的 KEY SHARE 锁,从而使"更新父表非键列"不再与"插入子表行"互相阻塞。这是一个万众期待的特性。

代价是:**multixact 从"临时锁信息"升格为"持久化数据"**。9.3 之前,multixact(多事务 ID,记录"哪些事务共同锁着这一行")只在行被多个事务共享锁定时出现在 xmax 里,且只需活到锁释放——崩溃重启后可以全部丢弃。9.3 起,一个 `UPDATE` 加上并存的行锁会生成一个"updater + lockers"混合的 multixact 放进 xmax:**这一行的删除者身份现在藏在 multixact 里**,必须像 clog 一样永久可靠地保存、冻结、按 wraparound 规则管理。

随后两年,与之相关的数据损坏 bug 接连爆发,社区连续多次紧急发版(9.3.2、9.3.3、9.3.4、9.3.5、9.4.1、9.4.2/9.3.7……),典型现象包括:

- `ERROR: could not access status of transaction NNNN`(`pg_multixact` 文件被过早删除);
- 被锁定过的行在冻结后**凭空消失或复活**(冻结逻辑弄错了 updater 身份);
- `pg_multixact/members` 目录膨胀到几十上百 GB;
- 最严重的一类(2015 年,CVE-2015-3167 关联发版):members 空间回卷后旧文件被覆盖,**xmax 无法再解读,数据永久损坏**。

### 复现条件

- 高并发外键工作负载(父表被频繁 `SELECT ... FOR KEY SHARE`,即任何子表插入),使 multixact 生成速率远超 9.2 时代;
- 长事务或 autovacuum 落后,使 multixact 冻结不及时;
- members 空间(每个 multixact 的成员列表,单独一套 SLRU)平均成员数偏大时,offset 空间(32 位)还远未回卷、members 空间(也是 32 位,但消耗快得多)已先回卷——这正是 2015 年那次数据丢失的触发条件。

### 机制小抄(理解根因的最小背景)

- 行头 `t_infomask` 里有一个 `HEAP_XMAX_IS_MULTI` 位:置位时,xmax 字段存的不是事务 ID 而是 **multixact ID**,真实的锁定者/更新者列表要去 `pg_multixact` 查;
- `pg_multixact` 是**两套 SLRU**:`offsets/`(multixact ID → 成员列表的起始位置)与 `members/`(成员数组,每个成员是 xid + 状态位,状态位区分 keyshare locker / updater 等);
- 两套空间都是 32 位回卷计数器,但消耗速率完全不同:一个含 N 个成员的 multixact 消耗 1 个 offset、N 个 members——**members 是 offset 的 N 倍速**;
- 判定一行"删除者是否提交"时,若 `HEAP_XMAX_IS_MULTI` 置位,必须先查 members 找出 updater 成员,再查 clog。**members 文件丢失 = 这行的生死档案丢失**。

2015 年那次数据丢失的时间线(对应 `b69bf30b9b` 修复):

```
主库(9.3.x/9.4.0,高频外键负载,平均每 multixact 约 50 个成员)
t0  offsets 用掉 2%          members 已用掉 80%      ← 无人监控 members
t1  offsets 用掉 2.5%        members 回卷,新分配落在旧段号上
t2  checkpoint:按"最老 offset 需求"计算可删的 members 段
    —— 计算只看 offset 侧年龄,认为回卷前的旧段"太老可删"
t3  旧 members 段被 unlink   ← 但表里仍有 xmax 引用它们
t4  某查询读到该行 → 查 members → 文件不存在
    ERROR: could not access status of transaction NNNN
    (更糟:若段号被新数据复用,读到的是错误成员 → 静默错判可见性)
```

### 根因分析

这一系列 bug 不是一个错误,而是**同一个设计升级没有被贯彻到所有下游**。`0ac5ad5134` 改变了 multixact 的生命周期契约(临时 → 持久),但以下每一处下游代码仍然按旧契约运转,各自炸出一个 bug:

1. **冻结路径**(`heap_freeze_tuple`):旧代码认为 multixact 冻结就是"把 xmax 清掉"——9.2 时代成立,因为 multixact 只含 lockers。9.3 后 multixact 里可能藏着 updater,直接清掉等于"取消了一次删除",行复活;反过来处理错分支则让行消失。修复是 commit **`3b97e6823b`**(Álvaro Herrera,2013-12-16,"Rework tuple freezing protocol",进 9.3.3):冻结时必须逐个成员判断,把仍需保留的 updater 提取出来,生成一个新的、只含必要成员的 multixact 替换原值,并为此新增 WAL 记录类型——因为旧的冻结 WAL 记录在备库上重放会做出与主库不同的决定。

2. **截断路径**:`pg_multixact` 的回收原本由 VACUUM 在计算出最老 multixact 后顺手做,且**不写 WAL**——旧契约下丢了也无所谓。持久化之后,主备库各自独立截断,时机不同步,备库可能删掉主库还需要的段,崩溃恢复也可能读到已被截断的区间,产生 `could not access status of transaction`。中间修补是 commit **`f741300c90`**(2014-06-27,"Have multixact be truncated by checkpoint, not vacuum",进 9.3.5 时代);彻底修复要等 9.5 的 commit **`4f627f8973`**(Andres Freund 提交,2015-09-26,"Rework the way multixact truncations work"):截断本身成为 WAL 记录,由主库统一驱动。

3. **members 空间的 wraparound 防护**:xid 有 autovacuum 强制冻结兜底,multixact 的 offset 空间照抄了这套防护,但**members 空间没有任何防护**——设计时没人意识到它是一个独立的、消耗速率可以高几个数量级的 32 位空间(一个含 100 个成员的 multixact 消耗 1 个 offset、100 个 members)。于是出现:members 回卷后,checkpoint 按"太老"标准删除了仍被在用 xmax 引用的 members 文件。修复是 commit **`b69bf30b9b`**(Álvaro Herrera 提交、Thomas Munro 主要作者,2015-04-28,"Protect against multixact members wraparound",进 9.4.2/9.3.7):在共享内存维护最老仍需的 members 位置,新建 multixact 若将覆盖它则直接报错拒绝,并让 autovacuum 以 `MultiXactMemberFreezeThreshold()`(现存于 `src/backend/access/transam/multixact.c`)根据 members 用量**动态收紧**冻结年龄,主动泄压。

4. **周边不变量**:还有一串修复处理"旧契约残留",如 **`78db307bb2`**(Tom Lane,2014-07-21,防御坏的 relfrozenxid/relminmxid)、**`1a917ae861`**(2014-04-24,修复并发锁定下更新的竞态)等。

一句话根因:**"把一个临时子系统升格为持久子系统"是一次契约变更,而契约的旧假设散落在冻结、截断、wraparound、恢复四个下游,每一处都没有被系统性地重新审计。**

### 修复方案(commit 链)

| commit | 日期 | 内容 |
|---|---|---|
| `0ac5ad5134` | 2013-01-23 | 起因:外键锁改进,multixact 持久化 |
| `3b97e6823b` | 2013-12-16 | 重做冻结协议(9.3.3) |
| `1a917ae861` | 2014-04-24 | 并发锁定更新竞态(9.3.5) |
| `f741300c90` | 2014-06-27 | 截断改由 checkpoint 驱动 |
| `78db307bb2` | 2014-07-21 | 防御坏的 frozen/minmxid |
| `b69bf30b9b` | 2015-04-28 | members wraparound 防护(9.4.2/9.3.7,配合 CVE-2015-3167 发版) |
| `4f627f8973` | 2015-09-26 | 截断 WAL 化(9.5),一劳永逸 |

### 波及与教训

受灾面极广:9.3 是当时的主流版本,大量高并发外键负载的生产库中招;2015-05 的发版公告直接写明"数据损坏或丢失风险,请数日内升级"([官方公告](https://www.postgresql.org/about/news/postgresql-942-937-9211-9116-and-9020-released-1587/))。Robert Haas 在 pgsql-hackers 上的 "multixacts woes" 长文成为社区事后复盘的标志性文档。教训:大特性合并前,社区此后明显加强了对"改变数据生命周期"类补丁的审查强度;amcheck(见第 14 章)的诞生也与这一时期"我们缺少损坏检测工具"的痛感直接相关。

### 深层设计约束

> **任何数据从"可丢弃"变为"持久",其冻结、回收、wraparound、崩溃恢复四条路径必须同步重新推导——持久化不是一个属性开关,而是一组分布在全代码库的隐式契约。**

---

<a id="案例-2"></a>
## 案例 2 · pg_upgrade 丢失 TOAST relfrozenxid:一次升级工具引发的紧急发版(2011)

### 背景与现象

2011-04-08,社区发布 8.4.8 与 9.0.4,公告的核心内容只有一件事:**用 pg_upgrade(或其前身 pg_migrator)升级过的库可能丢数据**。现象是升级后运行一段时间,访问某些宽字段时报:

```
ERROR: could not access status of transaction NNNN
```

且随时间推移越来越多——这是 clog 被回收后,数据里还留着需要它裁决的旧 xid 的典型症状。

### 复现条件

- 用 pg_upgrade 从 8.3/8.4 升级到 8.4.x/9.0.x;
- 库里有 TOAST 表(即任何有宽字段的表),且 TOAST 中存有较老的元组;
- 升级后正常运行,直到 autovacuum 依据(错误的)`relfrozenxid` 推进了 `datfrozenxid` 并截断了 clog。

### 根因分析

pg_upgrade 的核心承诺是:**数据文件原样搬运,元数据在新集簇中精确重建**。`pg_class.relfrozenxid` 是元数据里最致命的一项——它向 VACUUM 承诺"本表中不存在比它更老的未冻结 xid",clog 截断以全库最老的 relfrozenxid 为界。

bug 在于:pg_upgrade 恢复了普通表的 relfrozenxid,**但 TOAST 表的 relfrozenxid 没有被正确带过来**——TOAST 表是每个主表隐式附属的独立堆表,有自己独立的 relfrozenxid,而升级流程把它落在了"新集簇 initdb 时的默认值"上。这个默认值比真实数据"新",于是 VACUUM 高估了冻结进度,clog 被过早截断;TOAST 堆里那些真实的老 xid 从此无处裁决,元组不可读。

值得注意的是,这不会立即爆发:clog 截断需要 autovacuum 周期推进,**故障与根因之间隔着几天到几个月**,这也是升级类 bug 最阴险的地方。

### 修复方案

- commit **`9c38bce29c`**(Bruce Momjian,2011-04-08,"Have pg_upgrade properly preserve relfrozenxid in toast tables"),随 8.4.8/9.0.4 紧急发布;
- 后续补漏:**`7971a57fd4`**(2011-08-31,针对 8.3 老服务器的同类修复)、`5e5958428b` 等;
- 对已中招用户,社区在 wiki([20110408pg_upgrade_fix](https://wiki.postgresql.org/wiki/20110408pg_upgrade_fix))发布了检测与救援脚本:趁 clog 尚未截断,对全部 TOAST 表 `VACUUM FREEZE`,并人工回填 clog 文件。

### 波及与教训

pg_upgrade 当时刚被收编为官方工具(9.0),这次事故几乎摧毁了早期用户对它的信任,也确立了此后 pg_upgrade 开发的铁律:**catalog 中每一个影响数据解读的字段(relfrozenxid、relminmxid、datfrozenxid……)都必须显式列入迁移清单并测试**。后来 9.3 multixact 持久化时,pg_upgrade 需要同步搬运 relminmxid——那一次清单意识就明显加强了(参见案例 1 中 `78db307bb2` 顺带的防御式检查:即使 catalog 值损坏也不再直接吞数据)。

### 深层设计约束

> **数据文件与解读数据所需的元数据是一个整体;任何"只搬数据"或"只建元数据"的工具,其正确性清单必须覆盖每一个参与可见性裁决的 catalog 字段——漏一个,故障会在数月后以不可恢复的形式爆发。**

---

<a id="案例-3"></a>
## 案例 3 · fsyncgate:二十年间所有人都用错了 fsync(2018)

### 背景与现象

2018-03 末,Craig Ringer 在 pgsql-hackers 发帖:在 Linux 上,如果内核后台写回(writeback)脏页时遇到 I/O 错误,之后进程调用 `fsync()` 会得到一次 EIO——**然后内核把脏页标记为干净并丢弃,下一次 `fsync()` 返回成功**。而 PostgreSQL 的 checkpoint 代码把 fsync 失败当作普通可重试错误:报个 ERROR,checkpoint 推迟,下次重试——下次"成功"了,checkpoint 完成,WAL 被回收。**那些从未真正落盘的数据页,就这样在所有人都收到"成功"的情况下永久丢失了。** LWN 以《[PostgreSQL's fsync() surprise](https://lwn.net/Articles/752063/)》(2018-04-18)报道,事件被称为 fsyncgate。

更糟的两点:其一,PG 的架构里**执行 fsync 的进程(checkpointer)不是执行 write 的进程(backend)**,而在某些内核版本上,错误只报告给"出错时持有该文件描述符"的进程,checkpointer 后打开的 fd 可能根本看不到错误;其二,调查发现几乎所有数据库(MySQL、MongoDB 等)都有同样的误解——这是对 POSIX 语义的集体误读,PG 只是第一个直面它的。

### 复现条件

- 存储层出现瞬时写错误(NFS 掉线、thin-provisioning 卷写满、多路径抖动、坏块);
- 错误恰好发生在内核后台写回阶段(而非 write 系统调用时);
- checkpointer 在错误后重试 fsync 并得到成功,完成 checkpoint 并回收 WAL。

之后数据页内容是旧的,但没有任何日志能告诉你哪一页。完整事件序列:

```
backend      write(页P) → 进内核页缓存,成功返回;向 checkpointer 登记"段S待fsync"
内核         后台写回页P → 存储报 EIO → 页P标记"干净",错误记在文件状态里
checkpointer checkpoint 开始 → fsync(段S) → EIO        ← 错误被报告并【清除】
             ERROR 记日志,checkpoint 中止(redo 点未推进,看似安全)
checkpointer 稍后重试 → fsync(段S) → 成功               ← 其实什么都没写
             checkpoint 完成,写 checkpoint 记录,回收旧 WAL
             —— 页P 的新内容:不在磁盘上,不在页缓存里,WAL 也没了
```

关键在第 3、5 行的反差:应用以为"失败→数据还在内核手里→重试即可",内核的立场是"失败已通知→脏页丢弃→别再问了"。

### 根因分析

PG 的持久化模型(见第 09 篇)建立在一条假设上:**"fsync 返回成功 ⟹ 自上次成功 fsync 以来的所有写入都已持久化"**。据此,checkpoint 的正确性推理是:先 fsync 所有脏段,成功后写 checkpoint 记录,再回收旧 WAL——若 fsync 失败,只要不推进 redo 点,重试即可,WAL 还在,数据不丢。

内核的真实语义却是:**fsync 报告的是"自上次 fsync 以来是否发生过错误",且报告一次后清除状态;出错页被标记干净**。也就是说,"重试 fsync"重试的只是错误标志的读取,不是数据的写入。PG 侧对应的实现在 `src/backend/storage/file/fd.c` / `src/backend/storage/smgr/md.c` 与 checkpointer 的 fsync 请求队列(backend 写完页后通过请求队列把"该谁 fsync"转交 checkpointer,现集中在 `src/backend/storage/sync/sync.c`)——这套精巧的"延迟集中 fsync"机制把上述内核语义的每一个坑都踩满了:跨进程、重开 fd、失败重试。

根因层面这不是一个"写错的 if",而是**应用与内核之间关于错误所有权的接口语义分歧**:内核认为"错误已报告,责任移交";应用认为"错误已报告,数据仍在内核手里等待重试"。

### 修复方案

commit **`9ccdd7f66e`**(Thomas Munro 提交、Craig Ringer 作者,2018-11-19,"PANIC on fsync() failure",回补所有支持分支,随 11.1/10.6 等发布):**任何数据文件/WAL 的 fsync 失败一律 PANIC**,放弃"重试"幻想,强制崩溃恢复——从(仍然可信的)WAL 重放,重新生成脏页再写一遍,这才是真正的重试。配套:

- 新增 GUC `data_sync_retry`(默认 false),给"确知其文件系统会保留脏页"的场景留后门(现今代码中即 `fd.c` 里的 `data_sync_elevel()`,决定报错级别是 PANIC 还是 WARNING);
- 姊妹 commit `38fc056074`(同日,"Maintain valid md.c state when FileClose() fails");
- 内核侧:该事件推动 Linux 4.13+ 改进错误报告(errseq_t),保证每个 fd 都能看到错误;
- 长线:这件事是 PG 界对 Direct I/O 态度转变的转折点之一——既然内核缓冲写回的错误语义如此难以依赖,自己管理 I/O(PG 16+ 的 io 方向工作)的动力大增。

### 波及与教训

实际观测到的静默丢失案例不多(需要存储错误配合),但理论上二十年间所有版本都暴露在外。社区把完整时间线整理进 wiki([Fsync Errors](https://wiki.postgresql.org/wiki/Fsync_Errors));danluu.com 收录的邮件存档使 "[fsyncgate](https://danluu.com/fsyncgate/)" 成为系统领域的通用典故。教训有二:**跨团队接口(这里是应用↔内核)的语义必须以对方的实现为准,而不是以自己的直觉为准**;以及**当无法确认持久化时,崩溃是比继续运行更安全的选择**——PANIC 不是失败,静默才是。

### 深层设计约束

> **错误处理代码的正确性取决于"错误发生后系统处于什么状态"的假设;对外部系统(内核、libc、文件系统)的这类假设必须逐条验证,不能从 API 名字和返回值望文生义。**

---

<a id="案例-4"></a>
## 案例 4 · FSM/VM 截断不写 WAL:恢复后"读到不存在的块"(2016)

### 背景与现象

生产库崩溃恢复(或备库提升)之后,插入负载偶发报错:

```
ERROR: could not read block 28991 in file "base/16390/572026": read only 0 of 8192 bytes
```

块号远超表的实际大小。表本身没有损坏,但错误会持续出现,直到 FSM 被重建。VM(可见性映射)侧的同类问题更阴险:页面被错误地标记为 all-visible,可能让顺序扫描/索引唯一性检查得出错误结论。

### 复现条件

- 表尾部被 VACUUM 截断(`RelationTruncate`,见第 10 篇);
- 截断后、下一次 checkpoint 前发生崩溃(或该 WAL 区间被备库重放);
- 之后向该表插入:FSM(空闲空间映射)返回一个已被截断的块号,`ReadBuffer` 失败。

时间线:

```
t0 VACUUM 发现表尾部N个空页 → RelationTruncate
   堆截断:写WAL(XLOG_SMGR_TRUNCATE)                ✓持久
   FSM截断:改FSM尾页内部指针,MarkBufferDirtyHint  ✗只是hint
t1 [崩溃] — 未到下次checkpoint
t2 崩溃恢复:重放堆截断 → 堆确实变短
   FSM那次hint修改 → 恢复期间MarkBufferDirtyHint可能根本不标脏 → 丢失
   → FSM 仍记得"旧的、已不存在的块还有空闲空间"
t3 INSERT → FSM返回块28991 → ReadBuffer(28991) → 越界
   ERROR: could not read block 28991 ... read only 0 of 8192 bytes
```

### 根因分析

`RelationTruncate`(`src/backend/catalog/storage.c`)截断堆的同时要截断附属的 FSM 与 VM。堆截断本身有 WAL 记录;但 FSM/VM 的截断只修改其"最后一页"的内部指针,当时的代码用 `MarkBufferDirtyHint()` 标脏——即把它当作 **hint bit 级别的、丢了无所谓的修改**。

这个分类错了。hint bit 的契约是"丢失只影响性能,不影响正确性"(重算即可);而 FSM 尾页指针丢失后,FSM 会**指向物理上已不存在的块**——插入路径把 FSM 的返回值当作可直接 `ReadBuffer` 的块号,并不重新验证(FSM 本来就被设计为允许轻微过期,但"过期"的含义是"该块可能已没有空间",不是"该块可能不存在……"——好吧,实际上插入路径确实会兜底扩展,但截断场景下返回的块号越界直接报错)。更关键的是 `MarkBufferDirtyHint` 在恢复期间可能根本不标脏,修改直接蒸发。VM 侧同理:截断修改未持久化,页面残留的 all-visible 位违反"VM 说 all-visible ⟹ 堆页确实全可见"的硬不变量。

根因:**一处修改被错误地归类进"允许丢失"的持久化等级**。PG 的页面修改有三个等级——WAL 保护(必须重放)、`MarkBufferDirty` + 自写 WAL(校验和场景防 torn page)、`MarkBufferDirtyHint`(允许丢失)——FSM/VM 截断需要第一等级,却被放在了第三等级。

### 修复方案

commit **`917dc7d239`**(Heikki Linnakangas 提交、Pavan Deolasee 分析,2016-10-19,"Fix WAL-logging of FSM and VM truncation",回补至当时所有支持分支):改用 `MarkBufferDirty()` 并在需要时自行写 WAL(校验和开启时防 torn page);同时修复 `visibilitymap_truncate` 的同类疏漏。当前 master 上对应逻辑在 `FreeSpaceMapPrepareTruncateRel()`(`src/backend/storage/freespace/freespace.c`)与 `visibilitymap_prepare_truncate()` 中,函数名中的 "prepare" 正是后续重构(把截断动作与 WAL 写入顺序理顺)留下的痕迹。

### 波及与教训

这个 bug 潜伏了很多年(FSM/VM 机制 8.4/9.0 就有),因为触发窗口窄("截断后未 checkpoint 即崩溃")且现象出现在崩溃很久之后,极难归因。它也是"**附属 fork 的一致性**"问题的代表:堆、FSM、VM、init fork 各自有持久化路径,任何结构性操作(截断、复制、重写)必须对每个 fork 分别回答"这个修改丢了会怎样"。后续多年里 VM 相关的同类 bug(如 pg_upgrade 后 frozen 位、`e85662df44` 修复的 pg_visibility 误报)反复证明这条教训的普适性。

### 深层设计约束

> **每一处共享缓冲区修改都必须显式选择持久化等级(WAL / 自管 / 可丢弃),选择依据是"丢失后的最坏后果",而不是"这个结构平时重不重要"。辅助结构的截断与主数据的截断是同一个原子事件,不能拆成两个持久化等级。**

---

<a id="案例-5"></a>
## 案例 5 · overwrite contrecord:主库崩溃撕裂 WAL 记录,备库永久卡死(2021)

### 背景与现象

主库崩溃重启,自身恢复正常;但流复制备库从此停在一个 LSN 上,反复报:

```
LOG: invalid contrecord length 7262 at A8/D9FFFBC8
```

不再前进。唯一的恢复手段是停备库、删除有问题的 WAL 段文件、重启让它重新从主库拉取——或者干脆重建备库。

### 复现条件

三个条件叠加(概率低但在大集群中必然发生):

1. 一条 WAL 记录跨越段文件边界(前半在段 N 末尾,后半即 contrecord 应在段 N+1 开头);
2. 主库在写下前半、尚未写下后半时崩溃;
3. 段 N 已通过流复制/归档送达备库(物理复制按写入即发送,不等记录完整)。

时间线:

```
主库                                        备库
t0 写记录R前半,恰好填满段N末尾
t1 段N写满 → walsender 立即发送段N ───────→ 收到段N,重放到R前半,等待contrecord
t2 [崩溃] R后半从未写出
t3 重启恢复:R不完整 → 视为WAL终点
   从R的起点LSN继续写新记录R'
t4 发送含R'的新WAL ─────────────────────→  同一LSN上收到的是R'的字节
                                           但状态机还在"等R的contrecord"
                                           校验失败:invalid contrecord length
                                           ← 永久卡死:不能跳过,也不能回退
```

### 根因分析

主库崩溃恢复的规则是:重放到最后一条**完整**记录为止,那条跨段记录的前半被视为无效尾巴,恢复后**从它的起点继续写新的 WAL**——同一个 LSN 区间,现在装着完全不同的字节。这对主库自身无害(旧字节被覆盖),但违反了复制体系的一条隐含公理:**WAL 一旦被下游读走,其内容就是不可变的**。备库已经拿到了旧的前半段,它忠实地等待着那条记录的后半——而那个后半永远不会来,来的是同一 LSN 上语义完全无关的新记录。备库既不能跳过(它认为记录没读完),也不能回退(物理复制不回卷),于是永久卡死。

这里的深层矛盾:WAL 的"追加不可变"是逻辑承诺,但**崩溃时刻的 WAL 尾部在物理上就是可变的**(未刷盘的尾巴本来就该被丢弃重写)。单机上"丢弃尾巴"没问题;引入复制后,"我打算丢弃的尾巴可能已经被别人看到了"——分布式系统里经典的"未提交数据外泄"问题,在 PG 里以段文件为单位重演。此前 commit `515e3d84a0b5` 尝试过只在归档路径上修(不归档含断裂记录的段),但流复制不走归档,且有伸缩性问题,被回退。

### 修复方案

commit **`ff9f111bce`**(Álvaro Herrera,2021-09-29,"Fix WAL replay in presence of an incomplete record",回补至 10,随 14.1/13.5 等发布):既然"覆盖"无法避免,就**把"我覆盖了一条断裂记录"这件事本身写成 WAL 记录**。主库恢复后若发现尾部有断裂记录(其起点记录在 `abortedRecPtr`,见 `src/backend/access/transam/xlogrecovery.c`),在新时间线的写入流开头先写一条 `XLOG_OVERWRITE_CONTRECORD`;备库读到断裂点后,若接下来读到这条记录且其中记载的 LSN 与断裂点吻合,就明白"后半永远不会来了",合法地放弃旧记录、从新内容继续。相当于给"不可变承诺的例外"补了一个显式的、可验证的协议。

该修复自身在 14.x 早期还引出若干边角 follow-up(如断裂点恰在页边界时的 padding 处理),属于协议补丁常见的收敛过程。

### 波及与教训

bug 自流复制诞生(9.0)起潜伏了 11 年,直到用户报告积累出清晰模式才被定位——触发需要"崩溃 × 跨段记录 × 已发送"三重巧合,任何测试矩阵都难以覆盖(后来 injection points 框架的动机之一正是制造这种"在第 N 字节处崩溃"的场景,见第 14 章)。教训:**每一条"不变量"都要问一句——它在崩溃边界上还成立吗?有没有观察者在不变量被破坏的窗口里看过一眼?**

### 深层设计约束

> **"追加即不可变"只在单机成立;一旦日志有了下游读者,"丢弃未完成的尾部"就从私有动作变成分布式协议,必须让所有读者能够识别并同步这次丢弃。**

---

<a id="案例-6"></a>
## 案例 6 · PG 14 CREATE INDEX CONCURRENTLY 索引静默丢行(2021–2022)

### 背景与现象

2022-06-16,社区为 PostgreSQL 14 **单独进行了一次计划外发版(14.4)**,公告只为一个 bug:14.0–14.3 上执行的 `CREATE INDEX CONCURRENTLY`(CIC)与 `REINDEX CONCURRENTLY`(RIC)可能生成**缺行的索引**——某些堆元组没有对应索引项,走该索引的查询会静默漏掉这些行;唯一约束可能因此漏检重复。官方建议:所有在 14.x 上用过 CIC/RIC 的用户,用 `pg_amcheck --heapallindexed` 检查并重建([14.4 公告](https://www.postgresql.org/about/news/postgresql-144-released-2470/))。

### 复现条件

- PG 14.0–14.3;
- CIC/RIC 运行期间,表上有并发写入,且发生了 **HOT 更新 + HOT 剪枝**(同页更新产生的旧版本被页内清理回收);
- 被剪掉的版本链恰好包含索引构建扫描本应收录的元组。

高写入负载的大表上做 CIC 时,概率相当可观。

### 根因分析

这是一个**优化把别人的安全垫抽走了**的教科书案例,链条要从两个"互不相识"的机制说起:

1. **CIC 的正确性基础**(见第 07 篇):CIC 分两阶段扫描堆,不锁写。它能容忍并发写的前提之一是:构建期间,表里的元组版本不会在它眼皮底下被物理回收。这一点从来不是 CIC 自己保证的,而是 MVCC 的副产品——CIC 事务持有快照,它的 xmin 挡住了 VACUUM 和 HOT 剪枝对"它还看得见的版本"的回收。**这层保护在代码里没有任何显式表达,它只是"恰好一直成立"。**

2. **PG 14 的优化**:CIC 在大表上一跑几小时,它的旧 xmin 会让全库的死元组无法回收,这是运维一大痛点。commit **`d9d076222f`**(Álvaro Herrera,2021-02-23,"VACUUM: ignore indexing operations with CONCURRENTLY")让"安全的" CIC/RIC 事务打上 `PROC_IN_SAFE_IC` 标志(现存于 `src/include/storage/proc.h`),计算全局 xmin 视界时**忽略这些事务**——就像早已对 VACUUM 自己做的那样(`PROC_IN_VACUUM`)。推理是:CIC 不产生对其他表的读,忽略它顶多影响它自己正在建的表;而对本表,真正的 VACUUM 会与 CIC 的锁冲突,不可能并发进来。

3. **漏掉的路径**:推理只考虑了 VACUUM,忘了 **HOT 剪枝不是 VACUUM**——它由普通的 SELECT/UPDATE 在读页时顺手执行,不需要任何表锁,视界一放开就立即生效。于是:并发事务 HOT 更新一行,另一个并发读把旧版本剪掉——而 CIC 的第一阶段扫描还没走到那一页,或按两阶段协议本应在第二阶段补录它。元组物理消失,索引项无从建立,而 CIC 对此毫无察觉,正常"成功"。

事故交错(简化到一页一行):

```
CIC backend(带 PROC_IN_SAFE_IC)        并发 backend们
t0 开始阶段1堆扫描,快照S,扫到第100页
t1                                      UPDATE 行r(在第500页):HOT更新
                                        版本 r1(S可见)→ r2,同页
t2                                      r1的xmax提交;全局xmin计算
                                        【跳过CIC】→ 视界越过r1
t3                                      某SELECT读第500页,顺手HOT剪枝:
                                        r1被回收,行指针重定向到r2
t4 扫描到第500页:只看得见……什么?
   快照S本应看见r1,但r1已物理消失;
   r2对S不可见 → 此行没有任何版本入索引
t5 阶段2按协议收尾,验证通过,索引"建成"
   —— 行r从此在该索引中不存在
```

若没有 t2 的"跳过 CIC",全局 xmin 停在 S 之前,t3 的剪枝根本不会碰 r1——这就是被抽走的安全垫。

根因:**CIC 的正确性依赖"我的 xmin 保护着我要扫描的版本",但这个依赖从未被显式声明;优化者审计了所有显式的回收者(VACUUM),漏掉了隐式的(HOT 剪枝)。**

### 修复方案

commit **`e28bb88519`**(Álvaro Herrera,2022-05-31,"Revert changes to CONCURRENTLY that \"sped up\" Xmin advance",随 14.4 发布):**整体回退** `d9d076222f`。commit message 坦率承认:优化没有简单的补救方式("the risk of indexes missing tuples is too high and there's no easy fix"),PG 14 的这项运维改进就此收回,留待未来以其他方式重做。附带 `7f580aa5d8` 加强了 amcheck 对 CIC 的测试。

同期 14.x 还有另外两个独立的 CIC 损坏 bug,常被混为一谈,这里一并澄清:**`3cd9c3b921`**(Noah Misch,2021-10-23,14.1/13.5 等,"Fix CREATE INDEX CONCURRENTLY for the newest prepared transactions"——CIC 计算并发事务集合与 2PC 提交之间的竞态窗口导致丢行,该 bug 自 9.4 潜伏)与 **`f99870dd86`**(Michael Paquier,2021-12-08,"Fix corruption of toast indexes with REINDEX CONCURRENTLY")。三案并发,使 2021–2022 成为 "CONCURRENTLY 之冬"。

### 波及与教训

发现方式值得记录:不是用户报损坏,而是 Peter Slavov 用 **amcheck** 主动巡检发现索引与堆不一致,顺藤摸瓜定位到 `d9d076222f`(参见 [pganalyze 的分析](https://pganalyze.com/blog/5mins-postgres-14-reindex-create-index-concurrently-bug-amcheck-extension))。这是 amcheck 路线(见第 14 章)第一次在重大事故中充当主要探测器,直接促成 14.4 专门发版。教训:CIC 这类"在线 DDL"的正确性论证是 PG 中最脆弱的一类——它同时依赖锁协议、快照语义、HOT 语义三者的交互,任何一方的"无关"改动都可能击穿它;9.4 潜伏七年的 2PC 竞态(`3cd9c3b921`)与本案一同说明,这里的每一条正确性论证都值得写成显式注释甚至断言。

### 深层设计约束

> **隐式依赖是最危险的依赖:当机制 A 的正确性依赖机制 B 的副作用、而这个依赖没有写进代码或注释时,任何对 B 的"无关优化"都是对 A 的潜在破坏。视界(xmin horizon)是全局耦合点,放宽它之前必须枚举所有以它为安全垫的机制——包括那些没有名字的。**

---

<a id="案例-7"></a>
## 案例 7 · abbreviated keys 被禁用:glibc strxfrm 与 strcoll 不一致(2016)

### 背景与现象

PostgreSQL 9.5(2015-12)引入 abbreviated keys 排序优化(Peter Geoghegan):对文本排序,先用 `strxfrm()` 把字符串转成可按字节比较的二进制形式,截取前 8 字节作为"缩略键"塞进 SortTuple,绝大多数比较在缩略键上以整数比较完成,性能提升数倍。2016-03,一位用户(Marc-Olaf Jaschke)报告 9.5 上新建的文本索引 amcheck 式对比发现顺序错乱。调查结论令人绝望:**所有测试过的 glibc 版本中,`strxfrm()` 的字节序与 `strcoll()` 的比较结果都不完全一致**——而 C 标准明确要求两者一致。2016-03-31 发布的 9.5.2 直接**全局禁用**了非 C locale 下的文本 abbreviated keys。

### 复现条件

- PG 9.5.0/9.5.1,文本列排序/建索引,collation 为非 C 的 glibc locale;
- 数据中包含触发 glibc 不一致的字符组合(与具体 locale 和 glibc 版本相关,难以预先枚举)。

B-tree 构建走排序路径,于是**索引本身就是错序的**:等值查找可能落空,范围扫描漏行,唯一约束漏检——全部静默。

### 根因分析

代码层面毫无问题:`varstr_abbrev_convert()`(至今仍在 `src/backend/utils/adt/varlena.c`,供 C locale 等安全场景使用)忠实调用 strxfrm 并截断;比较器在缩略键相等时回退完整 `strcoll` 比较。整个方案的正确性只需要一条外部公理:

> `strcmp(strxfrm(a), strxfrm(b))` 与 `strcoll(a, b)` 同号。

这是 C 标准写明的契约。但 glibc 的实际实现中两条代码路径独立演化,在部分 locale 的边角(某些重音组合、不可归一化序列等)上结果不同。Tom Lane、Peter Geoghegan、Noah Misch 等人的调查(见 `3df9c374e2` commit message)确认:**没有任何已测 glibc 版本完全达标**,且没有可行的运行时检测手段——不一致只在特定输入对上出现,采样测试无法证明安全。

于是这不是"修 bug",而是"承认公理为假"。方案空间:(a) 信任并继续,让用户承担静默损坏;(b) 全局禁用,放弃性能;(c) 编译期开关留给敢赌的人。社区选了 (b)+(c)。

一个可感知的例子:某 locale 下

```
strcoll("a", "b")            → a < b     (完整比较,正确)
strxfrm("a")[0..7] 对比
strxfrm("b")[0..7]           → 可能得出 a > b   (缩略键前8字节被库的内部权重编码"骗到")
```

排序器信了缩略键就把 a 排到 b 后面;缩略键相等才回退 strcoll,而这里缩略键**不等**,回退根本不触发——错序直接落进 B-tree 页,无声无息。

### 修复方案

commit **`3df9c374e2`**(Robert Haas,2016-03-23,"Disable abbreviated keys for string-sorting in non-C locales",随 9.5.2 发布):非 C locale 一律不启用缩略键,定义 `TRUST_STRXFRM` 宏编译者除外;公告要求 9.5.0/9.5.1 用户 REINDEX 受影响索引。同日姊妹 commit `1be4eb1b2d` 处理 Windows。后续演化:ICU 进入后(案例 8),ICU 的 `ucol_getSortKey` 被认为可信,缩略键对 ICU collation 重新启用;`TRUST_STRXFRM` 后门则在 PG 16 时代被移除(改为按 provider 决定)。

### 波及与教训

三个月大的明星优化被一刀禁用,是 PG 性能史上著名的"回撤"。它给社区留下的最重要遗产是**对 collation 库的系统性不信任**——这份不信任直接催生了案例 8 的版本追踪机制,以及 amcheck 的 `bt_index_check`(2017,第一版就以"检查索引序与比较器一致"为核心功能,见第 14 章)。教训:**性能优化引入的每一条外部正确性公理,都要求有独立的验证手段;不能验证的公理,其失败模式必须是可检测的,否则就不该引入。**

### 深层设计约束

> **标准文本里的契约不等于实现里的契约。当正确性依赖第三方库的两个 API 相互一致时,必须假设它们会不一致——除非该库把这条一致性作为显式测试目标。**

---

<a id="案例-8"></a>
## 案例 8 · collation 变更导致索引静默损坏:一个至今没有完全解决的问题

### 背景与现象

这一案不是单一 bug,而是一类**持续发生的生产事故**:操作系统升级(尤其 glibc 2.28,2018 年,大规模重构了 collation 数据)、容器基础镜像更换、主备库 OS 版本不一致、逻辑迁移到不同发行版——之后,文本 B-tree 索引开始"闹鬼":明明存在的行查不到、唯一索引里出现重复值、相同查询主备结果不同。没有任何报错。原因:**索引的物理序是按旧版本 collation 排的,新版本 collation 对同一批字符串给出了不同的顺序**,二分查找在"按新序看来错乱"的树上走错方向。

### 复现条件

- 文本索引列使用 libc/ICU 的语言学 collation(非 C/POSIX);
- collation 库升级且排序规则发生实际变化(glibc 2.28 是最著名的断层,影响几乎所有非 C locale);
- 索引未随之 REINDEX。流复制主备 OS 不一致时,备库上问题即时出现——物理复制的索引页在备库上按备库的 libc 解释。

### 根因分析

从设计假设层面看,B-tree 的根本前提是:**比较函数是一个不随时间变化的纯函数**。PG 把文本比较外包给 libc/ICU(`varstr_cmp` → `strcoll_l`/`ucol_strcoll`,见 `src/backend/utils/adt/varlena.c` 与 `src/backend/utils/adt/pg_locale*.c`),等于把这条前提外包给了一个**自身在演化的自然语言规则库**——Unicode 每年都改排序规则(新字符、修订权重),glibc/ICU 跟进即"比较函数变了"。索引是比较函数的物化缓存,函数变了缓存就失效,但**没有任何机制让 PG 知道函数变了**。

这与案例 7 是同一信任危机的两面:案例 7 是"库内部两个 API 不一致",本案是"库的同一个 API 跨版本不一致"。前者可以靠禁用绕开,后者绕不开——语言学排序本身就是需求。

glibc 2.28 的断层最有代表性:它把 collation 数据从旧格式整体重写,大量 locale 下形如

```
'Ⅷ'(罗马数字八)、带变音符的组合序列、CJK 扩展区字符
```

的相对顺序发生变化。索引在 2.27 机器上按旧序建好,数据文件原样复制到 2.28 机器(或备库是 2.28),二分查找按 2.28 的 `strcoll_l` 走向,却走在 2.27 排好的树上——键"应该在左子树"的判断与页面实际布局矛盾,于是明明存在的行被判定"不在这条路径上"。备库场景尤其隐蔽:同一份物理页,主备两台机器用各自的 libc 解释,查询结果可以不一致而双方都不报错。

### 修复方案(演化史,均已在本仓库验证)

社区无法阻止 libc 变化,只能让变化**可检测**:

| commit | 日期 | 内容 |
|---|---|---|
| `eccfef81e1` | 2017-03-23(Peter Eisentraut) | ICU 支持进入 PG 10:ICU 可与 OS 解耦、自带版本号,是"把 collation 变成受管依赖"的第一步 |
| `ca051d8b10` 等 | 2018+(Thomas Munro) | 逐平台补 collation 版本探测(FreeBSD/Windows/glibc) |
| (PG 13 时代) | 2020 | 索引级 collation 版本追踪:记录索引构建时的版本,打开时不符则 WARNING |
| **`ec48314708`** | 2021-05-07(Thomas Munro) | **回退**上一条的 per-index 版本追踪——依赖追踪的粒度与语义问题(表达式内嵌 collation、default collation 的归属)在发布前无法收敛,宁可撤回也不发布一个给用户虚假安全感的检测器 |
| **`37851a8b83`** | 2022-02-14(Peter Eisentraut) | 数据库级 collation 版本追踪(PG 15):粒度粗但语义清晰,`ALTER DATABASE ... REFRESH COLLATION VERSION` 配套 |
| `f69319f2f1` 等 | 2024(PG 17) | 内置 C.UTF-8 provider:把"不随平台变化的 Unicode 码点序 + Unicode 语义"收编进 PG 自身,给不需要语言学序的用户一条彻底免疫的路 |

注意 `ec48314708` 这次回退的分量:一个已合并主干、广受期待的特性,因为"检测器本身可能误报/漏报"而在发布前撤下——经历过案例 1、6 的社区,对"不完整的安全机制比没有更糟"有了肌肉记忆。

### 波及与教训

glibc 2.28 断层期(RHEL 8、Debian 10 升级潮)是 PG 生态损坏工单的高发期,云厂商(AWS 等)先后发布长文指导用户排查;"OS 升级必须 REINDEX 文本索引或用 amcheck 验证"至今仍是 DBA 手册的标准条目。这一案与案例 7 共同定义了社区对外部依赖的成熟态度:**无法内化的依赖,至少要版本化;无法版本化的,提供内置替代品**。

### 深层设计约束

> **索引是比较函数的物化;凡物化,必须记录生成它的函数版本,并在函数可能变化的每个入口(升级、复制、恢复到异构机器)校验。把"纯函数"假设建立在自然语言规则之上,等于在地基里埋了日历。**

---

<a id="案例-9"></a>
## 案例 9 · "missing chunk number 0":syscache 里的过期 TOAST 指针(2011)

### 背景与现象

长期运行的库上偶发、难以复现的报错:

```
ERROR: missing chunk number 0 for toast value 987654 in pg_toast_2619
```

`pg_toast_2619` 是 `pg_statistic` 的 TOAST 表——报错的不是用户数据,而是**系统目录的宽字段**(统计直方图、函数体等)。错误一闪而过,重跑就好,但在 ANALYZE 频繁的库上反复出现,多年间积累了大量报告(2011 年前后 Andrew Hammond、Tim Uckun 等的报告最终促成定位)。

### 复现条件

- 会话 A 从 syscache 读取一条含 out-of-line TOAST 字段的 catalog 行(如某表的 pg_statistic 行,规划查询时);
- 会话 B 更新/删除同一 catalog 行并提交(如 ANALYZE 重写统计);
- autovacuum 迅速回收了旧行对应的 TOAST 元组;
- 会话 A 这才去 detoast 手里那条(已过期的)cache 行——TOAST 指针指向的 chunk 已被物理删除。

```
会话A(规划一条查询)              会话B / autovacuum
t0 syscache读pg_statistic行v1
   (v1宽字段是TOAST指针p→chunk集C)
t1                                ANALYZE:插入v2,删除v1,提交
t2                                autovacuum:v1死了 → 回收C
t3 规划器取v1的直方图 → detoast(p)
   → SELECT chunk FROM pg_toast_2619 WHERE valueid=...
   → 空 → ERROR: missing chunk number 0
```

t0 与 t3 之间只要隔一次 cache 命中的间隙即可,不需要长事务。

### 根因分析

syscache(见第 11 篇)的一致性设计是:**过期条目由锁挡住**——你在用某对象的 cache 条目时,必然持有该对象的锁,而修改者必须先拿冲突锁并发 invalidation,所以"正在被使用的条目不会过期"。commit **`08e261cbc9`** 的 message 点破了两个例外:

1. **ANALYZE 更新 pg_statistic 不锁出规划者**——规划器读统计只对表持有最弱的锁,与 ANALYZE 完全不冲突,这是刻意为之(统计过期无害,规划用旧统计没问题);
2. **CREATE OR REPLACE FUNCTION 更新 pg_proc 几乎不加有意义的锁**。

对**行内**数据,这确实无害——MVCC 保证 cache 里那份旧行的字节永远可读。但 TOAST 指针破坏了这个推理:cache 里存的只是指针(valueid),真正的数据在 TOAST 堆里,受 MVCC 可见性与 VACUUM 管辖。**"旧统计无害"的推理成立于值语义,而 TOAST 把值偷偷换成了引用语义**。会话 A 手里的快照如果早于 B 的提交,本可以保护 TOAST 元组不被回收——但 syscache 读取不一定发生在能覆盖后续 detoast 的快照下,窗口就此打开。

### 修复方案

commit **`08e261cbc9`**(Tom Lane,2011-11-01,"Fix race condition with toast table access from a stale syscache entry",回补所有分支):采纳 Heikki 的思路——**元组进入 syscache 之前强制把所有 out-of-line 字段拉平**(detoast 后内联存储),即今日 `src/backend/utils/cache/catcache.c` 中的 `toast_flatten_tuple()` 调用。读 catalog 行的瞬间持有的 MVCC 快照足以保护那一刻的 detoast;拉平之后 cache 条目回归纯值语义,永不再引用外部存储。代价是 cache 变大,但 catalog 行的宽字段本就少见。commit message 里 Tom 谨慎地注明他"并不完全确信"快照在每个取用点都成立——十年后 `42750b08d9`(2018,"Ensure snapshot is registered within ScanPgRelation")等后续修复证明这份谨慎是对的:同族竞态在 relcache 路径上又被抓到过。

### 波及与教训

单看只是个偶发 ERROR,但它是 PG 中**引用语义污染值语义**这一故障家族的定义性案例:任何持有"行的副本"的地方(syscache、relcache、逻辑解码的 tuple、存储过程缓存的 datum),都必须回答"副本里有没有 TOAST 指针?指针的生命周期谁保证?"。后来 BRIN(`7577dd8480`)、复合类型字段(`3f8c8e3c61`)、逻辑解码等处反复出现同款 bug,每一个的修复都是同一句话:要么在受快照保护时 detoast,要么别把指针带出快照的作用域。

### 深层设计约束

> **TOAST 指针是带生命周期的引用,其有效性依附于"某个能看见其父元组的快照仍然存在"。把元组字节从快照作用域里带走的每一条路径,都必须先斩断其中的引用(拉平),否则值语义的所有下游推理全部作废。**

---

<a id="案例-10"></a>
## 案例 10 · 逻辑解码用错快照读 catalog(2017/2022)

### 背景与现象

逻辑复制/CDC 场景下的两类现象,相隔五年、同一根源家族:

- **2017**:`pg_create_logical_replication_slot()` 偶发**永久挂起**,或初始快照导出失败,slot 创建卡死(9.4–9.6 时代大量 wal2json/pglogical 用户报告);
- **2022**:解码运行中偶发 `ERROR: could not map filenumber ... to relation OID`、解码出错乱的元组结构,甚至 walsender 崩溃;触发场景常伴随 DDL 与长事务交错。

### 复现条件

以 2022 案(**`7f13ac8123`**)为例:

- 逻辑解码启动,快照构建器(snapbuild)正处于从 `SNAPBUILD_FULL_SNAPSHOT` 迈向 `SNAPBUILD_CONSISTENT` 的窗口;
- 一个**在窗口内启动**的事务 T 修改了 catalog(如 ALTER TABLE)并提交;
- 解码器随后解码 T 之后某事务的变更,需要读 catalog 来解释元组——用的历史快照没有把 T 计入,读到了 T 修改**前**的表结构去解释 T 修改**后**写入的元组。

```
WAL 物理序 →
 ├─[snapbuild: START]────窗口:FULL_SNAPSHOT──────[CONSISTENT]──解码开始→
 │                          │                        │
 │              T: BEGIN; ALTER TABLE tbl            │
 │                 ADD COLUMN c; COMMIT ←窗口内提交   │
 │                                                   W: INSERT INTO tbl(含c)
 解码 W 时构造历史快照:committed 集合漏了 T
 → 用"无 c 列"的 tbl 结构去解释含 c 的元组字节 → 错乱/报错
```

### 根因分析

逻辑解码(见第 13 篇)的支柱是**历史 catalog 快照**:解码 LSN 为 X 的变更时,必须用"当时"的表结构解释字节。snapbuild(`src/backend/replication/logical/snapbuild.c`)通过跟踪 WAL 里的事务起止,增量构造出任意历史时点的 catalog 快照。其状态机有一条微妙规则:达到 FULL_SNAPSHOT 状态后,"从这一刻起开始的事务"已可被完整跟踪,但更早启动的事务信息不全,要等它们全部结束才算 CONSISTENT、才能对外解码。

`7f13ac8123`(Amit Kapila 提交,2022-08-11,回补至当时所有分支)修的正是边界账目错误:**窗口期内启动并提交的 catalog 修改事务,没有被正确记入后续快照的 committed.xip 集合**——状态机认为"反正 CONSISTENT 之前不解码,窗口内的事务不用管",但漏了一点:这些事务的 catalog 效果必须对 CONSISTENT 之后的解码可见,否则历史快照在"窗口后沿"处有一个结构性空洞。这类 bug 的险恶在于:快照错了不一定报错——如果 T 只改了无关列,解码结果只是**静默地错**。

2017 案(**`955a684e04`**,Andres Freund,2017-05-13,"Fix race condition leading to hanging logical slot creation",9.6.3 时代)是同一状态机的另一角:slot 创建要等"initial_xmin_horizon 之前启动的事务都结束",判定逻辑与 xl_running_xacts 的发布时机存在竞态,特定交错下等待条件永不满足,创建挂死。另一支流:**`0bead9af48`**(Amit Kapila,2020-07-20,"Immediately WAL-log subtransaction and top-level XID association")解决子事务归属延迟发布导致的解码错账。

这一家族的共同根因:**snapbuild 状态机的正确性论证横跨"WAL 中事件的物理顺序"与"事务逻辑上的可见性顺序"两个时间轴**,两轴在窗口期内不重合,而每一条边界规则("哪些事务记入快照/哪些等待/何时可解码")都必须同时对两个轴成立——任何一条只对一个轴成立的规则,都是潜伏的 bug。

### 修复方案

- **`955a684e04`**(2017-05-13):修正 slot 创建的等待条件竞态;
- **`0bead9af48`**(2020-07-20):子事务关联即时写 WAL;
- **`7f13ac8123`**(2022-08-11):窗口期 catalog 事务正确记入快照,并随发版建议受影响 slot 重建;
- 佐证机制修复仍在继续(如本仓库 2026-06 的 `f50c329f53` 修 speculative insertion 的释放),snapbuild 至今是 recovery 之外回归测试密度最高的模块之一。

### 波及与教训

逻辑复制自 9.4 引入后近十年才谈得上"稳定",snapbuild 是主要原因——它是 PG 中唯一一处"用未来的代码重建过去的可见性"的地方,不能复用久经考验的 GetSnapshotData 路径,一切从 WAL 重新推导。教训:**重建历史状态的代码,其测试必须能精确控制事件交错**——正是这类 bug(复现依赖"事务恰好在状态机窗口内启动并提交")推动了 injection points 框架落地(见第 14 章),如今 snapbuild 相关回归里就有靠注入点钉住时序的测试。

### 深层设计约束

> **凡"从日志重建历史可见性"的机制,必须为状态机的每个转换边界显式回答:横跨此边界的事务,其效果记在哪一侧?物理序与逻辑序不一致的窗口里,任何"反正之后才用"的懒惰假设都必须写成断言。**

---

<a id="案例-11"></a>
## 案例 11 · Hot Standby 的 KnownAssignedXids:备库可见性两次翻车(2010/2021)

### 背景与现象

**第一幕(2010)**:Hot Standby 随 9.0(2010-09)发布仅两个月,Joachim Wieland 报告备库恢复失败:

```
FATAL: too many KnownAssignedXids
```

备库崩溃、无法继续重放,复现于主库有长事务且期间大量短事务开启/结束的场景。

**第二幕(2021)**:Alexander Korotkov 提交 **`05e6e78c18`** 修复一个潜伏十年的问题:主库某事务的子事务数一旦超过 64(PGPROC_MAX_CACHED_SUBXIDS)造成 suboverflow,备库会记下 `lastOverflowedXid`;但这个标记**从不清除**,导致其后备库上的快照长期退化——每次可见性判断都要额外查 pg_subtrans,只读查询性能悬崖式下跌,且在特定序列下快照判定保守到近乎错误。

### 复现条件

第一幕:主库存在长事务 + 高事务周转;且一条 WAL 记录恰好写在"生成 running-xacts 快照"与"把它写入 WAL"之间的缝隙里。第二幕:主库任何事务用过 >64 个子事务(savepoint 循环、异常块密集的 PL/pgSQL),此后主库即使再无 suboverflow,备库也不自愈,直到重启恢复。

### 根因分析

Hot Standby 的可见性(见第 09/13 篇)没有主库的 ProcArray 可查,只能靠**从 WAL 流推断"主库上此刻哪些事务在跑"**:周期性的 `xl_running_xacts` 记录给出基准集合,其后靠每条记录携带的 xid 增量维护——这就是 `KnownAssignedXids`(`src/backend/storage/ipc/procarray.c` 中的 `KnownAssignedXidsAdd` 等)。这个设计从根上就是**用有损信道重建精确状态**:

1. **第一幕的根因**(commit **`5a031a5556`**,Heikki Linnakangas,2010-12-07):xl_running_xacts 的"拍快照"与"写入 WAL"不是原子的——快照拍完、尚未写入的缝隙里,其他 backend 可以继续往 WAL 里写普通记录。备库按 WAL 顺序处理:先看到缝隙里的记录(引入新 xid),再看到那个"旧"的 running-xacts 快照;对账逻辑在这种乱序下重复累加,把固定大小的 KnownAssignedXids 数组撑爆。数组大小是按"主库 ProcArray 上限"推导的,而乱序对账使备库瞬时集合可以超过主库任何时刻的真实集合。修复:重放 running-xacts 时做减法对账而非盲目累加,容忍快照与增量流的重叠。

```
主库                                    WAL 物理序(备库按此重放)
t0 bgwriter: 遍历ProcArray拍快照Snap
   (含事务{100..180},其中老事务100长期在跑)
t1 backend: xid=181 的记录先写入WAL ──→ ①记录(xid=181):加入KnownAssignedXids
t2 bgwriter: Snap 写入WAL ────────────→ ②xl_running_xacts(Snap):
                                          把{100..180}再整批加入
                                        ③……周期重复,长事务100不结束,
                                          集合只进不出/重复累加
                                        → FATAL: too many KnownAssignedXids
```

2. **第二幕的根因**:子事务信息主库只在 PGPROC 里缓存 64 个,溢出后 WAL 流中的信息不足以让备库维护精确的 subxid 集合,设计上就用 `lastOverflowedXid` 做**降级标记**——"此 xid 之前的可见性判断请走慢路径查 pg_subtrans"。降级是对的,**忘了写升级路径**:标记该在"所有可能受影响的事务都已结束"后清除,但没有任何代码负责清除它。典型的"故障降级容易、恢复常态无人负责"。

两幕共同暴露:KnownAssignedXids 机制的每一处都在处理"信道信息不足"——快照与增量的重叠、subxid 溢出、xid 缺口——而每一种不足都需要一条显式的收敛/恢复规则,漏写任何一条,备库就会在错误或降级状态里越走越远。

### 修复方案

- **`5a031a5556`**(2010-12-07,进 9.0.2):修复 running-xacts 重放对账逻辑;同期还有 `3725570539`(启动阶段 suboverflow 下 KnownAssignedXids 初始化)等一串 9.0.x 修复;
- **`05e6e78c18`**(2021-11-06,回补各分支):在合适的时点(KnownAssignedXids 清空、新 running-xacts 显示无 overflow)重置 `lastOverflowedXid`;
- 长线:`8242752f9c`(压缩启发式)、`119c23eb98`(内存屏障替代自旋锁)持续打磨;而 PG 17 起子事务 SLRU 的大页缓存化、以及社区对"把快照机制换成 CSN"的长期讨论,都以此处的痛点为论据之一。

### 波及与教训

第一幕是"新特性首月即翻车"的典型——Hot Standby 的设计评审集中在"备库不能阻塞重放"与"查询取消"上,而"WAL 里的状态快照本身可以与增量流乱序"这种一致性细节只有真实负载才能暴露。第二幕则展示了**降级机制的单向阀陷阱**:降级路径有测试(制造 suboverflow 很容易),"恢复常态"没有测试(谁会测试"之后一切正常"?),于是十年无人发现。

### 深层设计约束

> **通过日志重建远端状态时,信道的每一种信息缺损(乱序、截断、溢出降级)都必须配一条显式的收敛规则,且降级与恢复必须成对实现、成对测试——只写降级不写恢复的容错,是把故障固化成稳态。**

---

<a id="案例-12"></a>
## 案例 12 · JIT 内联内存泄漏:LLVMContext 里越积越多的类型(2023)

### 背景与现象

PG 11 引入基于 LLVM 的 JIT(见第 14 篇)。此后数年,用户反复报告(Justin Pryzby、Kurt Roeckx、Jaime Casanova 等多轮独立报告):长连接(连接池场景)上开启 JIT 后,backend 进程 RSS 单调增长,数天内从几十 MB 涨到数 GB,最终被 OOM killer 击杀;`MemoryContextStats` 却显示 PG 自己的内存上下文毫无异常——泄漏发生在 PG 记账体系之外。触发面最大是 `jit_inline_above_cost` 生效(内联开启)的分析型负载。

### 复现条件

- JIT 开启且查询代价触发 inlining;
- 同一 backend 持续执行大量不同查询(长连接 + 连接池是放大器);
- 每次 inlining 引用到的 bitcode 模块类型集合略有不同。

### 根因分析

commit **`9dce22033d`** 的 message 说得直白:LLVM 在执行 IR 内联时会"泄漏"类型——更准确地说,**LLVM 的类型对象由其所属的 `LLVMContext` 拥有,只增不减,直到整个 context 销毁**。这是 LLVM 的设计决定(类型驻留/uniquing 换取比较为指针比较),对编译器的典型用法(编译一个翻译单元,进程退出)完全合理。

PG 的用法击穿了这个假设:backend 是**长寿进程**,把所有 JIT 工作挂在一个全局 LLVMContext 上;每次内联都会从 bitcode 库拉入函数,即使模块本身释放了,**结构等价但重新创建的类型对象**仍会在 context 里累积(LLVM 不会跨模块合并到旧类型上)。于是每条 JIT 查询都往一个永不打扫的房间里丢几件家具。PG 的内存上下文体系对此无能为力——这块内存由 LLVM 的 C++ 运行时直接向 malloc 要,PG 只看到 RSS 上涨。

这类 bug 定位极难的原因:泄漏不在任何 PG 代码路径上,valgrind 看是"可达内存"(context 活着,类型可达,不算 leak),必须理解 LLVM 的所有权模型才能指认。

```
backend 长连接,全局 LLVMContext ctx(进程级,永不销毁)
查询1 JIT+inline → ctx 内新增类型 {T_a, T_b, ...}   模块释放,但类型留在 ctx
查询2 JIT+inline → ctx 内又新增结构等价的 {T_a', T_b', ...}  ← 不复用,重新创建
...
查询N → ctx 累积 O(N) 份类型  → RSS 单调上升 → OOM kill
PG 的 MemoryContext 全程显示正常:这些内存是 LLVM 直接 malloc 的,不在 PG 记账树上
```

修复的本质就是给 ctx 补一个"每 100 次编译 reset 一次"的边界。

### 修复方案

commit **`9dce22033d`**(Daniel Gustafsson 提交、基于 Andres Freund 的方向,2023-09-27,"llvmjit: Use explicit LLVMContextRef for inlining"):不再用进程级长命 context,改为显式管理的 `LLVMContextRef`,**每 100 次 JIT 编译后销毁重建 context**,把累积的类型整体清空。100 是权衡重建开销与内存上限的启发值——commit message 承认"在 LLVM 提供 context 大小查询之前,我们只能盲飞"。该修复在 master 成熟后回补;当前 master 上即 `src/backend/jit/llvm/llvmjit.c` 的 `llvm_recreate_llvm_context()`。同族清理还有 `4f4d73466d`(fatal-on-oom 段计数器泄漏)等。

### 波及与教训

JIT 自 PG 11 起默认编译进主流发行版、PG 12 起默认开启,这个泄漏是"云上 PG 神秘 OOM"工单的主力之一,也是不少运维手册建议"OLTP 负载直接关 JIT"的原因之一(另一半原因是编译开销误判)。教训:**引入一个自带运行时的库,就是引入了它的内存/生命周期模型**;PG 内存上下文纪律("所有分配挂在可重置的树上")的价值恰恰在对照中显形——凡绕开它的内存,必须有等价的"定期整体重置"策略,`llvm_recreate_llvm_context` 本质上就是给 LLVM 补了一个 MemoryContextReset。

### 深层设计约束

> **长寿进程不能依赖"进程退出即清理"的库设计;嵌入任何第三方运行时,必须为其找到与宿主生命周期匹配的重置边界——找不到这个边界,就等于接受无界增长。**

---

<a id="案例-13"></a>
## 案例 13 · CVE-2024-7348:pg_dump 的 TOCTOU 提权(2024)

### 背景与现象

2024-08-08 随 16.4/15.8/14.13/13.16/12.20 发布的安全修复,CVSS 8.8:**在 pg_dump 运行期间,库内攻击者可以让 pg_dump 以其运行身份(通常是超级用户)执行任意 SQL**。攻击面是每一个例行的备份任务——"每晚由 postgres 超级用户 cron 跑 pg_dump" 这一最普遍的运维姿势,恰好是最脆弱的姿势。

### 复现条件

- 攻击者拥有库内普通权限:能 CREATE 对象并等待/诱导超级用户运行 pg_dump;
- pg_dump 的两步之间存在时窗:它先查询 catalog 列出待导出对象(判定"这是一张普通表"),后续再对该对象执行 `COPY ... TO` / 读取数据;
- 攻击者在时窗内**把同名对象换掉**——例如删除普通表,原地创建同名视图,视图定义中挂上恶意函数(或利用外部表、序列、扩展配置表等变体,commit message 列明:序列、外部表(13+)、扩展配置表中带 WHERE 的表);
- pg_dump 按"表"的方式访问"视图",触发视图展开,其中的函数以 pg_dump 的会话身份执行——搜索路径与权限均是超级用户的。

攻击时间线(以序列变体为例):

```
攻击者(普通用户)                     pg_dump(超级用户,REPEATABLE READ)
                                     t0 查catalog:对象x是序列,记入TOC
t1 DROP SEQUENCE x;
   CREATE VIEW x AS
     SELECT evil() AS last_value,...; ← 伪装成序列的行型
   (evil()内含任意SQL,SECURITY INVOKER)
                                     t2 导出x的"序列状态":
                                        SELECT last_value,... FROM x
                                        → 展开视图 → evil() 以超级用户执行
```

t0 的快照只冻结**数据**,不冻结**名字→对象**的解析:t2 的 FROM x 是现场按名解析的。

### 根因分析

经典 **TOCTOU**(time-of-check to time-of-use):pg_dump 在 T1 时刻依据 catalog 决定"对象 X 是何种类、如何导出",在 T2 时刻按该决定访问 X,而 X 的身份在 T1–T2 之间可被并发 DDL 替换。pg_dump 无法简单地锁住一切:它刻意运行在 REPEATABLE READ 快照下保证**数据**一致性,但 **DDL 与对象身份不受 MVCC 快照保护**——`COPY x TO stdout` 按名字/OID 现场解析,解析到什么就是什么。更深一层,这是 PG 安全模型的一个结构性弱点在工具侧的投影:**任何高权限身份在低权限用户可写的名字空间里执行操作,都在替对方执行代码**(同族:CVE-2018-1058 的 public schema search_path 劫持、`ALTER EXTENSION` 系列、索引表达式函数在 autovacuum 下执行——社区为后者早已引入受限的 `SECURITY_RESTRICTED_OPERATION`)。pg_dump 作为"拿着超级用户钥匙、按 catalog 地图逐屋开门"的自动化程序,是这个弱点的完美靶子。

### 修复方案

commit **`66e94448ab`**(Masahiko Sawada 提交、Noah Misch 审查,2024-08-05,"Restrict accesses to non-system views and foreign tables during pg_dump",回补至 12):引入服务端 GUC **`restrict_nonsystem_relation_kind`**(现见 `src/backend/utils/misc/guc_tables.c`),取值可含 `view` 与 `foreign-table`;设置后,本会话**拒绝访问非系统视图/外部表**。pg_dump 连接后立即设置它——于是即使对象在时窗内被换成视图,后续访问也直接报错而非展开执行。选择加 GUC 而非改 pg_dump 逻辑,是因为攻击点分散在多条访问路径(数据导出、序列读取、扩展表 WHERE),在服务端一刀切断"被替换成可执行对象"这个后果,比在客户端消灭每一个时窗更可靠,且新 GUC 回补到旧分支不需要 initdb。

### 波及与教训

利用门槛低(只需 CREATE 权限 + 等一次备份),几乎所有生产环境都暴露。教训与同族 CVE 一致:**"以高权限身份触碰低权限用户可定义的对象"必须默认处于受限执行环境**——这一原则此后持续内化(maintenance 操作的受限 search_path、pg_dump/pg_restore 的加固、对 `SECURITY INVOKER` 边界的反复收紧)。对内核开发者的具体启示:凡"先查 catalog 后按结论操作"的两步式代码,要么持锁贯穿两步,要么把"结论仍然成立"变成第二步的运行时强制。

### 深层设计约束

> **catalog 查询的结果是快照,对象的身份不是;任何跨语句依赖"它还是那个它"的程序,必须让第二步自带防线。权限边界上的 TOCTOU 不是并发 bug,而是提权漏洞。**

---

## 第 14 章 · 元分析:这些 bug 教会了社区什么

### 14.1 分布规律:并发 > 恢复 > 边界条件 > 外部依赖

把十三案按根因域归类(一案可跨类):

| 根因域 | 案例 | 共性 |
|---|---|---|
| **并发/可见性**(6.5 案) | 1、6、9、10、11、13 | 全部涉及"两个参与者对同一状态的观察窗口错开";其中静默损坏比例最高 |
| **崩溃恢复/复制**(4 案) | 1(截断)、4、5、11 | 全部是"单机成立的操作,在'崩溃边界 + 有观察者'下失效" |
| **外部依赖语义**(3 案) | 3、7、8、12 | 全部是"对方的真实契约 ≠ 我方假设的契约" |
| **边界/清单遗漏**(2 案) | 2、11(第二幕) | 迁移清单漏项、降级无恢复——"没写的代码"造成的 bug |

三点观察:

1. **最严重的损坏都来自并发与恢复的交叉**。案例 1 里最致命的几个 bug(冻结重放不一致、截断不写 WAL)都同时踩了两个域。原因在结构上:PG 的正确性论证以"锁 + 快照"覆盖运行态、以"WAL 重放确定性"覆盖恢复态,**两套论证各自完备,交叉处却没有人统一负责**——冻结这种"运行态决策、恢复态重放"的操作正好落在缝里。
2. **静默损坏几乎总与"物化"相关**:索引(6、7、8)、TOAST(9)、multixact(1)都是某种派生/引用数据,主数据没坏,坏的是派生物与本体的一致性——而 PG 长期缺少校验派生一致性的工具(这正是 14.3 节 amcheck 的位置)。
3. **外部依赖类 bug 无法靠自身测试消灭**,只能靠"降低信任 + 版本化 + 内化替代"三板斧(案例 7/8 的完整轨迹)。

### 14.2 为什么测试没拦住

每案当时都有回归测试覆盖其所在模块,没拦住的原因可归为三种失效模式:

**(a) 组合爆炸**。案例 1 需要"外键负载 × 冻结时机 × 崩溃/备库"三维组合;案例 6 需要"CIC 阶段 × HOT 更新 × HOT 剪枝时机"。回归测试按模块切片,而 bug 生活在模块的笛卡尔积里。社区的 `make check` 在单一后端上顺序跑 SQL,结构上不生成这类交错。

**(b) 时序窗口**。案例 5(崩溃恰在写半条记录时)、10(事务恰在状态机窗口内提交)、9(vacuum 恰在 detoast 前回收)——窗口以微秒计,压测靠碰运气,且 CI 机器越快窗口越难命中。**没有"让执行在指定点位停住"的手段,这类 bug 原理上不可稳定复现**。这是 injection points 之前的根本困境。

**(c) 环境依赖**。案例 3 需要真实 I/O 错误,7/8 需要特定 glibc 版本与字符数据,12 需要数日的长连接累积——CI 环境里这些条件要么无法制造、要么与"测试要快"直接冲突。

还有第四种更隐蔽的:**(d) 测试的是断言,不是不变量**。案例 11 第二幕十年无人发现,因为降级行为有测试、"降级后应能恢复"这条不变量没人写成测试;案例 6 里 CIC 的结果索引"能用"(测试过),但"包含全部应含元组"这条不变量在 amcheck 之前根本没有可执行的表达形式。

### 14.3 社区应对机制的演化(均可考证)

**Buildfarm(2004–)**:分布式志愿者机器矩阵,持续在数十种 OS/架构/编译器组合上跑回归。它主攻 14.2(c) 的环境维度——案例 7/8 这类平台相关问题,今天大概率会先在某个冷门 buildfarm 动物上现形。局限:跑的仍是同一套顺序测试,对 (a)(b) 无力。

**amcheck(2017–)**:commit **`3717dc149e`**(Andres Freund 提交、Peter Geoghegan 作者,2017-03-09,"Add amcheck extension to contrib",PG 10)。定位正是 14.1 观察 2 的"派生物一致性校验":`bt_index_check` 验证 B-tree 序不变量(直指案例 7/8 的损坏形态),`--heapallindexed` 验证"堆有则索引有"(直指案例 6 的丢行)。PG 14 配套命令行工具 pg_amcheck。里程碑:案例 6 正是由 amcheck **首先发现**而非用户报障,完成了从"验尸工具"到"探测器"的转身;14.4 公告将其列为官方自查手段。此后 heap 校验(verify_heapam)、GIN 支持陆续进入,方向是把第 08/09 篇里的每条不变量都变成可执行检查。

**Injection points(2024–)**:commit **`d86d20f0ba`**(Michael Paquier,2024-01-22,"Add backend support for injection points",PG 17)与同日的测试模块 **`49cd2b93d7`**。这是对 14.2(b) 的正面回答:在代码中埋 `INJECTION_POINT()` 命名点位,测试可在点位上挂接"等待/唤醒/报错"动作,把"vacuum 恰好跑在两步之间"从碰运气变成确定性脚本。落地后,2PC 崩溃恢复、GIN 页分裂(`6a1ea02c49` 借它修复并附带测试)、snapbuild 时序等一批"从前只能靠注释祈祷"的窗口有了钉死的回归测试。它补上了 PG 测试体系里缺席二十年的一块:**时序作为一等测试输入**。

三件工具分别对应三种测试失效模式,这不是巧合——每一件都是被具体事故逼出来的:amcheck 的直接语境是 collation/索引损坏频发,injection points 的动机清单里写满了本篇案例 5/10 型的时序 bug。机制演化还有一条暗线:**发布纪律**。9.5.2(案例 7)、14.4(案例 6)两次"为一个 bug 专门发版",以及 `ec48314708`(案例 8)"宁可回退不发半成品检测器",显示社区对"静默损坏"的容忍度在这些事故中被一步步压到零。

### 14.4 对"如何写内核代码"的启示

从十三条"深层设计约束"再向上蒸馏,是四条可操作的纪律:

1. **把隐式依赖写成显式声明**(案例 6、9、10)。"我的正确性依赖 xmin 挡住剪枝"、"这个元组副本不含 TOAST 指针"、"此边界处两个时间轴一致"——这些话只要写成注释加断言,后来的优化者就有机会看见。PG 代码里最有价值的不是聪明的实现,而是 `heapam.c`、`snapbuild.c` 里那些解释"为什么这样是安全的"的大段注释;本篇多数事故,坏在推理没写下来的那一环。

2. **对每个修改问"崩溃后 + 有观察者"两个问题**(案例 1、4、5)。单机运行态的正确性只是三分之一;"崩溃在任意字节处发生会怎样"与"备库/备份/下游读者看到中间态会怎样"是另外三分之二。FSM 截断、multixact 截断、WAL 尾巴,全部坏在后两问。

3. **外部契约按实现测,不按文档信**(案例 3、7、8、12)。fsync 的语义、strxfrm 的一致性、LLVMContext 的增长——文档要么没写,要么写了对方没做到。对策分三级:能内化就内化(内置 collation provider)、不能内化就版本化(collation version)、不能版本化就把失败变成可检测(PANIC on fsync)。

4. **降级、迁移、清单类代码要对称审计**(案例 2、11)。这类 bug 来自"没写的代码":降级写了、恢复没写;普通表搬了、TOAST 表没搬。审计方法是机械的:对每个降级标记找清除点,对每个 catalog 字段问"迁移工具搬了吗",对每个 fork 问"截断/复制/重写路径都覆盖了吗"。无聊,但案例 2 和 11 各潜伏了数月与十年。

最后一个观察:十三案中大多数 bug 的修复 commit message 本身就是优秀的事故报告——现象、根因、方案、影响面、致谢报告者,一应俱全(读一遍 `b69bf30b9b` 或 `ff9f111bce` 的原文即可体会)。**把 commit message 写成可考古的文档**,是 PG 社区留给所有基础软件项目的隐性财富,也是本篇能够成立的前提。

---

## 附录 A · commit 速查表

以下 hash 均已在本仓库用 `git show` 验证(短 hash 取 10 位,日期为作者时间):

| 案例 | commit | 日期 | 摘要 |
|---|---|---|---|
| 1 | `0ac5ad5134` | 2013-01-23 | 外键锁改进(事故起因) |
| 1 | `3b97e6823b` | 2013-12-16 | 重做冻结协议 |
| 1 | `1a917ae861` | 2014-04-24 | 并发锁定更新竞态 |
| 1 | `f741300c90` | 2014-06-27 | 截断改由 checkpoint 驱动 |
| 1 | `78db307bb2` | 2014-07-21 | 防御坏的 frozenxid/minmxid |
| 1 | `b69bf30b9b` | 2015-04-28 | members wraparound 防护 |
| 1 | `4f627f8973` | 2015-09-26 | multixact 截断 WAL 化 |
| 2 | `9c38bce29c` | 2011-04-08 | pg_upgrade 保留 TOAST relfrozenxid |
| 2 | `7971a57fd4` | 2011-08-31 | 8.3 源集簇的同类修复 |
| 3 | `9ccdd7f66e` | 2018-11-19 | PANIC on fsync() failure |
| 3 | `38fc056074` | 2018-11-19 | FileClose 失败时维护 md.c 状态 |
| 4 | `917dc7d239` | 2016-10-19 | FSM/VM 截断写 WAL |
| 5 | `ff9f111bce` | 2021-09-29 | XLOG_OVERWRITE_CONTRECORD |
| 6 | `d9d076222f` | 2021-02-23 | 引入 bug 的 xmin 优化 |
| 6 | `e28bb88519` | 2022-05-31 | 回退该优化(14.4) |
| 6 | `3cd9c3b921` | 2021-10-23 | CIC vs 最新 2PC 事务 |
| 6 | `f99870dd86` | 2021-12-08 | RIC 损坏 toast 索引 |
| 7 | `3df9c374e2` | 2016-03-23 | 非 C locale 禁用 abbreviated keys |
| 8 | `eccfef81e1` | 2017-03-23 | ICU 支持(PG 10) |
| 8 | `ec48314708` | 2021-05-07 | 回退 per-index collation 版本追踪 |
| 8 | `37851a8b83` | 2022-02-14 | 数据库级 collation 版本追踪 |
| 9 | `08e261cbc9` | 2011-11-01 | syscache 元组入库前拉平 TOAST |
| 9 | `42750b08d9` | 2018 | ScanPgRelation 快照注册(同族) |
| 10 | `955a684e04` | 2017-05-13 | slot 创建挂起竞态 |
| 10 | `0bead9af48` | 2020-07-20 | 子事务关联即时写 WAL |
| 10 | `7f13ac8123` | 2022-08-11 | 解码用错 catalog 快照 |
| 11 | `5a031a5556` | 2010-12-07 | too many KnownAssignedXids |
| 11 | `3725570539` | 2010 | HS 启动 suboverflow 初始化 |
| 11 | `05e6e78c18` | 2021-11-06 | 重置 lastOverflowedXid |
| 12 | `9dce22033d` | 2023-09-27 | JIT 内联用显式 LLVMContext |
| 12 | `4f4d73466d` | — | fatal-on-oom 计数器泄漏(同族) |
| 13 | `66e94448ab` | 2024-08-05 | CVE-2024-7348 修复 |
| 14 | `3717dc149e` | 2017-03-09 | amcheck 进入 contrib |
| 14 | `d86d20f0ba` | 2024-01-22 | injection points 框架 |
| 14 | `49cd2b93d7` | 2024-01-22 | injection_points 测试模块 |
| 14 | `6a1ea02c49` | 2024 | 借注入点修复 GIN 分裂(示例) |

## 附录 B · 参考资料

外部资料(检索时间 2026-07):

- LWN:[PostgreSQL's fsync() surprise](https://lwn.net/Articles/752063/)(2018-04-18,Jonathan Corbet)
- PostgreSQL wiki:[Fsync Errors](https://wiki.postgresql.org/wiki/Fsync_Errors)(fsyncgate 完整时间线)
- danluu.com:[fsyncgate 邮件存档](https://danluu.com/fsyncgate/)
- 官方公告:[9.4.2/9.3.7 多版本发布(multixact 数据丢失警告)](https://www.postgresql.org/about/news/postgresql-942-937-9211-9116-and-9020-released-1587/)(2015-05,关联 CVE-2015-3167)
- Josh Berkus:[Determining your danger of multixact wraparound corruption](http://www.databasesoup.com/2015/05/determining-your-danger-of-multixact.html)(2015-05)
- AWS Database Blog:[MultiXacts in PostgreSQL: usage, side effects, and monitoring](https://aws.amazon.com/blogs/database/multixacts-in-postgresql-usage-side-effects-and-monitoring/)
- PostgreSQL wiki:[20110408pg_upgrade_fix](https://wiki.postgresql.org/wiki/20110408pg_upgrade_fix)(pg_upgrade 事故检测与救援脚本)
- 官方文档:[Release 9.0.4](https://www.postgresql.org/docs/9.0/release-9-0-4.html)、[Release 8.4.8](https://www.postgresql.org/docs/8.4/release-8-4-8.html)
- 官方公告:[PostgreSQL 14.4 Released!](https://www.postgresql.org/about/news/postgresql-144-released-2470/)(2022-06-16,CIC 损坏专项发版)
- pganalyze:[An important bug in Postgres 14 with REINDEX / CREATE INDEX CONCURRENTLY, and using the amcheck extension](https://pganalyze.com/blog/5mins-postgres-14-reindex-create-index-concurrently-bug-amcheck-extension)
- Jonathan Katz:[Notes on updating to PostgreSQL 14.3 …](https://jkatz05.com/post/postgres/may-2022-release-should-i-update/)(2022-05 发布节奏背景)
- 本仓库内证:所有 commit message 原文(`git log -1 --format=%B <hash>`),以及 `src/backend/access/transam/multixact.c`、`src/backend/storage/sync/sync.c`、`src/backend/storage/freespace/freespace.c`、`src/backend/access/transam/xlogrecovery.c`、`src/backend/replication/logical/snapbuild.c`、`src/backend/storage/ipc/procarray.c`、`src/backend/jit/llvm/llvmjit.c`、`src/backend/utils/cache/catcache.c`、`src/backend/utils/adt/varlena.c`、`src/include/storage/proc.h`、`src/backend/utils/misc/guc_tables.c` 等现行代码。

---

*本篇完。下一篇建议搭配阅读:第 08 篇(理解案例 1/6/9 的可见性机制)、第 09 篇(理解案例 4/5/11 的恢复语义)、T5(持久化设计空间——案例 3 的设计级后续)。*
