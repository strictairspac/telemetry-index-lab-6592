# gb-4102 · 索引试验锁等待诊断 — 测试报告

日期：2026-11-18
范围：worker 锁等待观测、证据归因、断档未知、封存与前端呈现。

## 1. 需求 → 实现对照

| 需求 | 实现 |
|---|---|
| 建索引或写探针等锁满 3 秒，页面看到实际阻塞方 | `LockWatch` 每 0.5s 独立连接轮询 `performance_schema`（`metadata_locks`/`threads` 覆盖 GET_LOCK 与 MDL，`data_lock_waits`/`data_locks`/`innodb_trx` 覆盖 InnoDB 行锁）；等待跨 3s 阈值写 `blocked` 证据，含阻塞方用户、来源连接、线程、SQL 与锁名。结果页「锁等待诊断」表直接展示 |
| 诊断失败不拖住试验 | 观察者跑在独立守护线程与独立连接，只采样不执行试验 SQL；采样/持久化异常全部捕获，试验流水线无任何诊断调用在关键路径上等待 |
| 采集失败后恢复观测，断档标未知 | 采样失败的周期对每个在册试验写且只写一条 `state=unknown, blocker=null`；下一周期采样恢复即继续出 `blocked` 证据（`_unknown_during_outage` 去重） |
| 并发与连接复用时证据归到当时试验及阶段 | 注册键为 `experiment_id → (connection_id, phase)`；采样结果按连接反查到试验，阶段切换先结清上一段。同一物理连接被池化复用到下一个试验时，两次证据按试验 id 与各自阶段分开落库 |
| 结束封存，迟到观测不改结论 | worker 完成/失败均先 `watch.seal(id)`（内存闸门拒绝迟到写入），再把行级证据快照进 `experiments.lock_evidence`，并把已有行 `sealed=1`；读取端封存后只返回快照 JSON |
| 阶段覆盖 | `serial_lock / drop_legacy / baseline_plan / baseline_query / write_probe_baseline / create_index / analyze / candidate_plan / candidate_query / write_probe_candidate / cleanup`，前后端枚举一致 |

## 2. 验证场景（共 17 个，全部通过）

命令：`test-artifacts/gb-4102/scripts/run_all.sh`（`python3 -I`，仅用标准库；本环境无 pip/Docker，SQLAlchemy/PyMySQL/Redis 以最小桩件注入，业务代码原样运行）。

### 2.1 持锁期间（要求点名场景）
- `test_held_lock_emits_blocker_after_three_seconds_and_release`：3s 内无证据；满 3s 出 1 条 `blocked`（阶段 `create_index`、阻塞连接 #77、阻塞 SQL 原文）；持续阻塞不重复告警；释放后补 1 条 `released` 且等待总时长正确。
- `test_real_thread_emits_evidence_for_held_lock_window`：真实后台线程 + 真实时钟，持锁 3.4s 的时间窗内自动产出 blocked/released，连接归属一致。
- `test_lock_wait_during_get_lock_is_attributed_and_sealed`：流水线集成，假引擎在 `GET_LOCK` 处挂起，观察者跨阈值记录 `serial_lock/user_level` 阻塞方 #77，放行后试验正常完成。
- `test_wait_under_threshold_leaves_no_evidence`：2.9s 释放，不留证据。

### 2.2 观测断开（要求点名场景）
- `test_unknown_during_outage_then_observation_resumes`：连续两个失败周期只产生 1 条 `unknown`（blocker 为 null、阶段正确）；恢复后真实行锁阻塞方仍能在 3s 后落 `blocked`。
- `test_background_thread_survives_sampler_errors`：连续 3 次采样抛异常，线程不退出、继续轮询（calls≥4）。
- `test_observer_failure_marks_unknown_without_stopping_trial`：端到端，试验期间观察者前 3 次断连，试验照常完成，证据同时含 unknown 与 blocked。
- `test_persister_failure_never_raises_into_observer`：持久化故障只记日志，不冒泡。

### 2.3 下一次试验复用连接（要求点名场景）
- `test_phases_on_reused_connection_are_attributed_separately`：连接 #303 先后给试验 1（`serial_lock`，阻塞方 #9）与试验 2（`create_index`，阻塞方 #10），证据互不串台。
- `test_sealed_experiment_keeps_evidence_while_next_trial_reuses_connection`：试验 1 封存后同连接给试验 2，试验 1 的证据快照不变，试验 2 独立记录。
- `test_connection_reuse_second_trial_keeps_separate_evidence`：流水线级两次 `run()` + 封存，迟到的断档周期无法改写已封存试验。

### 2.4 封存与并发
- `test_sealing_rejects_late_observations`：封存后采样到同连接阻塞、甚至断档标记，全部被拒。
- `test_concurrent_experiments_share_observer_without_cross_attribution`：两个试验并行、各持不同连接，阻塞方 #50/#60 与各自阶段精确对应。
- `test_phase_change_closes_open_episode`：阶段移动自动结清未闭合等待。
- `test_run_registers_phases_and_unregisters`：11 个阶段按序上报，结束注销无残留。
- `test_summarize_and_live_view` / `test_seal_rows_marks_rows_and_builds_snapshot`：读取端实时视图与封存快照语义、行 `sealed=1` 标记。

### 2.5 前端
- Node 对 `lockEvidence.js` 渲染冒烟：阻塞方表格、未知断档文案、空态与封存说明、列表徽章均正确（见手工执行记录，无 node 测试框架故未计入 17 个用例）。
- 全部改动 JS 通过 `node --check`，Python 通过 `py_compile`。

## 3. 环境边界与未覆盖项

- 本机无 Docker、无 MySQL 8、无 pip，**未对真实服务器执行诊断 SQL**。两条采样 SQL 按 MySQL 8.0 文档编写（`metadata_locks.LOCK_STATUS/OBJECT_TYPE='USER LEVEL LOCK'`、`data_lock_waits` 经 `innodb_trx.trx_mysql_thread_id` 映射 `threads.PROCESSLIST_ID`），已在迁移文档与 `docker-compose` 增加 `GRANT SELECT ON performance_schema.* TO 'trial'`（initdb 只在首次初始化卷时执行；已存在的卷需手工执行该 GRANT）。
- 阈值固定 3s、轮询 0.5s，定义在 `constants/enums.py`；最坏情况下证据出现延迟约 3.5s，不影响试验本身耗时。
- 进程崩溃后 running 任务沿用原设计不自动重试；崩溃时未落库的内存中等待段会缺失，表现为该试验无证据而非伪造结论。

## 4. 结果

17/17 通过，0 失败。日志中出现的 `RuntimeError` 栈是用例故意注入的采样/持久化故障，断言其被吞咽且观测继续。
