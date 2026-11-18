export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'等待执行',running:'试验中',completed:'已完成',failed:'执行失败'};
export const COLUMNS=['sensor_id','recorded_at','device_type','value'];
export const LOCK_PHASES={serial_lock:'获取串行试验锁',drop_legacy:'清理遗留索引',baseline_plan:'基线执行计划',baseline_query:'基线查询',write_probe_baseline:'基线写入探针',create_index:'创建候选索引',analyze:'ANALYZE 统计信息',candidate_plan:'候选执行计划',candidate_query:'候选查询',write_probe_candidate:'候选写入探针',cleanup:'清理索引与释放锁'};
export const LOCK_KINDS={user_level:'命名锁 GET_LOCK',metadata:'元数据锁 MDL',record:'InnoDB 行锁',unknown:'未知'};
export const OBSERVATION_STATES={blocked:'确认阻塞',released:'锁已释放',unknown:'观测断档（阻塞方未知）'};
