# Shared lock/observation enums used by worker, services and frontend contract.
ROLES=('admin','operator','viewer')
STATUSES=('queued','running','completed','failed')
COLUMNS=('sensor_id','recorded_at','device_type','value')
# Trial phases that can block on a MySQL lock. Order follows the worker pipeline.
LOCK_PHASES=('serial_lock','drop_legacy','baseline_plan','baseline_query','write_probe_baseline','create_index','analyze','candidate_plan','candidate_query','write_probe_candidate','cleanup')
LOCK_KINDS=('user_level','metadata','record')
OBSERVATION_STATES=('blocked','released','unknown')
# A wait becomes evidence once it crosses this many seconds.
LOCK_WAIT_THRESHOLD_S=3
# Observer polling cadence; diagnostics stays one poll behind at worst.
LOCK_POLL_INTERVAL_S=0.5
