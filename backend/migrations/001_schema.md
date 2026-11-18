# Schema v1
SQLAlchemy metadata creates users, audits, workloads, candidates, experiments and telemetry on first startup. Workload/candidate natural names are UNIQUE; experiment snapshots are immutable JSON. Foreign keys prevent orphan references. Index trials affect only the dedicated telemetry table, under MySQL GET_LOCK, and drop the trial index after each execution. No production database connection is accepted.

# Schema v2 — lock-wait diagnostics
Appends table `lock_observations(id, experiment_id FK, phase, lock_kind, state, waited_seconds Numeric(8,3), connection_id, blocker JSON, observed_at, sealed)` with index (experiment_id, id), and nullable columns `experiments.lock_evidence JSON`, `experiments.evidence_sealed_at DATETIME`. Rows are append-only evidence written by the worker-side lock observer (states: blocked / released / unknown). At trial end the worker seals the experiment: the grouped snapshot is copied into `experiments.lock_evidence` and later observer rows for that experiment are rejected by the in-process watch. Existing v1 rows get NULL lock columns (treated as no observations).

Equivalent DDL for an existing database:

```sql
CREATE TABLE lock_observations (
  id INT AUTO_INCREMENT PRIMARY KEY,
  experiment_id INT NOT NULL,
  phase VARCHAR(40) NOT NULL,
  lock_kind VARCHAR(20) NOT NULL,
  state VARCHAR(20) NOT NULL,
  waited_seconds DECIMAL(8,3) NOT NULL DEFAULT 0,
  connection_id INT NULL,
  blocker JSON NULL,
  observed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  sealed TINYINT NOT NULL DEFAULT 0,
  INDEX ix_lock_obs_experiment (experiment_id, id),
  CONSTRAINT fk_lock_obs_experiment FOREIGN KEY (experiment_id) REFERENCES experiments(id)
);
ALTER TABLE experiments ADD COLUMN lock_evidence JSON NULL, ADD COLUMN evidence_sealed_at DATETIME NULL;
```

The observer reads `performance_schema.metadata_locks`, `performance_schema.threads`, `performance_schema.data_lock_waits/data_locks` and `information_schema.innodb_trx`; the trial account needs SELECT on performance_schema/information_schema (granted by default in MySQL 8.0).
