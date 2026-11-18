-- Lock-watch diagnostics need read access to MySQL 8.0 instrumentation.
-- Runs once on first database initialization (as root), after the trial
-- account from MYSQL_USER/MYSQL_PASSWORD has been created.
GRANT SELECT ON performance_schema.* TO 'trial'@'%';
FLUSH PRIVILEGES;
