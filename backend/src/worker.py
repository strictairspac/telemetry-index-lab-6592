import time,logging,redis
from datetime import datetime,timezone
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.experiment import Experiment
from src.services.index_algorithm import run
from src.services.lock_watch import default_watch
from src.services.lock_evidence import seal_rows
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
watch=default_watch();watch.start()
def _seal(run_id):
 # Freeze evidence first; any observer row arriving afterwards is rejected.
 watch.seal(run_id)
 sealed_at=datetime.now(timezone.utc).isoformat()
 with Session() as s:
  row=s.get(Experiment,run_id)
  if row is None:return
  row.lock_evidence=seal_rows(s,run_id,sealed_at);row.evidence_sealed_at=datetime.now(timezone.utc)
  s.commit()
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:
  item=q.blpop('index-trials',timeout=2)
  if not item:continue
  run_id=int(item[1])
  with Session() as s:
   row=s.get(Experiment,run_id)
   if not row or row.status!='queued':continue
   row.status='running';snapshot=row.snapshot;s.commit()
  try:
   result=run(snapshot,experiment_id=run_id,watch=watch)
   with Session() as s:r=s.get(Experiment,run_id);r.result=result;r.status='completed';s.commit()
   _seal(run_id)
   log('worker','experiment.completed',str(run_id))
  except Exception as exc:
   logging.exception('trial failed')
   with Session() as s:r=s.get(Experiment,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
   _seal(run_id)
 except Exception:logging.exception('worker loop');time.sleep(2)
