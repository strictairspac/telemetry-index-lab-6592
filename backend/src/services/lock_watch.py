"""Lock-wait diagnostics for index trials.

A single background observer polls MySQL performance_schema on its own
connection. Trials register their experiment/phase/connection so evidence is
attributed correctly even when one connection is reused across phases or two
trials interleave. The observer never runs trial SQL: sampler failures mark
the outage window as unknown and observation resumes, so diagnostics can never
stall a trial. Sealing an experiment drops every observation that arrives late.
"""
import logging,threading,time
from sqlalchemy import text
from src.config.database import engine
from src.constants.enums import LOCK_WAIT_THRESHOLD_S,LOCK_POLL_INTERVAL_S
# MySQL 8.0 performance_schema. Two independent probes, merged in Python so a
# failure of one view never hides the other.
# 1) PENDING metadata/user-level locks (covers GET_LOCK and table MDL) joined
#    to the GRANTED holder of the same lock name.
SQL_METADATA_LOCKS="""
SELECT waiter.PROCESSLIST_ID AS connection_id,
       ml.lock_name AS lock_name,
       CASE WHEN ml.OBJECT_TYPE='USER LEVEL LOCK' THEN 'user_level' ELSE 'metadata' END AS lock_kind,
       holder.thread_id AS blocker_thread_id,
       holder.PROCESSLIST_ID AS blocker_connection_id,
       holder.PROCESSLIST_USER AS blocker_user,
       holder.PROCESSLIST_HOST AS blocker_host,
       holder.PROCESSLIST_DB AS blocker_db,
       holder.PROCESSLIST_INFO AS blocker_query,
       hml.lock_name AS blocker_lock_name
FROM performance_schema.metadata_locks ml
JOIN performance_schema.threads waiter ON waiter.thread_id=ml.OWNER_THREAD_ID
LEFT JOIN performance_schema.metadata_locks hml
  ON hml.lock_name=ml.lock_name
 AND hml.OBJECT_TYPE=ml.OBJECT_TYPE
 AND hml.OBJECT_SCHEMA<=>ml.OBJECT_SCHEMA
 AND hml.OBJECT_NAME<=>ml.OBJECT_NAME
 AND hml.LOCK_STATUS='GRANTED'
LEFT JOIN performance_schema.threads holder ON holder.thread_id=hml.OWNER_THREAD_ID
WHERE ml.LOCK_STATUS='PENDING' AND waiter.PROCESSLIST_ID IS NOT NULL
"""
# 2) InnoDB row-lock waits: requesting transaction -> blocking transaction.
#    Threads map to transactions through information_schema.innodb_trx.
SQL_RECORD_LOCKS="""
SELECT waiter_thr.PROCESSLIST_ID AS connection_id,
       CONCAT(req.ENGINE_LOCK_ID) AS lock_name,
       'record' AS lock_kind,
       blocker_thr.thread_id AS blocker_thread_id,
       blocker_thr.PROCESSLIST_ID AS blocker_connection_id,
       blocker_thr.PROCESSLIST_USER AS blocker_user,
       blocker_thr.PROCESSLIST_HOST AS blocker_host,
       blocker_thr.PROCESSLIST_DB AS blocker_db,
       blocker_thr.PROCESSLIST_INFO AS blocker_query,
       CONCAT(blk.ENGINE_LOCK_ID) AS blocker_lock_name
FROM performance_schema.data_lock_waits w
JOIN performance_schema.data_locks req ON req.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID
JOIN performance_schema.data_locks blk ON blk.ENGINE_LOCK_ID=w.BLOCKING_ENGINE_LOCK_ID
JOIN information_schema.innodb_trx wtrx ON wtrx.trx_id=w.REQUESTING_ENGINE_TRANSACTION_ID
JOIN information_schema.innodb_trx btrx ON btrx.trx_id=w.BLOCKING_ENGINE_TRANSACTION_ID
JOIN performance_schema.threads waiter_thr ON waiter_thr.PROCESSLIST_ID=wtrx.trx_mysql_thread_id
JOIN performance_schema.threads blocker_thr ON blocker_thr.PROCESSLIST_ID=btrx.trx_mysql_thread_id
"""
def sql_sampler(conn_factory):
 def sample():
  rows=[]
  with conn_factory() as conn:
   rows.extend(dict(r) for r in conn.execute(text(SQL_METADATA_LOCKS)).mappings())
   rows.extend(dict(r) for r in conn.execute(text(SQL_RECORD_LOCKS)).mappings())
  return rows
 return sample
def sql_persister(session_factory):
 def persist(experiment_id,rec):
  from src.models.lock_observation import LockObservation
  with session_factory() as s:
   s.add(LockObservation(experiment_id=experiment_id,phase=rec['phase'],lock_kind=rec['lock_kind'],state=rec['state'],waited_seconds=rec['waited_seconds'],connection_id=rec.get('connection_id'),blocker=rec.get('blocker'),sealed=0))
   s.commit()
 return persist
class LockWatch:
 """Process-wide registry and observer; one instance per worker process."""
 def __init__(self,sampler=None,persister=None,interval=LOCK_POLL_INTERVAL_S,clock=time.perf_counter,sleep=time.sleep):
  self._sampler=sampler
  self._persister=persister
  self._interval=interval
  self._clock=clock
  self._sleep=sleep
  self._entries={}
  self._episodes={}
  self._sealed=set()
  self._unknown_during_outage=set()
  self._lock=threading.RLock()
  self._stop=threading.Event()
  self._thread=None
 def bind(self,sampler,persister):
  self._sampler=sampler;self._persister=persister
 def register(self,experiment_id,connection_id):
  with self._lock:
   self._entries[experiment_id]={'connection_id':connection_id,'phase':None,'started_at':self._clock()}
   self._episodes.pop(experiment_id,None)
   self._unknown_during_outage.discard(experiment_id)
 def set_phase(self,experiment_id,phase):
  with self._lock:
   entry=self._entries.get(experiment_id)
   if not entry or entry['phase']==phase:return
   self._close_episode(experiment_id,'released')
   entry['phase']=phase
 def unregister(self,experiment_id):
  with self._lock:
   self._close_episode(experiment_id,'released')
   self._entries.pop(experiment_id,None)
   self._episodes.pop(experiment_id,None)
 def seal(self,experiment_id):
  """No observation after this point may change the experiment's evidence."""
  with self._lock:
   self._close_episode(experiment_id,'released')
   self._entries.pop(experiment_id,None)
   self._episodes.pop(experiment_id,None)
   self._sealed.add(experiment_id)
 def snapshot(self):
  with self._lock:return {'entries':dict(self._entries),'episodes':{k:dict(v) for k,v in self._episodes.items()},'sealed':set(self._sealed)}
 def start(self):
  if self._thread:return
  self._stop.clear();self._thread=threading.Thread(target=self._run,name='lock-watch',daemon=True);self._thread.start()
 def stop(self):
  self._stop.set()
  if self._thread:self._thread.join(timeout=5);self._thread=None
 def _close_episode(self,experiment_id,state):
  ep=self._episodes.pop(experiment_id,None)
  if not ep or experiment_id in self._sealed:return
  waited=self._clock()-ep['started_at']
  if waited<LOCK_WAIT_THRESHOLD_S:return
  self._write(experiment_id,{'phase':ep['phase'],'lock_kind':ep['lock_kind'],'state':state,'waited_seconds':round(waited,3),'connection_id':ep['connection_id'],'blocker':ep['blocker']})
 def _write(self,experiment_id,rec):
  if experiment_id in self._sealed:return
  if not self._persister:
   logging.getLogger('lock_watch').warning('no persister; drop observation experiment=%s phase=%s',experiment_id,rec.get('phase'));return
  try:self._persister(experiment_id,rec)
  except Exception:logging.exception('persist lock observation failed; experiment=%s',experiment_id)
 def _attributed(self,rows):
  """Map sampled waiting connections to registered experiments."""
  by_conn={}
  with self._lock:
   for exp_id,entry in self._entries.items():by_conn.setdefault(entry['connection_id'],[]).append(exp_id)
  found={}
  for row in rows:
   for exp_id in by_conn.get(row.get('connection_id'),[]):found[exp_id]=row
  return found
 def poll_once(self,rows=None,sampler_failed=False):
  """One observation cycle. Exposed for deterministic unit tests."""
  with self._lock:active=dict(self._entries)
  if sampler_failed:
   for exp_id,entry in active.items():
    if exp_id in self._unknown_during_outage:continue
    self._unknown_during_outage.add(exp_id)
    self._write(exp_id,{'phase':entry['phase'] or 'serial_lock','lock_kind':'unknown','state':'unknown','waited_seconds':LOCK_WAIT_THRESHOLD_S,'connection_id':entry['connection_id'],'blocker':None})
   return
  self._unknown_during_outage.clear()
  waits=self._attributed(rows if rows is not None else (self._sampler() if self._sampler else []))
  for exp_id,entry in active.items():
   row=waits.get(exp_id)
   if not row:
    self._close_episode(exp_id,'released');continue
   phase=entry['phase'] or 'serial_lock'
   ep=self._episodes.get(exp_id)
   if not ep:
    self._episodes[exp_id]={'phase':phase,'lock_kind':row['lock_kind'],'started_at':self._clock(),'blocker':self._blocker(row),'emitted':False,'connection_id':entry['connection_id']}
    ep=self._episodes[exp_id]
   else:
    ep['blocker']=self._blocker(row)
   if self._clock()-ep['started_at']>=LOCK_WAIT_THRESHOLD_S and not ep['emitted']:
    ep['emitted']=True
    self._write(exp_id,{'phase':phase,'lock_kind':row['lock_kind'],'state':'blocked','waited_seconds':LOCK_WAIT_THRESHOLD_S,'connection_id':entry['connection_id'],'blocker':ep['blocker']})
 @staticmethod
 def _blocker(row):
  if not row.get('blocker_thread_id'):return None
  return {'thread_id':row['blocker_thread_id'],'connection_id':row.get('blocker_connection_id'),'user':row.get('blocker_user'),'host':row.get('blocker_host'),'db':row.get('blocker_db'),'query':(row.get('blocker_query') or '')[:500],'lock_name':row.get('blocker_lock_name') or row.get('lock_name')}
 def _run(self):
  while not self._stop.is_set():
   try:
    rows=self._sampler() if self._sampler else []
    self.poll_once(rows=rows)
   except Exception:
    logging.exception('lock sampler failed; outage marked unknown and observation resumes')
    try:self.poll_once(sampler_failed=True)
    except Exception:logging.exception('unknown-marker write failed')
   self._stop.wait(self._interval)
watch=LockWatch()
def default_watch():
 """Attach real MySQL sampler/persister lazily so importing stays side-effect free."""
 if watch._sampler is None:
  from src.config.database import Session
  watch.bind(sql_sampler(engine.connect),sql_persister(Session))
 return watch
