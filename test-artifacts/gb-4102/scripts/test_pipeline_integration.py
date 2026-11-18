"""Pipeline integration tests: index_algorithm.run phases and worker sealing.

A fake SQLAlchemy engine records every SQL call and can block on a named
statement to drive the observer, proving end-to-end attribution and sealing
without a MySQL server.
"""
import sys,os,time,threading,unittest,types
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..','..','..','backend'))
sys.path.insert(0,os.path.dirname(__file__))
import _stubs
_stubs.install()
from src.services import lock_watch as lw_mod
from src.services.lock_watch import LockWatch
SNAPSHOT={'workload':{'sensor_id':17,'start_at':'2026-11-01T00:00:00','end_at':'2026-11-30T23:59:59','device_type':None},
          'candidate':{'columns':['sensor_id','recorded_at']}}
class Result:
 def __init__(self,rows=()):self._rows=[dict(r) for r in rows]
 def mappings(self):return self
 def __iter__(self):return iter(self._rows)
 def all(self):return list(self._rows)
 def scalar(self):return self._rows[0].get('v') if self._rows and 'v' in self._rows[0] else (self._rows[0][list(self._rows[0])[0]] if self._rows else None)
class FakeConn:
 def __init__(self,owner):self.owner=owner;self.closed=False
 def execute(self,stmt,params=None):
  sql=stmt.sql if hasattr(stmt,'sql') else str(stmt)
  self.owner.calls.append(sql)
  if sql.startswith('SELECT CONNECTION_ID()'):return Result([{'v':self.owner.connection_id}])
  if sql.startswith('SELECT GET_LOCK'):
   self.owner.gate.wait()
   return Result([{'v':1}])
  if sql.startswith('SELECT COUNT(*)'):return Result([{'v':0}])
  if sql.startswith('EXPLAIN'):return Result([{'id':1,'type':'ref','key':'trial_candidate','rows':5}])
  if sql.startswith('ANALYZE'):return Result([])
  if sql.startswith('SELECT RELEASE_LOCK'):return Result([{'v':1}])
  if sql.startswith('SELECT id,sensor_id'):return Result([{'id':1,'sensor_id':17,'recorded_at':'2026-11-17T12:00:00','device_type':'pump','value':42.5}])
  if sql.startswith('INSERT INTO telemetry'):return Result([])
  return Result([])
 def rollback(self):self.owner.calls.append('ROLLBACK')
 def commit(self):self.owner.calls.append('COMMIT')
 def close(self):self.closed=True
 def __enter__(self):return self
 def __exit__(self,*a):self.close()
class FakeEngine:
 def __init__(self,connection_id=4242):self.connection_id=connection_id;self.calls=[];self.gate=threading.Event();self.gate.set()
 def connect(self):return FakeConn(self)
class SamplerHarness:
 """Sampler that reports a wait on the trial connection while the gate is closed."""
 def __init__(self,engine):self.engine=engine;self.seen=[]
 def __call__(self):
  if not self.engine.gate.is_set():
   self.seen.append(time.time())
   return [{'connection_id':self.engine.connection_id,'lock_name':'index_trial_serial','lock_kind':'user_level',
            'blocker_thread_id':9,'blocker_connection_id':77,'blocker_user':'trial','blocker_host':'db',
            'blocker_db':'indextrial','blocker_query':'GET_LOCK other session','blocker_lock_name':'index_trial_serial'}]
  return []
class PipelineTests(unittest.TestCase):
 def setUp(self):
  self.engine=FakeEngine()
  import src.config.database as dbmod
  self._orig_engine=dbmod.engine;dbmod.engine=self.engine
  import src.services.index_algorithm as ia
  self.ia=ia;ia.engine=self.engine
  self.persister=_stubs.RecordingPersister()
  self.sampler=SamplerHarness(self.engine)
  self.watch=LockWatch(sampler=self.sampler,persister=self.persister,interval=0.05,clock=time.perf_counter,sleep=time.sleep)
 def tearDown(self):
  import src.config.database as dbmod
  dbmod.engine=self._orig_engine;self.ia.engine=self._orig_engine
 def test_run_registers_phases_and_unregisters(self):
  phases=[]
  orig=self.watch.set_phase
  def spy(exp,phase):phases.append(phase);orig(exp,phase)
  self.watch.set_phase=spy
  result=self.ia.run(SNAPSHOT,experiment_id=11,watch=self.watch)
  self.assertEqual(result['rows'],1)
  self.assertIn('serial_lock',phases);self.assertIn('create_index',phases);self.assertIn('cleanup',phases)
  self.assertEqual(phases[-1],'cleanup')
  self.assertNotIn(11,self.watch.snapshot()['entries'])  # unregistered at end
  self.assertTrue(any(c.startswith('CREATE INDEX trial_candidate') for c in self.engine.calls))
  self.assertTrue(any(c.startswith('SELECT RELEASE_LOCK') for c in self.engine.calls))
 def test_lock_wait_during_get_lock_is_attributed_and_sealed(self):
  self.engine.gate.clear()  # GET_LOCK blocks until released
  t=threading.Thread(target=lambda:self._run_result())
  t.start()
  time.sleep(0.2)
  self.watch.start()
  time.sleep(3.6)          # cross the 3s threshold while held
  self.engine.gate.set()   # holder releases; trial proceeds
  t.join(timeout=10)
  self.watch.stop()
  self.assertIsNotNone(self._result)
  ev=self.persister.for_experiment(12)
  blocked=[r for r in ev if r['state']=='blocked']
  self.assertEqual(len(blocked),1)
  self.assertEqual(blocked[0]['phase'],'serial_lock')
  self.assertEqual(blocked[0]['lock_kind'],'user_level')
  self.assertEqual(blocked[0]['blocker']['connection_id'],77)
  self.assertEqual(blocked[0]['connection_id'],self.engine.connection_id)
  # seal: snapshot taken, then late rows rejected
  self.watch.seal(12)
  before=list(self.persister.for_experiment(12))
  self.watch.poll_once(sampler_failed=True)
  self.assertEqual(self.persister.for_experiment(12),before)
 def _run_result(self):
  try:self._result=self.ia.run(SNAPSHOT,experiment_id=12,watch=self.watch)
  except Exception as e:self._result=e;raise
 def test_observer_failure_marks_unknown_without_stopping_trial(self):
  calls={'n':0}
  def flaky():
   calls['n']+=1
   if calls['n']<=3:raise RuntimeError('performance_schema unreachable')
   return self.sampler()
  self.watch._sampler=flaky
  self.engine.gate.clear()
  t=threading.Thread(target=lambda:self._run_result_13())
  t.start();time.sleep(0.2);self.watch.start()
  time.sleep(3.8);self.engine.gate.set();t.join(timeout=10);self.watch.stop()
  self.assertFalse(isinstance(self._result13,Exception))
  ev=self.persister.for_experiment(13)
  self.assertTrue(any(r['state']=='unknown' for r in ev))
  self.assertTrue(any(r['state']=='blocked' and r['blocker'] for r in ev))
 def _run_result_13(self):
  try:self._result13=self.ia.run(SNAPSHOT,experiment_id=13,watch=self.watch)
  except Exception as e:self._result13=e;raise
 def test_connection_reuse_second_trial_keeps_separate_evidence(self):
  # Two sequential trials over the same fake connection id; seal between them.
  self.ia.run(SNAPSHOT,experiment_id=21,watch=self.watch);self.watch.seal(21)
  first=list(self.persister.for_experiment(21))
  self.ia.run(SNAPSHOT,experiment_id=22,watch=self.watch);self.watch.seal(22)
  self.assertEqual(self.persister.for_experiment(21),first)  # sealed trial untouched
  self.watch.poll_once(sampler_failed=True)                  # late outage row
  self.assertEqual(self.persister.for_experiment(21),first)
class EvidenceGroupingTests(unittest.TestCase):
 def test_summarize_and_live_view(self):
  from src.services.lock_evidence import summarize,live_view
  obs=[{'id':1,'phase':'create_index','lock_kind':'metadata','state':'blocked','waited_seconds':3.0,'connection_id':4,'blocker':{'connection_id':9},'observed_at':None},
       {'id':2,'phase':'create_index','lock_kind':'metadata','state':'released','waited_seconds':6.2,'connection_id':4,'blocker':{'connection_id':9},'observed_at':None},
       {'id':3,'phase':'cleanup','lock_kind':'unknown','state':'unknown','waited_seconds':3,'connection_id':4,'blocker':None,'observed_at':None}]
  s=summarize(obs)
  self.assertEqual((s['count'],s['blocked_count'],s['unknown_count'],s['released_count']),(3,1,1,1))
  self.assertTrue(s['has_blocker']);self.assertEqual(s['latest']['id'],3)
  row=types.SimpleNamespace(id=5,lock_evidence=None,evidence_sealed_at=None)
  # live view reads fresh when nothing sealed
  class Sess:
   def __enter__(self):return self
   def __exit__(self,*a):return False
  import src.services.lock_evidence as le
  orig=le.observations_for
  le.observations_for=lambda session,eid:obs
  try:view=live_view(Sess(),row)
  finally:le.observations_for=orig
  self.assertIsNone(view['sealed_at']);self.assertEqual(view['summary']['count'],3)
  # sealed JSON wins and is returned verbatim
  row.lock_evidence={'sealed_at':'2026-11-18T08:00:00+00:00','observations':obs[:1],'summary':summarize(obs[:1])}
  sealed=live_view(Sess(),row)
  self.assertEqual(sealed['observations'],obs[:1])
 def test_seal_rows_marks_rows_and_builds_snapshot(self):
  from src.services.lock_evidence import seal_rows
  import datetime as dt
  class R:
   def __init__(self,i,state):
    self.id=i;self.experiment_id=1;self.phase='create_index';self.lock_kind='metadata'
    self.state=state;self.waited_seconds=3.0;self.connection_id=4
    self.blocker={'connection_id':9} if state!='unknown' else None
    self.observed_at=dt.datetime(2026,11,18,tzinfo=dt.timezone.utc);self.sealed=0
  rows=[R(1,'blocked'),R(2,'released')]
  class Q:
   def filter(self,*a,**k):return self
   def order_by(self,*a,**k):return self
   def all(self):return rows
  class S:
   def query(self,m):return Q()
  snap=seal_rows(S(),1,'2026-11-18T08:00:00+00:00')
  self.assertTrue(all(r.sealed==1 for r in rows))
  self.assertEqual(snap['sealed_at'],'2026-11-18T08:00:00+00:00')
  self.assertEqual(snap['summary']['count'],2)
  self.assertEqual([o['id'] for o in snap['observations']],[1,2])
if __name__=='__main__':
 unittest.main(verbosity=2)
