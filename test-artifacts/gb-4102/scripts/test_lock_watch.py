"""Lock-watch diagnostics state machine tests (stdlib only).

Covers: held-lock blocker evidence past 3s, observer outage -> unknown and
resumption, per-experiment/per-phase attribution with connection reuse,
sealing so late observations cannot change conclusions.
"""
import sys,os,time,threading,unittest
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..','..','..','backend'))
sys.path.insert(0,os.path.dirname(__file__))
import _stubs
_stubs.install()
from src.services.lock_watch import LockWatch
from src.constants.enums import LOCK_WAIT_THRESHOLD_S
class LockWatchTests(unittest.TestCase):
 def setUp(self):
  self.clock=_stubs.Clock();self.persister=_stubs.RecordingPersister()
  self.watch=LockWatch(sampler=lambda:[],persister=self.persister,interval=0,clock=self.clock.now,sleep=lambda s:None)
 def test_wait_under_threshold_leaves_no_evidence(self):
  self.watch.register(1,101);self.watch.set_phase(1,'create_index')
  self.watch.poll_once(rows=[_stubs.blocker_row(101)])
  self.clock.advance(2.9)
  self.watch.poll_once(rows=[_stubs.blocker_row(101)])
  self.watch.poll_once(rows=[])  # wait ends before 3s
  self.assertEqual(self.persister.for_experiment(1),[])
 def test_held_lock_emits_blocker_after_three_seconds_and_release(self):
  self.watch.register(1,101);self.watch.set_phase(1,'create_index')
  row=_stubs.blocker_row(101,blocker_conn=77,kind='metadata')
  self.watch.poll_once(rows=[row])
  self.clock.advance(LOCK_WAIT_THRESHOLD_S)
  self.watch.poll_once(rows=[row])
  blocked=[r for r in self.persister.for_experiment(1) if r['state']=='blocked']
  self.assertEqual(len(blocked),1)
  rec=blocked[0]
  self.assertEqual(rec['phase'],'create_index');self.assertEqual(rec['lock_kind'],'metadata')
  self.assertEqual(rec['connection_id'],101);self.assertEqual(rec['waited_seconds'],3)
  self.assertEqual(rec['blocker']['connection_id'],77)
  self.assertEqual(rec['blocker']['user'],'trial')
  self.assertIn('ALTER TABLE',rec['blocker']['query'])
  # still blocked: no duplicate blocked event
  self.clock.advance(2);self.watch.poll_once(rows=[row])
  self.assertEqual(len([r for r in self.persister.for_experiment(1) if r['state']=='blocked']),1)
  # released: closing episode records release with total wait
  self.clock.advance(1);self.watch.poll_once(rows=[])
  released=[r for r in self.persister.for_experiment(1) if r['state']=='released']
  self.assertEqual(len(released),1);self.assertGreaterEqual(released[0]['waited_seconds'],6)
  self.assertEqual(released[0]['blocker']['connection_id'],77)
 def test_unknown_during_outage_then_observation_resumes(self):
  self.watch.register(2,202);self.watch.set_phase(2,'write_probe_candidate')
  flaky=_stubs.FlakySampler(rows=[_stubs.blocker_row(202,kind='record')],fail_times=2)
  self.watch._sampler=flaky
  # two failed cycles: only ONE unknown marker, and trial is never blocked
  self.watch._run_once=lambda:self.watch.poll_once(sampler_failed=True)
  self.watch.poll_once(sampler_failed=True)
  self.watch.poll_once(sampler_failed=True)
  unknown=[r for r in self.persister.for_experiment(2) if r['state']=='unknown']
  self.assertEqual(len(unknown),1)
  self.assertIsNone(unknown[0]['blocker']);self.assertEqual(unknown[0]['lock_kind'],'unknown')
  self.assertEqual(unknown[0]['phase'],'write_probe_candidate')
  # sampler recovers; real blocker now observed across the 3s threshold
  self.watch.poll_once(rows=flaky.rows)
  self.clock.advance(LOCK_WAIT_THRESHOLD_S)
  self.watch.poll_once(rows=flaky.rows)
  blocked=[r for r in self.persister.for_experiment(2) if r['state']=='blocked']
  self.assertEqual(len(blocked),1);self.assertEqual(blocked[0]['lock_kind'],'record')
  self.assertIsNotNone(blocked[0]['blocker'])
 def test_phases_on_reused_connection_are_attributed_separately(self):
  # Same connection id reused by two sequential experiments.
  self.watch.register(1,303);self.watch.set_phase(1,'serial_lock')
  self.watch.poll_once(rows=[_stubs.blocker_row(303,blocker_conn=9,kind='user_level',name='index_trial_serial')])
  self.clock.advance(3);self.watch.poll_once(rows=[_stubs.blocker_row(303,blocker_conn=9,kind='user_level')])
  self.watch.seal(1)
  # connection returns to the pool and is handed to the next trial
  self.watch.register(2,303);self.watch.set_phase(2,'create_index')
  self.watch.poll_once(rows=[_stubs.blocker_row(303,blocker_conn=10,kind='metadata')])
  self.clock.advance(3);self.watch.poll_once(rows=[_stubs.blocker_row(303,blocker_conn=10,kind='metadata')])
  self.watch.seal(2)
  e1=self.persister.for_experiment(1);e2=self.persister.for_experiment(2)
  self.assertTrue(all(r['phase']=='serial_lock' for r in e1))
  self.assertTrue(all(r['phase']=='create_index' for r in e2))
  self.assertEqual({r['blocker']['connection_id'] for r in e1 if r['blocker']},{9})
  self.assertEqual({r['blocker']['connection_id'] for r in e2 if r['blocker']},{10})
 def test_concurrent_experiments_share_observer_without_cross_attribution(self):
  self.watch.register(1,401);self.watch.register(2,402)
  self.watch.set_phase(1,'baseline_query');self.watch.set_phase(2,'write_probe_baseline')
  rows=[_stubs.blocker_row(401,blocker_conn=50),_stubs.blocker_row(402,blocker_conn=60,kind='record')]
  self.watch.poll_once(rows=rows);self.clock.advance(3);self.watch.poll_once(rows=rows)
  b1=[r for r in self.persister.for_experiment(1) if r['state']=='blocked']
  b2=[r for r in self.persister.for_experiment(2) if r['state']=='blocked']
  self.assertEqual(b1[0]['blocker']['connection_id'],50);self.assertEqual(b2[0]['blocker']['connection_id'],60)
  self.assertEqual(b1[0]['phase'],'baseline_query');self.assertEqual(b2[0]['phase']=='write_probe_baseline',True)
 def test_phase_change_closes_open_episode(self):
  self.watch.register(1,501);self.watch.set_phase(1,'baseline_query')
  row=_stubs.blocker_row(501)
  self.watch.poll_once(rows=[row]);self.clock.advance(3.5);self.watch.poll_once(rows=[row])
  self.watch.set_phase(1,'write_probe_baseline')  # moving on closes the wait
  rel=[r for r in self.persister.for_experiment(1) if r['state']=='released']
  self.assertEqual(len(rel),1);self.assertEqual(rel[0]['phase'],'baseline_query')
 def test_sealing_rejects_late_observations(self):
  self.watch.register(1,601);self.watch.set_phase(1,'create_index')
  self.watch.seal(1)
  # observer keeps running and keeps sampling this connection id
  self.watch.poll_once(rows=[_stubs.blocker_row(601)])
  self.clock.advance(10);self.watch.poll_once(rows=[_stubs.blocker_row(601)])
  self.watch.poll_once(sampler_failed=True)
  self.assertEqual(self.persister.for_experiment(1),[])
 def test_sealed_experiment_keeps_evidence_while_next_trial_reuses_connection(self):
  # Trial 1 records a blocker, then seals.
  self.watch.register(1,601);self.watch.set_phase(1,'create_index')
  row1=_stubs.blocker_row(601,blocker_conn=70)
  self.watch.poll_once(rows=[row1]);self.clock.advance(3);self.watch.poll_once(rows=[row1])
  self.watch.seal(1)
  sealed_snapshot=list(self.persister.for_experiment(1))
  self.assertEqual({r['state'] for r in sealed_snapshot},{'blocked','released'})
  # Next trial reuses the same pooled connection; its evidence stays separate.
  self.watch.register(2,601);self.watch.set_phase(2,'write_probe_candidate')
  row2=_stubs.blocker_row(601,blocker_conn=71,kind='record')
  self.watch.poll_once(rows=[row2]);self.clock.advance(3);self.watch.poll_once(rows=[row2])
  self.watch.seal(2)
  self.assertEqual(self.persister.for_experiment(1),sealed_snapshot)  # untouched
  e2=self.persister.for_experiment(2)
  self.assertTrue(all(r['phase']=='write_probe_candidate' for r in e2))
  self.assertEqual(e2[0]['blocker']['connection_id'],71)
 def test_persister_failure_never_raises_into_observer(self):
  self.persister.fail=True
  self.watch.register(1,701);self.watch.set_phase(1,'create_index')
  row=_stubs.blocker_row(701)
  self.watch.poll_once(rows=[row]);self.clock.advance(4);self.watch.poll_once(rows=[row])  # must not raise
 def test_background_thread_survives_sampler_errors(self):
  flaky=_stubs.FlakySampler(rows=[],fail_times=3);self.watch._sampler=flaky
  self.watch.register(1,801);self.watch.set_phase(1,'create_index')
  self.watch.start()
  deadline=time.time()+3
  while time.time()<deadline and flaky.calls<4:time.sleep(0.02)
  self.watch.stop()
  self.assertGreaterEqual(flaky.calls,4)  # kept polling after repeated failures
  self.assertTrue(self.watch._thread is None or not self.watch._thread.is_alive())
class BackgroundBlockerTests(unittest.TestCase):
 def test_real_thread_emits_evidence_for_held_lock_window(self):
  persister=_stubs.RecordingPersister();clock=_stubs.Clock()
  hold={'until':time.time()+3.4}
  conn=909
  def sampler():
   return [_stubs.blocker_row(conn)] if time.time()<hold['until'] else []
  w=LockWatch(sampler=sampler,persister=persister,interval=0.1,clock=time.perf_counter,sleep=time.sleep)
  w.register(3,conn);w.set_phase(3,'create_index');w.start()
  time.sleep(4.0);w.seal(3);w.stop()
  ev=persister.for_experiment(3)
  self.assertTrue(any(r['state']=='blocked' for r in ev),ev)
  self.assertTrue(any(r['state']=='released' for r in ev),ev)
  self.assertTrue(all(r['connection_id']==conn for r in ev))
if __name__=='__main__':
 unittest.main(verbosity=2)
