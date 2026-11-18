"""Read-side grouping of lock-wait evidence.

Two views of the same truth:
* sealed summary stored on the experiment once the trial ends — immutable;
* live observations read from lock_observations while the trial is still
  running, so the page can show the blocker during the wait itself.
"""
from src.models.lock_observation import LockObservation
def _row(r):
 return {'id':r.id,'phase':r.phase,'lock_kind':r.lock_kind,'state':r.state,'waited_seconds':float(r.waited_seconds or 0),'connection_id':r.connection_id,'blocker':r.blocker,'observed_at':r.observed_at.isoformat() if r.observed_at else None}
def observations_for(session,experiment_id):
 rows=session.query(LockObservation).filter(LockObservation.experiment_id==experiment_id).order_by(LockObservation.id.asc()).all()
 return [_row(r) for r in rows]
def summarize(observations):
 blocked=[o for o in observations if o['state']=='blocked']
 unknown=[o for o in observations if o['state']=='unknown']
 released=[o for o in observations if o['state']=='released']
 return {'count':len(observations),'blocked_count':len(blocked),'unknown_count':len(unknown),'released_count':len(released),'has_blocker':any(o.get('blocker') for o in blocked),'latest':observations[-1] if observations else None}
def evidence_for(session,experiment_id):
 obs=observations_for(session,experiment_id)
 return {'sealed_at':None,'observations':obs,'summary':summarize(obs)}
def seal_rows(session,experiment_id,sealed_at_iso):
 """Freeze the rows that made it in before sealing and return the snapshot."""
 rows=session.query(LockObservation).filter(LockObservation.experiment_id==experiment_id).order_by(LockObservation.id.asc()).all()
 for r in rows:r.sealed=1
 obs=[_row(r) for r in rows]
 return {'sealed_at':sealed_at_iso,'observations':obs,'summary':summarize(obs)}
def live_view(session,experiment):
 """Sealed JSON wins once present; otherwise read fresh rows."""
 if experiment.lock_evidence is not None:
  return experiment.lock_evidence
 obs=observations_for(session,experiment.id)
 return {'sealed_at':experiment.evidence_sealed_at.isoformat() if experiment.evidence_sealed_at else None,'observations':obs,'summary':summarize(obs)}
