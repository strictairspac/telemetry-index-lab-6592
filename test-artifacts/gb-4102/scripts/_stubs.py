"""Shared stubs so backend modules import without third-party packages.

Only the surfaces used by lock_watch / index_algorithm / worker are modelled.
Tests run with `python3 -I` from the backend directory.
"""
import sys,types,datetime,os
# Backend packages are namespace packages (no __init__.py); make `src`
# resolvable before injecting stub submodules.
_BACKEND=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..','..','backend'))
if _BACKEND not in sys.path:sys.path.insert(0,_BACKEND)
_src=types.ModuleType('src');_src.__path__=[os.path.join(_BACKEND,'src')]
sys.modules['src']=_src
for _sub in ('config','constants','models','services','routes','middlewares','schemas','utils'):
 _m=types.ModuleType('src.'+_sub);_m.__path__=[os.path.join(_BACKEND,'src',_sub)]
 sys.modules['src.'+_sub]=_m
# --- sqlalchemy stubs -------------------------------------------------------
class _Text:
 def __init__(self,sql):self.sql=sql
def text(sql):return _Text(sql)
class _Column:
 def __init__(self,*a,**k):pass
def _maker(*a,**k):
 class _C:
  def __init__(self,*x,**y):pass
 return _C
sa=types.ModuleType('sqlalchemy')
sa.text=text;sa.create_engine=lambda *a,**k:object()
sa.Column=_Column;sa.Integer=sa.String=sa.JSON=sa.DateTime=sa.ForeignKey=sa.Index=sa.Numeric=object
class _Func:
 def __getattr__(self,n):return lambda *a,**k:n
sa.func=_Func()
orm=types.ModuleType('sqlalchemy.orm')
orm.declarative_base=lambda:type('Base',(),{'__init__':lambda self,**k:None})
orm.sessionmaker=lambda *a,**k:(lambda *x,**y:None)
sys.modules['sqlalchemy']=sa;sys.modules['sqlalchemy.orm']=orm
# --- pymysql stub ------------------------------------------------------------
sys.modules['pymysql']=types.ModuleType('pymysql')
# --- project config ----------------------------------------------------------
db=types.ModuleType('src.config.database')
class FakeEngine:pass
db.engine=FakeEngine();db.Base=type('Base',(),{});db.Session=None
settings=types.ModuleType('src.config.settings')
settings.REDIS_URL='redis://test/0';settings.DATABASE_URL='mysql://x';settings.JWT_SECRET='t'
# --- model stub (avoids ORM import chain) ------------------------------------
lo=types.ModuleType('src.models.lock_observation')
class _Asc:
 def asc(self):return self
class LockObservation:
 # Class-level column placeholders mirroring SQLAlchemy descriptors.
 experiment_id=object();id=_Asc()
 def __init__(self,**k):self.__dict__.update(k)
lo.LockObservation=LockObservation
def install():
 sys.modules['src.config.database']=db
 sys.modules['src.config.settings']=settings
 sys.modules['src.models.lock_observation']=lo
 return db,settings,lo
class Clock:
 """Deterministic monotonic clock driven by the test."""
 def __init__(self):self.t=0.0
 def now(self):return self.t
 def advance(self,dt):self.t+=dt
class RecordingPersister:
 def __init__(self):self.records=[];self.fail=False
 def __call__(self,experiment_id,rec):
  if self.fail:raise RuntimeError('persister down')
  self.records.append((experiment_id,dict(rec)))
 def for_experiment(self,exp):return [r for e,r in self.records if e==exp]
class FlakySampler:
 """Raises for the first `fail_times` polls, then returns queued rows."""
 def __init__(self,rows,fail_times=1):self.rows=rows;self.fail_times=fail_times;self.calls=0
 def __call__(self):
  self.calls+=1
  if self.calls<=self.fail_times:raise RuntimeError('observer connection lost')
  return self.rows
def blocker_row(connection_id,blocker_conn=77,kind='metadata',name='telemetry'):
 return {'connection_id':connection_id,'lock_name':name,'lock_kind':kind,
  'blocker_thread_id':555,'blocker_connection_id':blocker_conn,
  'blocker_user':'trial','blocker_host':'10.0.0.9','blocker_db':'indextrial',
  'blocker_query':'ALTER TABLE telemetry ADD INDEX x(y)','blocker_lock_name':name}
def utc_now_iso():return datetime.datetime.now(datetime.timezone.utc).isoformat()
