import redis
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.experiment import Experiment
from src.services.lock_evidence import live_view,observations_for,summarize
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
def list_items():
 with Session() as s:
  out=[]
  for r in s.query(Experiment).order_by(Experiment.id.desc()).limit(100).all():
   item=record(r)
   if r.lock_evidence is not None:item['lock_evidence']=r.lock_evidence
   else:
    obs=observations_for(s,r.id);item['lock_evidence']={'sealed_at':None,'observations':obs,'summary':summarize(obs)}
   out.append(item)
  return out
def detail(item_id):
 with Session() as s:
  row=s.get(Experiment,item_id)
  if not row:raise HTTPException(404,'未找到试验')
  item=record(row);item['lock_evidence']=live_view(s,row);return item
def create(data):
 with Session() as s:
  c=s.get(Candidate,data.candidate_id)
  if not c:raise HTTPException(404,'未找到候选')
  w=s.get(Workload,c.workload_id)
  row=Experiment(name=data.name,candidate_id=c.id,workload_id=w.id,status='queued',snapshot={'workload':record(w),'candidate':record(c)})
  s.add(row);s.commit();s.refresh(row);result=record(row)
 try:queue.rpush('index-trials',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Experiment,result['id']);r.status='failed';r.error='队列不可用，请创建新试验';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重新执行')
 return result
