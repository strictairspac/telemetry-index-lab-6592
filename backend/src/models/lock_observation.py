from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey,Index,Numeric
from src.config.database import Base
class LockObservation(Base):
 __tablename__='lock_observations'
 id=Column(Integer,primary_key=True)
 experiment_id=Column(Integer,ForeignKey('experiments.id'),nullable=False)
 phase=Column(String(40),nullable=False)
 lock_kind=Column(String(20),nullable=False)
 state=Column(String(20),nullable=False)
 waited_seconds=Column(Numeric(8,3),nullable=False,default=0)
 connection_id=Column(Integer,nullable=True)
 blocker=Column(JSON,nullable=True)
 observed_at=Column(DateTime,server_default=func.now())
 sealed=Column(Integer,nullable=False,default=0)
 __table_args__=(Index('ix_lock_obs_experiment','experiment_id','id'),)
