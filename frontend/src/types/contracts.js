/** @typedef {{id:number,name:string,sensor_id:number,start_at:string,end_at:string,device_type:string|null}} Workload */
/** @typedef {{id:number,name:string,workload_id:number,columns:string[]}} Candidate */
/** @typedef {{id:number,phase:string,lock_kind:string,state:'blocked'|'released'|'unknown',waited_seconds:number,connection_id:number|null,blocker:object|null,observed_at:string|null}} LockObservation */
/** @typedef {{sealed_at:string|null,observations:LockObservation[],summary:{count:number,blocked_count:number,unknown_count:number,released_count:number,has_blocker:boolean,latest:LockObservation|null}}} LockEvidence */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null,lock_evidence:LockEvidence}} Experiment */
export {};
