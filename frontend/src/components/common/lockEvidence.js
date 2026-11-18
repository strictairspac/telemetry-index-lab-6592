import {esc} from '../../utils/html.js';
import {LOCK_PHASES,LOCK_KINDS,OBSERVATION_STATES} from '../../constants/enums.js';
function blocker(b){if(!b)return '<span class="muted">断档：诊断连接失败，阻塞方未知</span>';return `<b>${esc(b.user||'未知用户')}@${esc(b.host||'未知来源')}</b><small>连接 #${esc(String(b.connection_id??'?'))} · 线程 ${esc(String(b.thread_id??'?'))} · ${esc(b.lock_name||'')}</small>${b.query?`<code>${esc(b.query)}</code>`:''}`;}
export function lockEvidence(evidence){
 if(!evidence)return '';
 const obs=evidence.observations||[];
 const s=evidence.summary||{count:0,blocked_count:0,unknown_count:0,released_count:0,has_blocker:false};
 const head=evidence.sealed_at?`<p class="muted">证据已于 ${esc(evidence.sealed_at)} 封存，迟到观测不再改写结论。</p>`:'<p class="muted live-note">试验进行中，证据实时刷新；结束时自动封存。</p>';
 if(!obs.length)return `<div class="lock-evidence"><h3>锁等待诊断</h3>${head}<p class="muted">截至目前各阶段均未出现满 3 秒的锁等待。</p></div>`;
 return `<div class="lock-evidence"><h3>锁等待诊断 <em>${s.blocked_count} 次阻塞 · ${s.unknown_count} 次断档</em></h3>${head}
 <table class="lock-table"><thead><tr><th>阶段</th><th>锁类型 / 状态</th><th>等待</th><th>实际阻塞方</th></tr></thead><tbody>${obs.map(o=>`<tr class="obs-${esc(o.state)}"><td>${esc(LOCK_PHASES[o.phase]||o.phase)}<small>本试验连接 #${esc(String(o.connection_id??'?'))}</small></td><td>${esc(LOCK_KINDS[o.lock_kind]||o.lock_kind)}<small>${esc(OBSERVATION_STATES[o.state]||o.state)}</small></td><td>${esc(String(o.waited_seconds))} s</td><td>${blocker(o.blocker)}</td></tr>`).join('')}</tbody></table></div>`;
}
export function lockBadge(evidence){
 const s=evidence?.summary;
 if(!s||!s.count)return '';
 if(s.unknown_count&&!s.has_blocker)return '<span class="lock-badge unknown">锁等待·阻塞方未知</span>';
 if(s.has_blocker)return '<span class="lock-badge blocked">锁等待·见阻塞方</span>';
 return '<span class="lock-badge released">锁等待·已释放</span>';
}
