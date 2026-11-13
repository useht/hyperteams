module.exports=[750227,(e,t,r)=>{t.exports=e.x("node:path",()=>require("node:path"))},951389,(e,t,r)=>{t.exports=e.x("better-sqlite3-a9b1042fd0ef418e",()=>require("better-sqlite3-a9b1042fd0ef418e"))},254799,(e,t,r)=>{t.exports=e.x("crypto",()=>require("crypto"))},522734,(e,t,r)=>{t.exports=e.x("fs",()=>require("fs"))},446786,(e,t,r)=>{t.exports=e.x("os",()=>require("os"))},233405,(e,t,r)=>{t.exports=e.x("child_process",()=>require("child_process"))},144407,e=>{"use strict";var t=e.i(960251),r=e.i(947788),n=e.i(726133),s=e.i(779291);let a=/^(\d{4})-(\d{2})-(\d{2})$/,o=e=>String(e).padStart(2,"0");function i(){return!!(0,s.linkConfigured)()||null!=r.db.get(t.sql`SELECT 1 AS n FROM app_settings WHERE substr(key, 1, 5) IN ('link:', 'sync:') LIMIT 1`)}function l(e){return t.sql`
    t.status IN ('done', 'stopped', 'error')
    AND t.updated_at < ${e}
    AND t.resume_after IS NULL
    AND (t.handoff IS NULL OR t.handoff = '')
    AND t.crew_id IS NULL AND t.crew_role IS NULL
    AND t.worktree_path IS NULL
    AND t.land_started_at IS NULL AND t.settle_started_at IS NULL
    AND NOT EXISTS (SELECT 1 FROM task_queue q WHERE q.task_id = t.id)
  `}let u=()=>new Promise(e=>setImmediate(e));async function d(e,n){let s=0;for(;!i();){let a=r.db.run(t.sql`
      DELETE FROM messages
      WHERE id IN (
        SELECT m.id FROM messages m JOIN tasks t ON t.id = m.task_id
        WHERE m.task_id = ${e} AND ${l(n)}
        LIMIT ${2e3}
      )
    `);if(a.changes<=0||(s+=a.changes,a.changes<2e3))break;await u()}return s}async function m(e,s=new Date){let u,c=(u=(0,n.getSetting)("data:retentionBefore")?.trim())&&/^\d{4}-\d{2}-\d{2}$/.test(u)?u:null;if(!c)return null;let f=function(e,t){let r;if("string"!=typeof e)return{ok:!1,problem:"format"};let n=a.exec(e);if(!n)return{ok:!1,problem:"format"};let s=Number(n[1]),i=Number(n[2]),l=Number(n[3]),u=new Date(s,i-1,l);return s<1970||u.getFullYear()!==s||u.getMonth()!==i-1||u.getDate()!==l?{ok:!1,problem:"calendar"}:e>=`${t.getFullYear()}-${o(t.getMonth()+1)}-${o(t.getDate())}`?{ok:!1,problem:"notPast"}:{ok:!0,date:e,cutoff:(r=a.exec(e))?Math.floor(new Date(Number(r[1]),Number(r[2])-1,Number(r[3])).getTime()/1e3):NaN}}(c,s);if(!f.ok)return null;let g=f.cutoff,E=Math.floor(s.getTime()/1e3),N={tasks:0,messages:0,terminalRuns:0,commandLog:0,keptForConnect:i()};if(!N.keptForConnect)for(let e of r.db.all(t.sql`
        SELECT t.id AS id FROM tasks t
        WHERE ${l(g)}
          AND EXISTS (SELECT 1 FROM messages m WHERE m.task_id = t.id)
      `).map(e=>e.id)){let t=await d(e,g);t>0&&(N.messages+=t,N.tasks+=1)}N.terminalRuns=r.db.run(t.sql`
    DELETE FROM terminal_runs
    WHERE status != 'running' AND coalesce(ended_at, started_at) < ${g}
  `).changes,N.commandLog=r.db.run(t.sql`
    DELETE FROM link_command_log WHERE created_at < ${Math.min(g,E-604800)}
  `).changes;let p={...N,date:c,at:E},L=N.messages+N.terminalRuns+N.commandLog>0;return(e||L)&&(0,n.setSetting)("data:retentionLastRun",JSON.stringify(p)),L&&console.log(`[retention] ${c} 이전 기록 삭제 — 업무 ${N.tasks}개의 메시지 ${N.messages}건, 터미널 실행 ${N.terminalRuns}건, 명령 장부 ${N.commandLog}건`),p}let c=globalThis,f=c.__retentionSweep??={chain:Promise.resolve(),timer:null};function g(e={}){let t=f.chain.then(()=>m(!0===e.saved)).catch(e=>(console.error("[retention] 삭제 실패:",e instanceof Error?e.message:e),null));return f.chain=t,t}e.s(["startRetentionSweeper",0,function(){f.timer||(g(),f.timer=setInterval(()=>void g(),864e5),f.timer.unref())}],144407)}];