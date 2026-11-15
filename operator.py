"""Persistent finite relation worker; deliberately not a feedback coordinator."""
import json,sys,threading,time,os
from pathlib import Path
kind=sys.argv[sys.argv.index('--kind')+1]
if kind not in ('expand','reduce'):raise ValueError('unknown operator')
contexts={};completed=set();state_lock=threading.Lock();output_lock=threading.Lock();workers=[]
def emit(f):
    with output_lock:print(json.dumps({'operator':kind,'pid':os.getpid(),**f}),flush=True)
def key(f):return f['run'],f['round']
def process(ctx):
    tag={'run':ctx['run'],'round':ctx['round']};rows=ctx['rows'];event=ctx['cancel']
    try:
        emit({'type':'input_closed',**tag,'received':len(rows)})
        if kind=='expand':
            out=[]
            for r in rows:
                out.append(dict(r))
                for e in ctx['edges']:
                    if e['src']==r['key']:
                        v=r['value'] if ctx['mode']=='min_label' else min(ctx['cap'],r['value']+e['delta'])
                        out.append({'key':e['dst'],'value':v})
        else:
            merged={}
            for r in rows:
                if r['key'] not in merged:merged[r['key']]=r['value']
                elif ctx['mode']=='min_label':merged[r['key']]=min(merged[r['key']],r['value'])
                else:merged[r['key']]=max(merged[r['key']],r['value'])
            out=[{'key':k,'value':v} for k,v in sorted(merged.items())]
        produced=0
        for i,r in enumerate(out):
            gate=ctx.get('tail_gate') if i==len(out)-1 else None
            if gate:
                emit({'type':'tail_wait',**tag,'produced':produced})
                while not Path(gate).exists() and not event.is_set():time.sleep(0.005)
            if event.is_set():break
            emit({'type':'batch',**tag,'rows':[r]});produced+=1
        if event.is_set():emit({'type':'cancelled',**tag,'produced':produced})
        else:emit({'type':'sealed',**tag,'produced':produced})
    finally:
        with state_lock:contexts.pop(key(ctx),None);completed.add(key(ctx))
for line in sys.stdin:
    f=json.loads(line);t=f['type'];k=key(f)
    if t=='begin':
        with state_lock:
            if k in contexts or k in completed:raise RuntimeError('duplicate context identity')
            contexts[k]={**f,'rows':[],'cancel':threading.Event(),'closed':False}
    elif t=='batch':
        with state_lock:
            c=contexts[k]
            if c['closed']:raise RuntimeError('input already closed')
            c['rows'].extend(f['rows'])
    elif t=='end':
        with state_lock:
            c=contexts[k]
            if c['closed']:raise RuntimeError('duplicate end')
            c['closed']=True
        w=threading.Thread(target=process,args=(c,));workers.append(w);w.start()
    elif t=='cancel':
        with state_lock:
            c=contexts.get(k)
            if c:c['cancel'].set()
        emit({'type':'cancel_requested','run':f['run'],'round':f['round'],'context_known':bool(c)})
        if c and not c['closed']:
            with state_lock:contexts.pop(k,None);completed.add(k)
            emit({'type':'cancelled','run':f['run'],'round':f['round'],'produced':0})
    else:raise RuntimeError('unknown frame')
for w in workers:w.join()
