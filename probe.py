"""Small full-snapshot oracle, not an incremental/session-publishing implementation."""
def sessions(events,gap):
    out=[]
    for key in sorted({r['key'] for r in events}):
        rows=sorted([r for r in events if r['key']==key],key=lambda r:(r['time'],r['event_id']))
        current=[]
        for row in rows:
            if current and row['time']-current[-1]['time']>gap:
                out.append(current);current=[]
            current.append(row)
        if current:out.append(current)
    return [{'key':g[0]['key'],'start':g[0]['time'],'end':g[-1]['time'],'ids':sorted(r['event_id'] for r in g),'sum':sum(r['value'] for r in g)} for g in out]
