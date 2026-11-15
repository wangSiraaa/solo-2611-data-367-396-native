def dist(a,b):return sum((x-y)**2 for x,y in zip(a,b))
def exact(rows,q):
    return sorted(((dist(r['vector'],q['vector']),r['id']) for r in rows if r['category']==q['category']))[:q['k']]
def global_then_filter(rows,q,budget):
    near=sorted(((dist(r['vector'],q['vector']),r) for r in rows),key=lambda x:(x[0],x[1]['id']))[:budget]
    return [(d,r['id']) for d,r in near if r['category']==q['category']][:q['k']]
