import json
from pathlib import Path
def edge(i,a,b,label,cost=1):return {'edge_id':i,'src':a,'dst':b,'label':label,'cost':cost}
def generate():
 rows=[edge('f-own-1','sF','aF','owns'),edge('f-own-2','sF','aF','owns',2),edge('f-use','aF','bF','uses',-2),edge('f-place','bF','zF','located')]
 for i in range(300):rows.append(edge('f-noise-'+str(i),'bNoise'+str(i),'zF','located'))
 for i in range(200):
  rows.extend([edge('b-own-'+str(i),'sB','aB'+str(i),'owns'),edge('b-use-'+str(i),'aB'+str(i),'bB'+str(i),'uses'),edge('b-place-'+str(i),'bB'+str(i),'zB' if i==0 else 'zOther'+str(i),'located')])
 # A path candidate which repeats its start must not become a legal simple path.
 rows.extend([edge('cycle-1','sC','aC','owns'),edge('cycle-2','aC','sC','uses'),edge('cycle-3','sC','zC','located')])
 return rows
if __name__=='__main__':
 p=Path(__file__).resolve().parent/'private/edges.json';p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(generate(),sort_keys=True,separators=(',',':')))
