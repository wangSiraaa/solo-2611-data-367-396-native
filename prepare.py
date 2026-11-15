import sys,json
from pathlib import Path
from shard import Shard
def prepare(root):
 root=Path(root);root.mkdir(parents=True,exist_ok=True)
 source=Shard(root/'shard-a.sqlite');target=Shard(root/'shard-b.sqlite')
 source.initialize_tenant('moving',1,True);source.initialize_tenant('stay',1,True);target.initialize_tenant('moving',1,False)
 for i in range(8):source.write('moving',1,'seed-'+str(i),{'op':'put','row_id':'r'+str(i),'value':i*10})
 source.write('stay',1,'stay-seed',{'op':'put','row_id':'s1','value':999})
 (root/'routing.json').write_text(json.dumps({'moving':{'owner':'shard-a','generation':1},'stay':{'owner':'shard-a','generation':1}}))
 return source,target
if __name__=='__main__':prepare(sys.argv[1])
