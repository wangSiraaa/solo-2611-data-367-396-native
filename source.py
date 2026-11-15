from fractions import Fraction as F
import json

def fixture_rows():
 rows=[]
 for kind in ('interval_total','interval_mean','instant'):
  for i in range(80):
   if i%11==0:continue
   for repeat in range(2 if kind=='instant' else 1):
    r={'stream_id':kind,'stable_id':kind+':'+str(i)+':'+str(repeat),'kind':kind,'value':None if i%7==0 else str(i%9+repeat+1)}
    r.update({'t':i} if kind=='instant' else {'a':i,'b':i+1});rows.append(r)
 rows.extend([{'stream_id':'long','stable_id':'long:1','kind':'interval_total','a':0,'b':19,'value':'190'},{'stream_id':'long','stable_id':'long:2','kind':'interval_total','a':19,'b':43,'value':'48'},{'stream_id':'long','stable_id':'long:3','kind':'interval_total','a':43,'b':50,'value':None},{'stream_id':'long','stable_id':'long:4','kind':'interval_total','a':55,'b':80,'value':'25'}])
 return rows

def state(rows,stream,a,b):
 kind='interval_total' if stream=='long' else stream
 s={'known_duration':0,'unknown_duration':0,'unobserved_duration':0,'point_count':0,'non_null_points':0,'numerator':'0'};num=F(0)
 for r in rows:
  if r['stream_id']!=stream:continue
  if kind=='instant':
   if not a<=r['t']<b:continue
   s['point_count']+=1
   if r['value'] is not None:s['non_null_points']+=1;num+=F(r['value'])
  else:
   overlap=max(0,min(b,r['b'])-max(a,r['a']))
   if not overlap:continue
   if r['value'] is None:s['unknown_duration']+=overlap
   else:
    s['known_duration']+=overlap;num+=F(r['value'])*overlap/(r['b']-r['a']) if kind=='interval_total' else F(r['value'])*overlap
 if kind!='instant':s['unobserved_duration']=b-a-s['known_duration']-s['unknown_duration']
 s['numerator']=str(num);return s

class Source:
 def __init__(self):
  self._rows=fixture_rows();self.calls=[]
  self._summaries={}
  for stream in ('interval_total','interval_mean','instant','long'):
   for width in (10,40):
    for a in range(0,80,width):self._summaries[(stream,width,a)]={'snapshot':'time-v1','stream_id':stream,'kind':'interval_total' if stream=='long' else stream,'tile':[a,a+width],'state_profile':'exact-sufficient/v1','state':state(self._rows,stream,a,a+width)}
 def catalog(self):return {'snapshot':'time-v1','coverage':[0,80],'widths':[10,40],'streams':['interval_total','interval_mean','instant','long'],'stream_kinds':{'interval_total':'interval_total','interval_mean':'interval_mean','instant':'instant','long':'interval_total'},'summary_profile':'exact-sufficient/v1'}
 def read_summary(self,stream,width,a):
  item=self._summaries[(stream,width,a)];raw=json.dumps(item,separators=(',',':')).encode();self.calls.append({'op':'summary','stream':stream,'tile':[a,a+width],'bytes':len(raw)});return json.loads(raw)
 def read_raw(self,stream,a,b):
  rows=[r for r in self._rows if r['stream_id']==stream and (a<=r['t']<b if r['kind']=='instant' else r['a']<b and a<r['b'])];raw=json.dumps(rows,separators=(',',':')).encode();self.calls.append({'op':'raw','stream':stream,'range':[a,b],'rows':len(rows),'bytes':len(raw)});return json.loads(raw)
 def inspect_calls(self):return list(self.calls)
