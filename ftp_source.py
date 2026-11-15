import argparse,socket,socketserver,json,hashlib,threading
FILES={'data.bin':b'ABCD'*4096,'control.bin':b'XYZ'*128,'late.csv':b'id,value\n1,10\n2,20\n'}
ATTEMPTS={};LOCK=threading.Lock();GENERATION=1
class H(socketserver.StreamRequestHandler):
 def reply(self,s):self.wfile.write((s+'\r\n').encode());self.wfile.flush()
 def handle(self):
  global GENERATION
  self.reply('220 local dataset source');pasv=None;restart=0
  try:
   while True:
    line=self.rfile.readline()
    if not line:return
    cmd,*rest=line.decode().strip().split(' ',1);cmd=cmd.upper();arg=rest[0] if rest else ''
    if cmd=='USER':self.reply('331 password')
    elif cmd=='PASS':self.reply('230 logged in')
    elif cmd in ('TYPE','CWD'):self.reply('200 ok')
    elif cmd=='PWD':self.reply('257 "/"')
    elif cmd=='SYST':self.reply('215 UNIX Type: L8')
    elif cmd=='PASV':
     if pasv:pasv.close()
     pasv=socket.socket();pasv.bind(('127.0.0.1',0));pasv.listen(1);p=pasv.getsockname()[1];self.reply('227 Entering Passive Mode (127,0,0,1,%d,%d)'%(p//256,p%256))
    elif cmd=='REST':
     if self.server.profile=='b':self.reply('502 unsupported')
     else:
      try:restart=int(arg);assert restart>=0
      except (ValueError,AssertionError):self.reply('501 invalid offset');continue
      self.reply('350 restart position accepted')
    elif cmd=='SIZE':
     if self.server.profile=='b':self.reply('502 unsupported')
     elif arg in FILES:self.reply('213 '+str(len(FILES[arg])))
     else:self.reply('550 missing')
    elif cmd in ('RETR','MLSD','NLST'):
     if cmd=='MLSD' and self.server.profile=='b':self.reply('502 unsupported');continue
     if pasv is None:self.reply('425 need PASV');continue
     tail='226 transfer complete';change=False;used_generation=GENERATION;offset=restart;restart=0
     if cmd=='RETR':
      if arg=='descriptor.json':
       desc={'batch_id':'batch-'+str(GENERATION),'files':[{'id':str(i),'path':name,'generation':'g'+str(GENERATION),'size':len(v),'sha256':hashlib.sha256(v).hexdigest()} for i,(name,v) in enumerate(FILES.items())]};data=json.dumps(desc).encode()
      elif arg in FILES:
       with LOCK:ATTEMPTS[arg]=ATTEMPTS.get(arg,0)+1;n=ATTEMPTS[arg]
       if offset>len(FILES[arg]):self.reply('551 offset out of range');continue
       data=FILES[arg][offset:]
       if n==1 and arg=='data.bin' and offset==0:data=data[:4096];tail='426 data interrupted';change=self.server.change_after_first
       if n==1 and arg=='control.bin':tail='426 aborted after all data'
       if n==1 and arg=='late.csv':data=data[:len(data)//2]
      else:self.reply('550 missing');continue
     elif cmd=='NLST':data=b'data.bin\r\ncontrol.bin\r\ndescriptor.json\r\n'
     else:data=('type=file;size=%d;unique=g%d; data.bin\r\ntype=file;size=%d;unique=g%d; control.bin\r\ntype=file; descriptor.json\r\n'%(len(FILES['data.bin']),GENERATION,len(FILES['control.bin']),GENERATION)).encode()
     self.reply('150 opening data');pasv.settimeout(3);c,_=pasv.accept();c.sendall(data);c.close();pasv.close();pasv=None;self.reply(tail)
     if self.server.audit_file:
      with LOCK:
       with open(self.server.audit_file,'a') as audit:audit.write(json.dumps({'command':cmd,'path':arg,'rest':offset,'generation':used_generation,'bytes':len(data),'control':tail})+'\n')
     if change:
      with LOCK:FILES['data.bin']=b'WXYZ'*4096;GENERATION+=1
    elif cmd=='QUIT':self.reply('221 bye');return
    else:self.reply('502 unsupported')
  finally:
   if pasv:pasv.close()
class S(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--profile',choices=['a','b'],required=True);p.add_argument('--port',type=int,default=0);p.add_argument('--change-after-first',action='store_true');p.add_argument('--audit-file');a=p.parse_args();s=S(('127.0.0.1',a.port),H);s.profile=a.profile;s.change_after_first=a.change_after_first;s.audit_file=a.audit_file;print(s.server_address[1],flush=True);s.serve_forever()
