"""Run on server after code upload; preserve runtime secrets in Docker memory."""
import http.client,json,socket,pathlib,subprocess
class Docker(http.client.HTTPConnection):
 def connect(self):
  self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.sock.connect('/var/run/docker.sock')
def request(method,path,body=None):
 c=Docker('localhost');c.request(method,path,body=json.dumps(body) if body is not None else None,headers={'Content-Type':'application/json'});r=c.getresponse();b=r.read()
 if r.status>=300: raise RuntimeError(f'Docker {method} {path}: HTTP {r.status}')
 return json.loads(b) if b else {}
overlay=pathlib.Path(__file__).resolve().parent
backup=pathlib.Path('/opt/backups/academy-enable-20260925-1820')
for old,new,port,main in [('amo-fix-fields-academy-alert-v4','amo-fix-fields-academy-delivery-v5','8026',True),('amo-academy-webhook-v10-dedup-lock','amo-academy-webhook-v11-enabled','8027',False)]:
 a=request('GET','/containers/'+old+'/json');p=backup/(old+'.json');p.write_text(json.dumps(a));p.chmod(0o600)
 image=a['Image']
 if main:
  (overlay/'Dockerfile').write_text('FROM '+image+'\nCOPY academy_invite_delivery.py academy_invite_link.py academy_intent_alert.py waybill_config.py webhooks.py /app/\n')
  image='amo-fix-fields:academy-delivery-v5'
  subprocess.run(['docker','build','-t',image,str(overlay)],check=True)
 env=dict(x.split('=',1) for x in a['Config']['Env'])
 prefix='/app/var/academy/' if main else '/app/var/'
 env.update(ACADEMY_INVITE_SEND_ENABLED='1',ACADEMY_INVITE_WAZZUP_CHANNEL_ID='782075b4-137e-43b2-839e-8ff21232d7df',ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID='79250833349',ACADEMY_INVITE_OUTBOX_PATH=prefix+'academy_invite_outbox_v11.sqlite3',ACADEMY_INVITE_HISTORY_REVIEW_PATH=prefix+'academy_invite_history_reviews_v11.json',ACADEMY_INVITE_SENT_PATH=prefix+'academy_invite_sent.json')
 config={k:v for k,v in a['Config'].items() if k in ['Cmd','Entrypoint','WorkingDir','User','ExposedPorts']};config.update(Image=image,Env=[k+'='+v for k,v in env.items()]);h=a['HostConfig'];h['PortBindings']={'8000/tcp':[{'HostIp':'127.0.0.1','HostPort':port}]};config['HostConfig']=h
 r=request('POST','/containers/create?name='+new,config);request('POST','/containers/'+r['Id']+'/start');print('started',new,port)
