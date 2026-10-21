import importlib.util,json,tempfile,threading,unittest,urllib.error,urllib.request
from pathlib import Path
spec=importlib.util.spec_from_file_location('hook',Path(__file__).with_name('webhook_daemon.py'))
hook=importlib.util.module_from_spec(spec);spec.loader.exec_module(hook)

class HookTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();hook.STATE=Path(self.tmp.name);hook.DB_PATH=hook.STATE/'queue.sqlite3';hook.init_db()
  self.server=hook.ThreadingHTTPServer(('127.0.0.1',0),hook.Handler)
  self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
 def tearDown(self):
  self.server.shutdown();self.server.server_close();self.thread.join();self.tmp.cleanup()
 def request(self,path='/hook',data=None):
  try:
   r=urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{self.server.server_port}'+path,data=data),timeout=2)
  except urllib.error.HTTPError as e:r=e
  return r.status,r.read()
 def test_health(self):
  status,body=self.request('/health');self.assertEqual(status,200);self.assertEqual(json.loads(body)['pending'],0)
 def test_wrong_account(self):
  self.assertEqual(self.request(data=b'account[subdomain]=wrong&leads[add][0][id]=123')[0],403);self.assertEqual(hook.queue_size(),0)
 def test_valid_debounced_queue_only(self):
  payload=f'account[subdomain]={hook.ACCOUNT}&leads[add][0][id]=123'.encode()
  self.assertEqual(self.request(data=payload)[0],200);self.assertEqual(self.request(data=payload)[0],200);self.assertEqual(hook.queue_size(),1)
 def test_empty(self):self.assertEqual(self.request(data=b'')[0],413)
 def test_oversized(self):self.assertEqual(self.request(data=b'x'*(hook.MAX_BODY+1))[0],413)
 def test_other_path(self):self.assertEqual(self.request('/other',data=b'x')[0],404)
if __name__=='__main__':unittest.main()
