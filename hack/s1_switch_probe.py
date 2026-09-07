"""Fresh native PG18 SWITCH/latest replay-position experiment, not product acceptance.
Only normal SQL, native backup/recovery and a test restore_command. Original
manifest/source WAL remain unchanged; the only fault input is post-EndLSN padding.
"""
import hashlib,json,os,re,shutil,subprocess,tarfile,tempfile,time
from pathlib import Path
B=Path(os.environ['PG_BIN']).resolve()
WORK=Path(__file__).resolve().parents[1]/'.work'
WORK.mkdir(exist_ok=True)
ROOT=Path(tempfile.mkdtemp(prefix='s1-switch-',dir=WORK))
ENV=dict(os.environ,LANG='C',LC_ALL='C')
os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:2])
COMMANDS=[]; SERVERS=[]; PORT='65435'; SEG=16<<20
TRIALS=int(os.environ.get('S1_TRIALS','3')); assert 1<=TRIALS<=3

def run(tool,*args,check=True):
 argv=[str(B/tool),*map(str,args)]
 p=subprocess.run(argv,env=ENV,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60)
 COMMANDS.append({'argv':argv,'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
 if check: assert p.returncode==0,COMMANDS[-1]
 return p

def sql(q): return run('psql','-XAtq','-h',ROOT,'-p',PORT,'-U','probe','-d','postgres','-v','ON_ERROR_STOP=1','-c',q).stdout.strip()
def lsn(s): a,b=s.split('/');return int(a,16)*2**32+int(b,16)
def fmt(n): return f'{n>>32:X}/{n&0xffffffff:X}'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def start(data,log):
 SERVERS.append(data)
 return run('pg_ctl','-D',data,'-l',log,'-t','15','-w','start',check=False)
def stop(data):return run('pg_ctl','-D',data,'-m','fast','-w','stop',check=False)
def verify(data,wal):
 run('pg_verifybackup','--exit-on-error','--no-parse-wal',data)
 for r in json.loads((data/'backup_manifest').read_text())['WAL-Ranges']:
  run('pg_waldump','--quiet','--path='+str(wal),'--timeline='+str(r['Timeline']),'--start='+r['Start-LSN'],'--end='+r['End-LSN'])
print(json.dumps({'root':str(ROOT),'trials':TRIALS,'version':run('postgres','--version').stdout.strip()}),flush=True)
results=[]
try:
 for trial in range(1,TRIALS+1):
  d=ROOT/str(trial);d.mkdir(); source=d/'source'
  run('initdb','-D',source,'-L',os.environ['PG_SHARE'],'-U','probe','-A','trust','--no-locale')
  with (source/'postgresql.conf').open('a') as f:
   f.write(f"\nlisten_addresses=''\nport={PORT}\nunix_socket_directories='{ROOT}'\nwal_level=replica\nmax_wal_senders=5\nmax_replication_slots=5\nshared_buffers='32MB'\ncheckpoint_timeout='30s'\ncheckpoint_completion_target=0.1\n")
  assert start(source,d/'source.log').returncode==0
  sql("CREATE TABLE s1_oracle(id int PRIMARY KEY, value text); INSERT INTO s1_oracle VALUES(1,'before-full')")
  original=d/'tar'
  run('pg_basebackup','-h',ROOT,'-p',PORT,'-U','probe','--no-password','--pgdata='+str(original),'--format=tar','--wal-method=stream','--checkpoint=spread','--manifest-checksums=SHA256')
  manifest=json.loads((original/'backup_manifest').read_text()); assert len(manifest['WAL-Ranges'])==1
  r=manifest['WAL-Ranges'][0]; end=lsn(r['End-LSN']);segno=(end-1)//SEG
  name=f"{r['Timeline']:08X}{segno//256:08X}{segno%256:08X}"
  seed=d/'seed'; seed.mkdir()
  with tarfile.open(original/'base.tar') as t:t.extractall(seed,filter='data')
  (seed/'pg_wal').mkdir(exist_ok=True)
  with tarfile.open(original/'pg_wal.tar') as t:t.extractall(seed/'pg_wal',filter='data')
  shutil.copyfile(original/'backup_manifest',seed/'backup_manifest')
  archive=d/'archive';archive.mkdir()
  for wal in (seed/'pg_wal').iterdir():
   if re.fullmatch('[0-9A-F]{24}',wal.name): shutil.copyfile(source/'pg_wal'/wal.name,archive/wal.name)
  # Freeze source files only after native capture has completed its switch.
  dump=run('pg_waldump','--path='+str(archive),'--start='+r['End-LSN'],'--limit=1',name).stdout
  (d/'switch.log').write_text(dump)
  assert re.search(r'lsn: ([0-9A-F]+/[0-9A-F]+).*desc: SWITCH',dump)
  assert lsn(re.search(r'lsn: ([0-9A-F]+/[0-9A-F]+)',dump)[1])==end
  assert stop(source).returncode==0
  verify(original,seed/'pg_wal'); verify(seed,seed/'pg_wal')
  archive_hash=sha(archive/name); manifest_hash=sha(seed/'backup_manifest')
  original_bundle_hash=sha(seed/'pg_wal'/name)
  padded=d/'padded';shutil.copytree(seed,padded)
  p=padded/'pg_wal'/name;raw=p.read_bytes();offset=end%SEG;assert 0<offset<SEG
  p.write_bytes(raw[:offset]+bytes(SEG-offset));assert sha(padded/'backup_manifest')==manifest_hash
  verify(padded,padded/'pg_wal')
  expected_replay=fmt((segno+1)*SEG)
  result={'trial':trial,'range':r,'filename':name,'expected_latest_replay_lsn':expected_replay,
          'original_manifest_sha256':manifest_hash,'source_final_wal_sha256':archive_hash,
          'original_bundle_sha256':original_bundle_hash,'padded_bundle_sha256':sha(p),
          'native_original_and_padded_range_verified':True,'cases':{},'product_acceptance':False}
  for mode in ('archive','wrong-bundle','required-missing','archive-after-negative'):
   data=d/('restore-'+mode);shutil.copytree(padded,data)
   helper=d/(mode+'.sh');requests=d/(mode+'-requests.log')
   src=archive if mode.startswith('archive') else padded/'pg_wal'
   action=(f'if test -f "{src}/$1"; then cp "{src}/$1" "$2" || exit 255; exit 0; fi; exit 1' if mode!='required-missing' else 'exit 255')
   helper.write_text(f'#!/bin/sh\nprintf "%s\\n" "$1" >> "{requests}"\ncase "$1" in *.history) exit 1;; esac\n{action}\n');helper.chmod(0o700)
   (data/'postgresql.auto.conf').write_text('');(data/'recovery.signal').touch()
   with (data/'postgresql.conf').open('a') as f:
    # NO explicit name/LSN/immediate target: ordinary supported latest recovery.
    f.write(f"\nlisten_addresses=''\nport={PORT}\nunix_socket_directories='{ROOT}'\narchive_mode=off\nrestore_command='exec {helper} \"%f\" \"%p\"'\nrecovery_target_timeline='1'\nrecovery_target_action='promote'\n")
   log=d/(mode+'.log');started=start(data,log)
   observed={'pg_ctl_exit':started.returncode,'replay_lsn':None,'promoted':False}
   if started.returncode==0:
    until=time.monotonic()+5
    while time.monotonic()<until:
     if sql('SELECT pg_is_in_recovery()')=='f':break
     time.sleep(.05)
    observed['promoted']=sql('SELECT pg_is_in_recovery()')=='f'
    observed['replay_lsn']=sql("SELECT COALESCE(pg_last_wal_replay_lsn()::text,'NULL')")
    observed['rows']=sql("SELECT string_agg(id::text||':'||value,',' ORDER BY id) FROM s1_oracle")
    observed['position_oracle_pass']=(observed['promoted'] and observed['rows']=='1:before-full' and observed['replay_lsn']==expected_replay)
    timeline=int(sql('SELECT timeline_id FROM pg_control_checkpoint()'))
    assert timeline>r['Timeline']
    history=(data/'pg_wal'/f'{timeline:08X}.history').read_text()
    entries=[line.split(None,2) for line in history.splitlines() if line.strip() and not line.lstrip().startswith('#')]
    assert len(entries)==1 and int(entries[-1][0])==r['Timeline']
    assert entries[-1][1]==observed['replay_lsn']
    observed.update(new_timeline=timeline,history=history,history_sha256=hashlib.sha256(history.encode()).hexdigest())
    assert stop(data).returncode==0
    if mode.startswith('archive'):
     assert start(data,d/'archive-normal-restart.log').returncode==0
     observed['normal_restart_replay_lsn']=sql("SELECT COALESCE(pg_last_wal_replay_lsn()::text,'NULL')")
     assert stop(data).returncode==0
   else:observed['position_oracle_pass']=False
   text=log.read_text();observed['redo_done']=re.findall(r'redo done at ([0-9A-F]+/[0-9A-F]+)',text)
   observed['requested_final_filename']=name in requests.read_text().splitlines()
   assert observed['requested_final_filename'],'archive-first fault precondition never fired'
   if mode.startswith('archive'): assert observed['position_oracle_pass'],(observed,text)
   elif mode=='wrong-bundle': assert not observed['position_oracle_pass'],'wrong bundle did not fail distinguishing oracle'
   else:assert started.returncode!=0 and 'FATAL' in text and '255' in text
   result['cases'][mode]=observed
  assert sha(archive/name)==archive_hash and sha(original/'backup_manifest')==manifest_hash
  (d/'result.json').write_text(json.dumps(result,indent=2)+'\n');results.append(result)
  print(json.dumps(result),flush=True)
 print(json.dumps({'distinguishing_trials':len(results),'product_acceptance':False}),flush=True)
finally:
 for data in SERVERS:
  if (data/'postmaster.pid').exists():stop(data)
 (ROOT/'commands.json').write_text(json.dumps(COMMANDS,indent=2)+'\n')
