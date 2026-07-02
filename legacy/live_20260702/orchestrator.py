#!/usr/bin/env python3
import argparse, json, os, sys, time, sqlite3, subprocess, urllib.parse, shutil, fnmatch, re, hashlib, importlib.util
from pathlib import Path
from datetime import datetime

from junk_rules import text_link_junk_reason

GB = 1024**3
MB = 1024**2

def now(): return int(time.time())
def iso(ts=None): return datetime.fromtimestamp(ts or now()).strftime('%Y-%m-%d %H:%M:%S')

def parse_duration(s):
    if isinstance(s, (int, float)): return int(s)
    s=str(s).strip().lower()
    m=re.fullmatch(r'(\d+)(s|m|h|d)?', s)
    if not m: raise ValueError(f'bad duration: {s}')
    n=int(m.group(1)); u=m.group(2) or 's'
    return n * {'s':1,'m':60,'h':3600,'d':86400}[u]

def parse_speed(s):
    if isinstance(s, (int, float)): return int(s)
    s=str(s).strip().lower().replace('/s','')
    m=re.fullmatch(r'(\d+(?:\.\d+)?)(b|kib|kb|mib|mb|gib|gb)?', s)
    if not m: raise ValueError(f'bad speed: {s}')
    n=float(m.group(1)); u=m.group(2) or 'b'
    mult={'b':1,'kb':1000,'mb':1000**2,'gb':1000**3,'kib':1024,'mib':1024**2,'gib':1024**3}[u]
    return int(n*mult)

def safe_name(s, maxlen=96):
    s=re.sub(r'[\\/:*?"<>|\x00-\x1f]+','_',s).strip().strip('.') or 'torrent'
    return s[:maxlen]

class Orchestrator:
    def __init__(self, cfg):
        self.cfg=cfg
        self.mode=cfg.get('mode','live')
        self.qbt=cfg['qbt']; self.paths=cfg['paths']; self.rclone=cfg['rclone']
        Path(self.paths['work_dir']).mkdir(parents=True, exist_ok=True)
        Path(self.paths['log_file']).parent.mkdir(parents=True, exist_ok=True)
        Path(self.qbt['torrent_store_host']).mkdir(parents=True, exist_ok=True)
        self.db=sqlite3.connect(self.paths['state_db'])
        self.db.row_factory=sqlite3.Row
        self.init_db()

    def log(self, msg):
        line=f'[{iso()}] {msg}'
        print(line)
        with open(self.paths['log_file'],'a',encoding='utf-8') as f: f.write(line+'\n')

    def init_db(self):
        self.db.executescript('''
        create table if not exists torrent_state (
          hash text primary key,
          name text,
          mode text,
          archived_indices text default '[]',
          skipped_indices text default '[]',
          current_batch text,
          batch_no integer default 0,
          seed_start integer,
          last_uploaded integer default 0,
          idle_since integer,
          added_on integer,
          done integer default 0,
          updated_at integer
        );
        create table if not exists events (
          id integer primary key autoincrement,
          ts integer,
          hash text,
          level text,
          message text
        );
        ''')
        self.db.commit()
        self.ensure_state_columns()

    def ensure_state_columns(self):
        existing={r[1] for r in self.db.execute('pragma table_info(torrent_state)')}
        columns={
          'last_completed':'integer default 0',
          'last_progress':'real default 0',
          'last_dlspeed_ema':'real default 0',
          'last_dl_check':'integer default 0',
          'slow_since':'integer',
          'slow_start_completed':'integer default 0',
          'slow_strikes':'integer default 0',
          'cooldown_until':'integer default 0',
          'probe_started':'integer',
          'slot_kind':'text'
        }
        for name, decl in columns.items():
            if name not in existing:
                self.db.execute(f'alter table torrent_state add column {name} {decl}')
        self.db.commit()

    def event(self, h, level, msg):
        self.db.execute('insert into events(ts,hash,level,message) values(?,?,?,?)',(now(),h,level,msg))
        self.db.commit()
        self.log(f'{level} {h[:8] if h else "-"}: {msg}')

    def docker_curl(self, method, path, data=None, files=None, ok=(200,)):
        url=self.qbt['api_base']+path
        cmd=['docker','exec',self.qbt['container'],'curl','-sS','-w','\n%{http_code}', '-X', method]
        if files:
            # multipart request, used by /torrents/add; send normal fields as -F too
            if data:
                for k,v in data.items():
                    if isinstance(v, (dict, list)):
                        v=json.dumps(v, separators=(',',':'))
                    cmd += ['-F', f'{k}={v}']
            for k,v in files.items():
                cmd += ['-F', f'{k}=@{v}']
        elif data:
            for k,v in data.items():
                if isinstance(v, (dict, list)):
                    v=json.dumps(v, separators=(',',':'))
                cmd += ['--data-urlencode', f'{k}={v}']
        cmd.append(url)
        p=subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
        out=p.stdout
        if '\n' not in out:
            code='000'; body=out
        else:
            body, code = out.rsplit('\n',1)
        try: c=int(code.strip())
        except: c=0
        if p.returncode != 0 or c not in ok:
            raise RuntimeError(f'curl {method} {path} failed rc={p.returncode} http={c} stderr={p.stderr.strip()} body={body[:300]}')
        return body

    def qget(self, path, params=None):
        if params:
            path += '?' + urllib.parse.urlencode(params, doseq=True)
        return self.docker_curl('GET', path)
    def qpost(self, path, data=None, files=None, ok=(200,)):
        return self.docker_curl('POST', path, data=data or {}, files=files, ok=ok)
    def qjson(self, path, params=None):
        txt=self.qget(path, params)
        return json.loads(txt) if txt else None

    def start_torrent(self,h):
        for ep in ['/api/v2/torrents/start','/api/v2/torrents/resume']:
            try: self.qpost(ep, {'hashes':h}); return
            except Exception: pass
        raise RuntimeError('cannot start torrent')
    def stop_torrent(self,h):
        for ep in ['/api/v2/torrents/stop','/api/v2/torrents/pause']:
            try: self.qpost(ep, {'hashes':h}); return
            except Exception: pass
        raise RuntimeError('cannot stop torrent')

    def dynamic_download_limit(self, free=None):
        sched=self.cfg.get('scheduler', {})
        if not sched.get('dynamic_downloads_enabled', False):
            return int(sched.get('max_active_downloads', 3))
        if free is None:
            free=self.free_gb()
        disk=self.cfg.get('disk', {})
        if free < disk.get('pause_new_free_below_gb', 4):
            return 0
        usable = free - disk.get('target_min_free_gb', 5) - disk.get('batch_overhead_gb', 1)
        per = max(float(sched.get('dynamic_gb_per_download', 3)), 0.5)
        raw = int(usable // per)
        if raw <= 0:
            return 0
        return max(int(sched.get('min_active_downloads', 1)), min(int(sched.get('max_active_downloads', 8)), raw))

    def is_download_active(self, t):
        state=(t.get('state') or '').lower()
        active_states={'downloading','forceddl','stalleddl','metadl','checkingdl','allocating'}
        return state in active_states or int(t.get('dlspeed') or 0) > 0

    def slow_cfg(self):
        return self.cfg.get('slow_policy', {}) or {}

    def completed_bytes(self, t):
        completed=int(t.get('completed') or 0)
        if completed > 0:
            return completed
        size=int(t.get('size') or t.get('total_size') or 0)
        progress=max(0.0, min(1.0, float(t.get('progress') or 0)))
        return int(size * progress)

    def is_cooling(self, st):
        sp=self.slow_cfg()
        if not sp.get('enabled', False):
            return False
        return int(st.get('cooldown_until') or 0) > now()

    def cooldown_left(self, st):
        return max(0, int(st.get('cooldown_until') or 0) - now())

    def update_download_health(self, t, files, st):
        sp=self.slow_cfg()
        if not sp.get('enabled', False):
            return st
        h=t['hash']
        if self.torrent_complete(t, files):
            return st
        n=now()
        completed=self.completed_bytes(t)
        progress=float(t.get('progress') or 0)
        dlspeed=int(t.get('dlspeed') or 0)
        active=self.is_download_active(t)
        last_check=int(st.get('last_dl_check') or 0)
        old_ema=float(st.get('last_dlspeed_ema') or 0)
        ema=dlspeed if old_ema <= 0 else (old_ema * 0.65 + dlspeed * 0.35)

        updates={'last_completed':completed,'last_progress':progress,'last_dlspeed_ema':ema,'last_dl_check':n}
        if not active:
            # If it is not actively downloading, keep cooldown data but reset the active-window timer.
            if not self.is_cooling(st) and st.get('slot_kind') not in {'stable','probe','overflow'}:
                updates.update({'probe_started':None,'slow_since':None})
            self.put_state(h, **updates)
            st.update(updates)
            return st

        started=int(st.get('probe_started') or 0) or n
        if not st.get('probe_started'):
            updates['probe_started']=started
        runtime=n-started
        min_probe=parse_duration(sp.get('min_probe_time','20m'))
        min_speed=int(sp.get('min_speed_bytes_per_sec', 262144))
        no_progress_timeout=parse_duration(sp.get('no_progress_timeout','30m'))
        min_progress=int(sp.get('min_progress_bytes', 268435456))

        slow_since=st.get('slow_since')
        slow_start_completed=int(st.get('slow_start_completed') or completed)
        low_speed=ema < min_speed
        if runtime >= min_probe and low_speed:
            if not slow_since:
                slow_since=n
                slow_start_completed=completed
                updates.update({'slow_since':slow_since,'slow_start_completed':slow_start_completed})
            progressed=completed - slow_start_completed
            if n-int(slow_since) >= no_progress_timeout and progressed < min_progress:
                strikes=int(st.get('slow_strikes') or 0) + 1
                max_strikes=int(sp.get('max_slow_strikes',3))
                cd=parse_duration(sp.get('long_cooldown','12h') if strikes >= max_strikes else sp.get('cooldown','2h'))
                cooldown_until=n+cd
                try:
                    self.stop_torrent(h)
                except Exception as e:
                    self.event(h,'ERROR',f'slow cooldown failed to pause: {e}')
                updates.update({
                    'slow_strikes':strikes,
                    'cooldown_until':cooldown_until,
                    'slow_since':None,
                    'slow_start_completed':completed,
                    'probe_started':None,
                    'slot_kind':'cooldown'
                })
                self.event(h,'INFO',f'slow download cooldown: speed_ema={ema/1024:.1f}KiB/s, progress={progressed/MB:.1f}MiB/{(n-int(slow_since))//60}m, strikes={strikes}, cooldown={cd//60}m')
        else:
            if slow_since and (not low_speed or completed - slow_start_completed >= min_progress):
                updates.update({'slow_since':None,'slow_start_completed':completed})

        self.put_state(h, **updates)
        st.update(updates)
        return st

    def ensure_qbt_basics(self):
        # Create category and set default paths. Ignore already-exists errors.
        try: self.qpost('/api/v2/torrents/createCategory', {'category':self.qbt['category_auto'], 'savePath':self.qbt['save_path']}, ok=(200,409))
        except Exception as e: self.log(f'WARN createCategory: {e}')
        prefs={
          'save_path': self.qbt['save_path'],
          'temp_path_enabled': True,
          'temp_path': self.qbt['temp_path'],
          'auto_tmm_enabled': False,
          'preallocate_all': False
        }
        sched=self.cfg.get('scheduler', {})
        if sched.get('dynamic_set_qbt_queue_limits', False) and not (sched.get('size_aware_enabled') or sched.get('planner') == 'size_aware'):
            limit=self.dynamic_download_limit()
            qbt_limit=max(1, limit)
            prefs.update({
              'queueing_enabled': True,
              'max_active_downloads': qbt_limit,
              'max_active_torrents': max(20, qbt_limit + int(sched.get('qbt_extra_active_torrents', 12))),
              'max_active_uploads': max(10, qbt_limit + int(sched.get('qbt_extra_active_torrents', 12)) // 2),
              'preallocate_all': False
            })
            self.download_limit=limit
            self.log(f'dynamic download limit={limit}, qbt_max_active_downloads={qbt_limit}, free={self.free_gb():.1f}G')
        try: self.qpost('/api/v2/app/setPreferences', {'json':prefs})
        except Exception as e: self.log(f'WARN setPreferences: {e}')

    def get_state(self,h,name=None,added_on=0):
        r=self.db.execute('select * from torrent_state where hash=?',(h,)).fetchone()
        if not r:
            self.db.execute('insert into torrent_state(hash,name,added_on,updated_at) values(?,?,?,?)',(h,name or '',added_on,now()))
            self.db.commit(); r=self.db.execute('select * from torrent_state where hash=?',(h,)).fetchone()
        return dict(r)
    def put_state(self,h,**kw):
        kw['updated_at']=now()
        sets=','.join([f'{k}=?' for k in kw])
        self.db.execute(f'update torrent_state set {sets} where hash=?', list(kw.values())+[h])
        self.db.commit()

    def managed(self,t):
        tags=set(x.strip() for x in (t.get('tags') or '').split(',') if x.strip())
        if self.qbt['tag_hold'] in tags: return False
        return t.get('category') == self.qbt['category_auto'] or self.qbt['tag_auto'] in tags


    def add_tags(self,h,tags):
        tags=[str(t) for t in tags if str(t)]
        if tags:
            self.qpost('/api/v2/torrents/addTags', {'hashes':h, 'tags':','.join(tags)})

    def remove_tags(self,h,tags):
        tags=[str(t) for t in tags if str(t)]
        if tags:
            self.qpost('/api/v2/torrents/removeTags', {'hashes':h, 'tags':','.join(tags)})

    def set_category(self,h,category):
        self.qpost('/api/v2/torrents/setCategory', {'hashes':h, 'category':category})

    def set_force_start(self,h,value):
        self.qpost('/api/v2/torrents/setForceStart', {'hashes':h, 'value':'true' if value else 'false'})

    def set_sequential_download(self,h,enabled=True):
        try:
            info=self.qjson('/api/v2/torrents/info', {'hashes':h})
            if info:
                current=bool(info[0].get('seq_dl'))
                if current == bool(enabled):
                    return False
        except Exception as e:
            self.event(h,'ERROR',f'failed to read sequential download state: {e}')
            return False
        self.qpost('/api/v2/torrents/toggleSequentialDownload', {'hashes':h})
        return True

    def set_observe_file_priorities(self,h,files):
        min_bytes=int(self.cfg.get('batching',{}).get('min_file_size_mb_for_main',100))*MB
        keep=[]
        for i,f in enumerate(files or []):
            name=f.get('name','')
            size=int(f.get('size') or 0)
            if self.is_video_file(name) and size >= min_bytes and not self.junk(name,size):
                keep.append(i)
        if not keep:
            self.event(h,'WARN','observe promotion found metadata but no main video candidate; leaving file priorities unchanged')
            return
        drop=sorted(set(range(len(files))) - set(keep))
        if drop:
            self.set_file_prio(h, drop, '0')
        self.set_file_prio(h, keep, '1')
        self.event(h,'INFO',f'observe file priorities set: keep={keep}, drop_count={len(drop)}')

    def promote_observe_if_ready(self,t):
        h=t.get('hash')
        tags=self.tags(t)
        if 'observe' not in tags:
            return False
        try:
            files=self.qjson('/api/v2/torrents/files', {'hash':h}) or []
        except Exception as e:
            self.event(h,'ERROR',f'observe metadata check failed: {e}')
            return False
        has_file_names=bool(files) and any((f.get('name') or '') for f in files)
        known_size=int(t.get('total_size') or t.get('size') or 0) > 0 or any(int(f.get('size') or 0) > 0 for f in files)
        if not (has_file_names and known_size):
            # Keep it running. qBT/DHT should continue trying to acquire metadata.
            try:
                state=str(t.get('state') or '').lower()
                self.set_force_start(h, True)
                if state.startswith('stopped') or state in {'pauseddl','pausedup'}:
                    self.start_torrent(h)
                    self.event(h,'INFO','observe torrent restarted while waiting for metadata')
            except Exception as e:
                self.event(h,'ERROR',f'observe restart failed: {e}')
            return False
        self.set_observe_file_priorities(h, files)
        clear=sorted({'hold','precheck','metadata-timeout','observe'})
        self.remove_tags(h, clear)
        self.add_tags(h, ['auto','checked'])
        self.set_category(h, self.qbt['category_auto'])
        self.set_force_start(h, False)
        try:
            self.stop_torrent(h)
        except Exception as e:
            self.event(h,'ERROR',f'observe promotion stop-for-planner failed: {e}')
        self.event(h,'INFO',f'observe metadata ready; promoted to auto workflow, files={len(files)}')
        return True

    def free_gb(self):
        u=shutil.disk_usage(self.paths['host_downloads'])
        return u.free / GB

    def content_host_path(self,t):
        cp=t.get('content_path') or ''
        if cp.startswith('/downloads'):
            return Path(self.paths['host_downloads'] + cp[len('/downloads'):])
        sp=t.get('save_path') or self.qbt['save_path']
        if sp.startswith('/downloads'):
            return Path(self.paths['host_downloads'] + sp[len('/downloads'):]) / t.get('name','')
        return Path(self.paths['host_active']) / t.get('name','')

    def backup_torrent_file(self,h):
        src=Path(self.qbt['bt_backup_host'])/f'{h}.torrent'
        dst=Path(self.qbt['torrent_store_host'])/f'{h}.torrent'
        if src.exists() and not dst.exists():
            shutil.copy2(src,dst)
            try: os.chown(dst,1000,1000)
            except Exception: pass
        return dst.exists()

    def tags(self,t): return set(x.strip() for x in (t.get('tags') or '').split(',') if x.strip())

    def is_huge(self,t,files):
        if self.qbt['tag_no_batch'] in self.tags(t): return False
        if not self.cfg['batching']['enabled']: return False
        return t.get('total_size',0) >= self.cfg['batching']['huge_torrent_threshold_gb']*GB and len(files) > 1

    def cover_cfg(self):
        return self.cfg.get('cover_policy', {}) or {}

    def norm_rel(self, name):
        return str(name or '').replace('\\','/').lstrip('/')

    def match_any_regex(self, patterns, text):
        return any(re.search(p, text or '') for p in patterns or [])

    def file_ext(self, name):
        base=os.path.basename(str(name or '')).lower()
        return ('.' + base.rsplit('.',1)[-1]) if '.' in base else ''

    def is_video_file(self, name):
        cp=self.cover_cfg()
        text=self.norm_rel(name)
        pats=cp.get('video_ext_regex') or [r'(?i)\.(mp4|mkv|avi|mov|wmv|ts|m4v)$']
        return self.match_any_regex(pats, text)

    def is_image_file(self, name):
        cp=self.cover_cfg()
        text=self.norm_rel(name)
        pats=cp.get('image_ext_regex') or [r'(?i)\.(jpg|jpeg|png|webp)$']
        return self.match_any_regex(pats, text)

    def is_hard_junk(self, name):
        cp=self.cover_cfg()
        text=self.norm_rel(name)
        base=os.path.basename(text)
        if text_link_junk_reason(text):
            return True
        if self.match_any_regex(cp.get('hard_junk_regex', []), text):
            return True
        if self.match_any_regex(cp.get('hard_junk_regex', []), base):
            return True
        return False

    def is_cover_asset(self, name, size=0):
        cp=self.cover_cfg()
        if not cp.get('enabled', False):
            return False
        text=self.norm_rel(name)
        base=os.path.basename(text)
        if self.is_hard_junk(text):
            return False
        if not self.is_image_file(text):
            return False
        if self.match_any_regex(cp.get('cover_dir_regex', []), text):
            return True
        if self.match_any_regex(cp.get('cover_file_regex', []), base):
            return True
        return False

    def classify_file(self, name, size=0):
        if self.is_hard_junk(name):
            return 'junk'
        if self.is_cover_asset(name, size):
            return 'cover'
        if self.is_image_file(name):
            return 'image_other'
        return 'main'

    def junk(self,name,size):
        cls=self.classify_file(name,size)
        if cls == 'cover': return False
        if cls == 'junk': return True
        pats=self.cfg['batching'].get('skip_junk_patterns',[])
        text=self.norm_rel(name)
        base=os.path.basename(text)
        if any(fnmatch.fnmatch(base,p) or fnmatch.fnmatch(text,p) for p in pats): return True
        if size < self.cfg['batching'].get('min_file_size_mb_for_main',100)*MB:
            ext=base.lower().rsplit('.',1)[-1] if '.' in base else ''
            if ext in {'jpg','jpeg','png','gif','nfo'}: return True
            if text_link_junk_reason(text): return True
        return False

    def related_cover_indices(self, files, selected, archived=None, skipped=None):
        cp=self.cover_cfg()
        if not cp.get('enabled', False): return []
        archived=archived or set(); skipped=skipped or set()
        selected=set(selected or [])
        video_indices=[i for i in selected if 0 <= i < len(files) and self.is_video_file(files[i].get('name',''))]
        if not video_indices: return []
        video_names=[self.norm_rel(files[i].get('name','')) for i in video_indices]
        video_stems={Path(v).stem.lower() for v in video_names}
        video_dirs={str(Path(v).parent).replace('\\','/') for v in video_names}
        top_dirs={v.split('/')[0].lower() for v in video_names if '/' in v}
        max_cover_bytes=int(cp.get('max_cover_bytes', 50*MB))
        covers=[]
        for i,f in enumerate(files):
            # Historical versions may have marked cover images as skipped.
            # Re-allow them if they now match cover_policy; archived still wins.
            if i in selected or i in archived: continue
            name=self.norm_rel(f.get('name',''))
            if not self.is_cover_asset(name, f.get('size',0)): continue
            if int(f.get('size') or 0) > max_cover_bytes: continue
            p=Path(name); stem=p.stem.lower(); parent=str(p.parent).replace('\\','/')
            parts=[x.lower() for x in p.parts]
            # Attach covers in cover directories, same video dir, same top tree, or filename-stem match.
            if stem in video_stems or parent in video_dirs or (parts and parts[0] in top_dirs) or self.match_any_regex(cp.get('cover_dir_regex', []), name):
                covers.append(i)
        return covers

    def set_file_prio(self,h,ids,priority):
        if not ids: return
        # qBT accepts pipe-separated ids
        self.qpost('/api/v2/torrents/filePrio', {'hash':h,'id':'|'.join(map(str,ids)),'priority':priority})

    def choose_batch(self, files, archived, skipped, budget_bytes=None):
        cap = self.current_batch_limit_bytes(budget_bytes=budget_bytes)
        if cap <= 128*MB: return []
        chosen=[]; total=0
        for i,f in enumerate(files):
            if i in archived or i in skipped: continue
            cls=self.classify_file(f['name'], f.get('size',0))
            if cls == 'junk' or (cls == 'image_other' and self.junk(f['name'], f.get('size',0))):
                skipped.add(i); continue
            # Cover images are attached after main video selection, not used alone to form a batch.
            if cls == 'cover':
                continue
            if self.junk(f['name'], f.get('size',0)):
                skipped.add(i); continue
            size=int(f.get('size',0) or 0)
            if size <= 0: continue
            if chosen and total + size > cap: break
            if not chosen and size > cap:
                # Do not start a batch whose first/main file exceeds the current dynamic limit.
                # The caller will mark the torrent space-insufficient when the next main file cannot fit safely.
                break
            if total + size <= cap:
                chosen.append(i); total += size
        cover_ids=self.related_cover_indices(files, chosen, archived, skipped)
        if cover_ids:
            skipped.difference_update(cover_ids)
        for ci in cover_ids:
            if ci not in chosen:
                chosen.append(ci)
        return chosen

    def torrent_complete(self, t, files):
        return float(t.get('progress') or 0) >= 0.999 or (bool(files) and all(f.get('progress',0) >= 0.999 for f in files))

    def file_remaining_bytes(self, f):
        size=int(f.get('size') or 0)
        progress=max(0.0, min(1.0, float(f.get('progress') or 0)))
        return max(0, int(size * (1.0 - progress)))

    def indices_remaining_bytes(self, files, indices):
        return sum(self.file_remaining_bytes(files[i]) for i in indices if 0 <= i < len(files))

    def full_remaining_bytes(self, t, files):
        if files:
            rem=sum(self.file_remaining_bytes(f) for f in files)
            if rem > 0:
                return rem
        total=int(t.get('total_size') or t.get('size') or 0)
        progress=max(0.0, min(1.0, float(t.get('progress') or 0)))
        return max(0, int(total * (1.0 - progress)))

    def planned_overhead_bytes(self):
        return int(self.cfg.get('scheduler',{}).get('per_torrent_overhead_mb',256) * MB)

    def download_budget_bytes(self):
        disk=self.cfg.get('disk',{})
        free=self.free_gb()
        return max(0, int((free - disk.get('target_min_free_gb',3) - disk.get('batch_overhead_gb',0.5)) * GB))

    def current_batch_limit_bytes(self, budget_bytes=None):
        disk=self.cfg.get('disk',{})
        free=self.free_gb()
        available=max(0, int((free - disk.get('target_min_free_gb',3) - disk.get('batch_overhead_gb',0.5)) * GB))
        if budget_bytes is not None:
            available=min(available, max(0, int(budget_bytes)))
        return min(int(disk.get('max_batch_gb',12) * GB), available)

    def remaining_main_file_sizes(self, files, archived, skipped):
        sizes=[]
        local_skipped=set(skipped or set())
        for i,f in enumerate(files or []):
            if i in archived or i in local_skipped:
                continue
            name=f.get('name','')
            size=int(f.get('size') or 0)
            cls=self.classify_file(name, size)
            if cls in {'junk','cover','image_other'} or self.junk(name, size):
                continue
            if size > 0:
                sizes.append((i, size, name))
        return sizes

    def hold_space_insufficient_if_needed(self, t, files, st, budget_bytes=None):
        if not self.is_huge(t, files):
            return False
        if st.get('current_batch'):
            return False
        h=t['hash']
        archived=set(json.loads(st.get('archived_indices') or '[]'))
        skipped=set(json.loads(st.get('skipped_indices') or '[]'))
        sizes=self.remaining_main_file_sizes(files, archived, skipped)
        if not sizes:
            return False
        limit=self.current_batch_limit_bytes(budget_bytes=budget_bytes)
        next_i,next_size,next_name=sizes[0]
        if next_size <= limit:
            return False
        space_tag=self.qbt.get('tag_space_insufficient','space-insufficient')
        try:
            self.stop_torrent(h)
        except Exception as e:
            self.event(h,'ERROR',f'space-insufficient pause failed: {e}')
        try:
            self.add_tags(h, [self.qbt.get('tag_hold','hold'), space_tag])
        except Exception as e:
            self.event(h,'ERROR',f'space-insufficient tag failed: {e}')
        self.put_state(h, slot_kind='space-insufficient')
        self.event(h,'WARN',f'space insufficient for safe batch: next={next_size/GB:.2f}G file={next_name}, limit={limit/GB:.2f}G free={self.free_gb():.1f}G; tagged {self.qbt.get('tag_hold','hold')},{space_tag}')
        return True

    def candidate_need_bytes(self, t, files, st, budget_bytes=None):
        if self.torrent_complete(t, files):
            return 0
        overhead=self.planned_overhead_bytes()
        if self.is_huge(t, files):
            archived=set(json.loads(st.get('archived_indices') or '[]'))
            skipped=set(json.loads(st.get('skipped_indices') or '[]'))
            current=json.loads(st['current_batch']) if st.get('current_batch') else None
            if current:
                rem=self.indices_remaining_bytes(files, current)
                return rem + overhead if rem > 0 else 0
            local_skipped=set(skipped)
            alloc=max(0, (budget_bytes if budget_bytes is not None else self.download_budget_bytes()) - overhead)
            batch=self.choose_batch(files, archived, local_skipped, budget_bytes=alloc)
            if not batch:
                return 0
            rem=sum(int(files[i].get('size') or 0) for i in batch if 0 <= i < len(files))
            return rem + overhead if rem > 0 else 0
        rem=self.full_remaining_bytes(t, files)
        return rem + overhead if rem > 0 else 0

    def build_size_aware_plan(self, torrents, files_by_hash, states_by_hash):
        sched=self.cfg.get('scheduler',{})
        sp=self.slow_cfg()
        self.size_aware_enabled = bool(sched.get('size_aware_enabled') or sched.get('planner') == 'size_aware')
        if not self.size_aware_enabled:
            self.planned_hashes=set(); self.planned_budgets={}; self.planned_slot_kinds={}
            return
        budget=self.download_budget_bytes()
        max_active=int(sched.get('max_active_downloads',6))
        stable_slots=int(sched.get('stable_slots', sp.get('stable_slots',4)))
        probe_slots=int(sched.get('probe_slots', sp.get('probe_slots',1)))
        overflow_slots=max(0, max_active - stable_slots - probe_slots)
        min_active=int(sched.get('min_active_downloads',1))
        candidates=[]; cooling=0
        for t in torrents:
            if not self.managed(t) or self.qbt['tag_hold'] in self.tags(t):
                continue
            h=t['hash']; st=states_by_hash.get(h) or {}
            if st.get('done'):
                continue
            if self.is_cooling(st):
                cooling += 1
                if self.is_download_active(t):
                    try:
                        self.stop_torrent(h)
                        self.event(h,'INFO',f'paused cooling torrent, cooldown_left={self.cooldown_left(st)//60}m')
                    except Exception as e:
                        self.event(h,'ERROR',f'failed to pause cooling torrent: {e}')
                continue
            files=files_by_hash.get(h, [])
            if self.torrent_complete(t, files):
                continue
            need=self.candidate_need_bytes(t, files, st, budget)
            if need <= 0:
                if self.is_huge(t, files):
                    try:
                        self.hold_space_insufficient_if_needed(t, files, st, budget_bytes=budget)
                    except Exception as e:
                        self.event(h,'ERROR',f'space-insufficient check failed: {e}')
                continue
            active=self.is_download_active(t)
            progress=float(t.get('progress') or 0)
            dlspeed=int(t.get('dlspeed') or 0)
            ema=float(st.get('last_dlspeed_ema') or dlspeed)
            strikes=int(st.get('slow_strikes') or 0)
            seeds=int(t.get('num_complete') or t.get('seeds') or 0)
            peers=int(t.get('num_incomplete') or t.get('peers') or 0)
            # Lower tuple is better. Prefer active/near-complete/small/healthy tasks.
            rank=(strikes, 0 if active else 1, -progress, need, -ema, -(seeds+peers))
            candidates.append({'hash':h,'torrent':t,'files':files,'state':st,'need':need,'active':active,'progress':progress,'rank':rank})

        selected=[]; planned_budgets={}; planned_slot_kinds={}; remaining=budget
        selected_set=set()
        def try_add(c, kind):
            nonlocal remaining
            if c['hash'] in selected_set or len(selected) >= max_active:
                return False
            if remaining <= self.planned_overhead_bytes():
                return False
            need=self.candidate_need_bytes(c['torrent'], c['files'], c['state'], remaining)
            if need <= 0 or need > remaining:
                return False
            selected.append(c['hash']); selected_set.add(c['hash'])
            planned_budgets[c['hash']]=max(0, need - self.planned_overhead_bytes())
            planned_slot_kinds[c['hash']]=kind
            remaining -= need
            return True

        active_candidates=sorted([c for c in candidates if c['active']], key=lambda c: c['rank'])
        inactive_candidates=sorted([c for c in candidates if not c['active']], key=lambda c: c['rank'])
        ordered=active_candidates + inactive_candidates

        stable_count=0
        for c in ordered:
            if stable_count >= stable_slots: break
            if try_add(c,'stable'):
                stable_count += 1

        probe_count=0
        # Probe slots intentionally prefer not-yet-active candidates, then any remaining candidate.
        for c in inactive_candidates + active_candidates:
            if probe_count >= probe_slots: break
            if try_add(c,'probe'):
                probe_count += 1

        overflow_count=0
        for c in ordered:
            if overflow_count >= overflow_slots: break
            if try_add(c,'overflow'):
                overflow_count += 1

        if not selected and candidates and budget > self.planned_overhead_bytes() and min_active > 0:
            c=sorted(candidates, key=lambda x: x['rank'])[0]
            if try_add(c,'probe'):
                probe_count += 1

        self.planned_hashes=set(selected)
        self.planned_budgets=planned_budgets
        self.planned_slot_kinds=planned_slot_kinds
        self.download_limit=len(selected)
        self.log(f'size-aware plan selected={len(selected)}/{len(candidates)}, stable={stable_count}/{stable_slots}, probe={probe_count}/{probe_slots}, overflow={overflow_count}/{overflow_slots}, cooling={cooling}, budget={budget/GB:.1f}G, remaining={remaining/GB:.1f}G, hashes={[h[:8]+":"+planned_slot_kinds[h] for h in selected]}')
        # Sync qBT queue to configured upper bound. The orchestrator still controls the actual selected set.
        if sched.get('dynamic_set_qbt_queue_limits', False):
            qbt_limit=max(1, max_active)
            prefs={
              'queueing_enabled': True,
              'max_active_downloads': qbt_limit,
              'max_active_torrents': max(20, qbt_limit + int(sched.get('qbt_extra_active_torrents',12))),
              'max_active_uploads': max(10, qbt_limit + int(sched.get('qbt_extra_active_torrents',12)) // 2),
              'preallocate_all': False
            }
            try: self.qpost('/api/v2/app/setPreferences', {'json':prefs})
            except Exception as e: self.log(f'WARN set dynamic qBT queue limits: {e}')

    def apply_size_aware_plan(self, torrents, files_by_hash):
        if not getattr(self, 'size_aware_enabled', False):
            return
        pause_unplanned=self.cfg.get('scheduler',{}).get('pause_unplanned_managed', True)
        slot_kinds=getattr(self,'planned_slot_kinds',{})
        for t in torrents:
            if not self.managed(t) or self.qbt['tag_hold'] in self.tags(t):
                continue
            h=t['hash']; files=files_by_hash.get(h, [])
            if self.torrent_complete(t, files):
                continue
            selected=h in self.planned_hashes
            huge=self.is_huge(t, files)
            current=None
            try:
                st=self.get_state(h,t.get('name'),t.get('added_on',0))
                current=json.loads(st['current_batch']) if st.get('current_batch') else None
            except Exception:
                current=None
            if selected:
                kind=slot_kinds.get(h,'stable')
                updates={'slot_kind':kind}
                if not st.get('probe_started'):
                    updates['probe_started']=now()
                if st.get('cooldown_until'):
                    updates['cooldown_until']=0
                try:
                    self.put_state(h, **updates)
                except Exception as e:
                    self.event(h,'ERROR',f'failed to record slot kind: {e}')
                # Huge torrents without current_batch must be started by handle_huge after file priorities are set.
                if not huge or current:
                    if not self.is_download_active(t):
                        try:
                            self.start_torrent(h)
                            self.event(h,'INFO',f'planner started selected download slot={kind}')
                        except Exception as e:
                            self.event(h,'ERROR',f'planner failed to start: {e}')
            elif pause_unplanned and self.is_download_active(t):
                try:
                    self.stop_torrent(h)
                    self.put_state(h, slot_kind='paused', probe_started=None)
                    self.event(h,'INFO','planner paused unselected download')
                except Exception as e:
                    self.event(h,'ERROR',f'planner failed to pause: {e}')
        self.active_downloads=len(self.planned_hashes)

    def release_due(self,t,st,policy_name):
        policies=self.cfg['seed_policy']
        free=self.free_gb()
        emergency = free < self.cfg['disk']['emergency_free_below_gb']
        pol=policies['emergency'] if emergency else policies[policy_name]
        min_seed=parse_duration(pol['min_seed_time']); max_seed=parse_duration(pol['max_seed_time']); idle_timeout=parse_duration(pol['idle_timeout']); idle_speed=parse_speed(pol['idle_upload_speed'])
        seed_start=st.get('seed_start') or now()
        elapsed=now()-seed_start
        ratio=float(t.get('ratio') or 0)
        upspeed=int(t.get('upspeed') or 0)
        uploaded=int(t.get('uploaded') or 0)
        last_uploaded=int(st.get('last_uploaded') or 0)
        idle_since=st.get('idle_since')
        if uploaded > last_uploaded or upspeed >= idle_speed:
            idle_since=None
        elif not idle_since:
            idle_since=now()
        self.put_state(t['hash'], last_uploaded=uploaded, idle_since=idle_since, seed_start=seed_start)
        if elapsed >= max_seed: return True, f'max_seed_time {pol["max_seed_time"]}'
        if elapsed >= min_seed and ratio >= float(pol['target_ratio']): return True, f'ratio {ratio:.3f} >= {pol["target_ratio"]}'
        if elapsed >= min_seed and idle_since and now()-idle_since >= idle_timeout: return True, f'idle {pol["idle_timeout"]}'
        if emergency and elapsed >= min_seed: return True, f'emergency free={free:.1f}G'
        return False, f'not due elapsed={elapsed}s ratio={ratio:.3f} up={upspeed} free={free:.1f}G'

    def make_filelist(self,t,files,indices):
        # Use /data/downloads as rclone source root so both old /downloads/done
        # and new /downloads/active torrents can be uploaded safely. qBT file
        # names for multi-file torrents often include the torrent root directory,
        # while content_path/save_path may already include that root. Try both
        # rooted and de-duplicated forms before refusing cleanup.
        base=Path(self.paths['host_downloads'])
        content=self.content_host_path(t)
        sp=t.get('save_path') or self.qbt['save_path']
        if sp.startswith('/downloads'):
            save_root=Path(self.paths['host_downloads'] + sp[len('/downloads'):])
        else:
            save_root=Path(self.paths['host_active'])

        rels=[]
        seen=set()

        def add_if_file(path):
            try:
                path=Path(path)
                if not path.exists() or not path.is_file():
                    return False
                rel=str(path.relative_to(base))
            except Exception:
                return False
            if rel not in seen:
                seen.add(rel); rels.append(rel)
            return True

        def candidate_paths(file_name):
            file_name=str(file_name or '').lstrip('/')
            if not file_name:
                return []
            rel=Path(file_name)
            out=[content / rel, base / rel, save_root / rel]
            parts=rel.parts
            if len(parts) > 1:
                stripped=Path(*parts[1:])
                # Fix duplicate root cases such as:
                #   content=/data/downloads/糖心Vlog
                #   file=糖心Vlog/a.mp4
                # and category save paths such as:
                #   content=/data/downloads/active/mkmp-731
                #   file=mkmp-731/a.mp4
                out += [content / stripped, save_root / stripped]
            return out

        if len(files)==1 and add_if_file(content):
            pass
        else:
            for i in indices:
                if i < 0 or i >= len(files):
                    continue
                f=files[i]
                for abs_path in candidate_paths(f.get('name')):
                    if add_if_file(abs_path):
                        break
        work=Path(self.paths['work_dir'])/t['hash']
        work.mkdir(parents=True, exist_ok=True)
        listfile=work/f'files-{int(time.time())}.txt'
        listfile.write_text('\n'.join(rels)+'\n', encoding='utf-8')
        return listfile, rels

    def choose_existing_path_for_file(self, t, f):
        base=Path(self.paths['host_downloads'])
        content=self.content_host_path(t)
        sp=t.get('save_path') or self.qbt['save_path']
        save_root=Path(self.paths['host_downloads'] + sp[len('/downloads'):]) if sp.startswith('/downloads') else Path(self.paths['host_active'])
        name=self.norm_rel(f.get('name',''))
        rel=Path(name)
        candidates=[content/rel, base/rel, save_root/rel]
        parts=rel.parts
        if len(parts)>1:
            stripped=Path(*parts[1:])
            candidates += [content/stripped, save_root/stripped]
        for c in candidates:
            try:
                if c.exists() and c.is_file():
                    return c
            except Exception:
                pass
        return None

    def uploaded_rel_for_file(self, t, f):
        base=Path(self.paths['host_downloads'])
        p=self.choose_existing_path_for_file(t,f)
        if p:
            try: return str(p.relative_to(base)).replace('\\','/')
            except Exception: pass
        return self.norm_rel(f.get('name',''))

    def cover_sidecar_targets(self, t, files, indices, dest_rels_by_index=None):
        cp=self.cover_cfg()
        if not cp.get('enabled', False): return []
        idx=[i for i in indices if 0 <= i < len(files)]
        videos=[i for i in idx if self.is_video_file(files[i].get('name',''))]
        covers=[i for i in idx if self.is_cover_asset(files[i].get('name',''), files[i].get('size',0))]
        if not videos or not covers: return []
        dest_rels_by_index=dest_rels_by_index or {}
        video_rels={i:dest_rels_by_index.get(i) or self.uploaded_rel_for_file(t,files[i]) for i in videos}
        cover_rels={i:self.uploaded_rel_for_file(t,files[i]) for i in covers}
        out=[]; used=set()
        def ext_of(rel):
            e=Path(rel).suffix.lower()
            return e if e in {'.jpg','.jpeg','.png','.webp'} else '.jpg'
        # Exact/fuzzy stem match to per-video poster.
        for vi,vrel in video_rels.items():
            vstem=Path(vrel).stem.lower()
            vdir=str(Path(vrel).parent).replace('\\','/')
            best=None
            for ci,crel in cover_rels.items():
                if ci in used: continue
                cstem=Path(crel).stem.lower(); cdir=str(Path(crel).parent).replace('\\','/')
                if cstem == vstem or vstem in cstem or cstem in vstem or cdir == vdir:
                    best=ci; break
            if best is not None:
                used.add(best)
                ext=ext_of(cover_rels[best])
                target=str(Path(vdir)/(Path(vrel).stem + '-poster' + ext)).replace('\\','/')
                out.append((best,target))
        # If one video only, attach first remaining cover as video-poster.
        if len(videos)==1:
            vi=videos[0]; vrel=video_rels[vi]; vdir=str(Path(vrel).parent).replace('\\','/')
            for ci,crel in cover_rels.items():
                if ci not in used:
                    used.add(ci)
                    target=str(Path(vdir)/(Path(vrel).stem + '-poster' + ext_of(crel))).replace('\\','/')
                    out.append((ci,target)); break
        # Directory-level poster/folder for first remaining cover.
        if cp.get('default_folder_poster', True):
            first_dir=str(Path(next(iter(video_rels.values()))).parent).replace('\\','/')
            for ci,crel in cover_rels.items():
                if ci not in used:
                    ext=ext_of(crel)
                    out.append((ci, str(Path(first_dir)/('poster'+ext)).replace('\\','/')))
                    out.append((ci, str(Path(first_dir)/('folder'+ext)).replace('\\','/')))
                    used.add(ci)
                    break
        # Unmatched covers are still uploaded into _unmatched_covers for manual review.
        unmatched_dir=cp.get('unmatched_dir','_unmatched_covers')
        if cp.get('unmatched_action','upload_to_unmatched_covers') == 'upload_to_unmatched_covers':
            for ci,crel in cover_rels.items():
                if ci in used: continue
                target=str(Path(unmatched_dir)/Path(crel).name).replace('\\','/')
                out.append((ci,target))
        # De-duplicate same cover->target.
        seen=set(); uniq=[]
        for ci,target in out:
            key=(ci,target)
            if key not in seen:
                seen.add(key); uniq.append((ci,target))
        return uniq

    def upload_cover_sidecars(self, t, files, indices, remote, dest_rels_by_index=None):
        targets=self.cover_sidecar_targets(t, files, indices, dest_rels_by_index=dest_rels_by_index)
        if not targets: return True
        ok=True; count=0
        for ci,target in targets:
            src=self.choose_existing_path_for_file(t, files[ci])
            if not src:
                self.event(t['hash'],'ERROR',f'cover sidecar source missing: {files[ci].get("name")}')
                ok=False; continue
            dst=f"{remote.rstrip('/')}/{target.lstrip('/')}"
            cmd=self.rclone_cmd_base(include_excludes=False)+['copyto',str(src),dst,'--log-file',self.paths['log_file'],'--log-level','INFO']
            if self.mode!='live':
                self.event(t['hash'],'DRYRUN',' '.join(cmd)); continue
            p=subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=3600)
            if p.returncode != 0:
                self.event(t['hash'],'ERROR',f'cover sidecar copy failed rc={p.returncode}: {p.stdout[-500:]}')
                ok=False
            else:
                count += 1
        self.event(t['hash'],'INFO',f'cover sidecars uploaded: {count}/{len(targets)}')
        return ok

    def remote_root(self,t):
        return f"{self.rclone['remote'].rstrip('/')}/{safe_name(t.get('name','torrent'))}-{t['hash'][:12]}"

    def remote_base(self):
        return str(self.rclone.get('remote','gcrypt:')).rstrip('/')

    def remote_path(self, rel):
        rel=self.norm_rel(rel)
        return f"{self.remote_base()}/{rel.lstrip('/')}"

    def normalizer(self):
        if hasattr(self, '_jav_normalize_func'):
            return self._jav_normalize_func
        func=None
        path=(self.cfg.get('dedupe') or {}).get('normalizer') or '/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py'
        try:
            if path and Path(path).exists():
                spec=importlib.util.spec_from_file_location('jav_name_normalize_for_orchestrator', path)
                mod=importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                func=getattr(mod, 'normalize', None)
        except Exception as e:
            self.log(f'WARN failed to load normalizer {path}: {e}')
            func=None
        self._jav_normalize_func=func
        return func

    def normalized_media_id(self, name):
        base=Path(self.norm_rel(name)).name
        func=self.normalizer()
        if func:
            try:
                row=func(base)
                nid=str(row.get('normalized_id') or '').strip().upper()
                conf=float(row.get('confidence') or 0)
                if nid and conf >= 0.70:
                    return safe_name(nid, 80)
            except Exception as e:
                self.log(f'WARN normalize failed for {base}: {e}')
        stem=Path(base).stem or safe_name(base)
        return safe_name(stem, 80)

    def upload_dest_for_video(self, file_name, used):
        src_base=Path(self.norm_rel(file_name)).name
        ext=Path(src_base).suffix.lower() or '.mp4'
        media_id=self.normalized_media_id(src_base)
        rel=f"{media_id}/{media_id}{ext}"
        if rel not in used:
            used.add(rel)
            return rel, media_id
        # Rare duplicate version/part. Keep it inside the same movie directory,
        # but preserve enough of the original stem to avoid overwriting.
        stem=safe_name(Path(src_base).stem, 80)
        rel=f"{media_id}/{stem}{ext}"
        n=2
        while rel in used:
            rel=f"{media_id}/{stem}-{n}{ext}"; n += 1
        used.add(rel)
        return rel, media_id

    def subtitle_suffix(self, src_base):
        # Preserve language/variant suffixes like Movie.zh.ass -> ID.zh.ass.
        p=Path(src_base)
        stem=p.stem
        ext=p.suffix.lower()
        parts=stem.split('.')
        if len(parts) >= 2 and 1 <= len(parts[-1]) <= 12:
            return f".{parts[-1]}{ext}"
        return ext

    def build_upload_plan(self, t, files, indices):
        """Map selected qBT files to an Emby-friendly Google Drive layout.

        Old layout preserved qBT internals:
          gcrypt:/Torrent-hash/active/Torrent/file.mp4

        New layout is one movie per directory:
          gcrypt:/ABCD-123/ABCD-123.mp4
          gcrypt:/ABCD-123/ABCD-123-poster.jpg
          gcrypt:/ABCD-123/extrafanart/fanart1.jpg
        """
        selected=[]
        for i in indices:
            if i < 0 or i >= len(files):
                continue
            src=self.choose_existing_path_for_file(t, files[i])
            if not src:
                self.event(t['hash'],'ERROR',f'upload source missing: {files[i].get("name")}')
                continue
            selected.append((i,src,files[i]))
        used=set()
        plan=[]
        video_meta={}
        video_items=[(i,src,f) for i,src,f in selected if self.is_video_file(f.get('name',''))]
        for i,src,f in video_items:
            rel,media_id=self.upload_dest_for_video(f.get('name',''), used)
            plan.append((i,src,rel))
            video_meta[i]={'rel':rel,'dir':str(Path(rel).parent).replace('\\','/'),'media_id':media_id,'src_parent':src.parent}

        # Attach subtitles/NFO/other useful sidecars to the closest selected
        # video directory. Cover images are converted by upload_cover_sidecars()
        # into poster/folder sidecars, so their original ad/source names are not
        # kept in the library.
        for i,src,f in selected:
            if i in video_meta:
                continue
            name=f.get('name','')
            if self.is_cover_asset(name, f.get('size',0)):
                continue
            if self.junk(name, f.get('size',0)):
                continue
            target_video=None
            same_parent=[m for m in video_meta.values() if m['src_parent'] == src.parent]
            if same_parent:
                target_video=same_parent[0]
            elif len(video_meta) == 1:
                target_video=next(iter(video_meta.values()))
            if not target_video:
                self.event(t['hash'],'INFO',f'skipping unassociated non-video file during normalized upload: {name}')
                continue
            media_id=target_video['media_id']
            ext=Path(src.name).suffix.lower()
            if ext in {'.srt','.ass','.ssa','.vtt','.sub'}:
                dest_name=media_id + self.subtitle_suffix(src.name)
            elif ext == '.nfo':
                dest_name=f'{media_id}.nfo'
            else:
                dest_name=safe_name(src.name, 96)
            rel=str(Path(target_video['dir'])/dest_name).replace('\\','/')
            n=2
            base_rel=rel
            while rel in used:
                p=Path(base_rel)
                rel=str(p.with_name(f"{p.stem}-{n}{p.suffix}")).replace('\\','/')
                n += 1
            used.add(rel)
            plan.append((i,src,rel))
        return plan

    def remote_size_bytes(self, remote_file):
        cmd=self.rclone_cmd_base(include_excludes=False)+['size','--json',remote_file]
        p=subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
        if p.returncode != 0:
            return None, p.stdout[-500:]
        try:
            data=json.loads(p.stdout or '{}')
            return int(data.get('bytes') or 0), None
        except Exception as e:
            return None, f'cannot parse rclone size json: {e}: {(p.stdout or "")[-300:]}'

    def upload_plan_copy_and_check(self, t, plan):
        copied=0
        for _i,src,rel in plan:
            dst=self.remote_path(rel)
            self.event(t['hash'],'INFO',f'rclone copyto {src} -> {dst}')
            cmd=self.rclone_cmd_base(include_excludes=False)+['copyto',str(src),dst,'--log-file',self.paths['log_file'],'--log-level','INFO']
            if self.mode!='live':
                self.event(t['hash'],'DRYRUN',' '.join(cmd))
                continue
            p=subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=24*3600)
            if p.returncode != 0:
                self.event(t['hash'],'ERROR',f'rclone copyto failed rc={p.returncode}: {p.stdout[-500:]}')
                return False
            if self.cfg['cleanup'].get('require_rclone_check',True):
                remote_size,err=self.remote_size_bytes(dst)
                local_size=src.stat().st_size
                if err or remote_size != local_size:
                    self.event(t['hash'],'ERROR',f'rclone size check failed for {dst}: local={local_size} remote={remote_size} err={err}')
                    return False
            copied += 1
        self.event(t['hash'],'INFO',f'normalized upload copied and checked: {copied}/{len(plan)}')
        return True

    def rclone_cmd_base(self, include_excludes=True):
        c=['rclone','--config',self.rclone['config'],'--transfers',str(self.rclone['transfers']),'--checkers',str(self.rclone['checkers']), '--drive-chunk-size',self.rclone['drive_chunk_size'], '--buffer-size',self.rclone['buffer_size'], '--tpslimit',str(self.rclone['tpslimit']), '--retries',str(self.rclone['retries']), '--low-level-retries',str(self.rclone['low_level_retries'])]
        # rclone does not allow --files-from together with normal filters such
        # as --exclude. upload_and_check passes an exact file manifest, so omit
        # excludes for those operations and keep this switch for future callers.
        if include_excludes:
            for e in self.rclone.get('exclude',[]): c += ['--exclude', e]
        return c

    def upload_and_check(self,t,files,indices):
        plan = self.build_upload_plan(t,files,indices)
        if not plan:
            self.event(t['hash'],'ERROR','no existing local files matched upload manifest; refusing cleanup')
            return False
        remote=self.remote_base()
        dest_rels_by_index={i:rel for i,_src,rel in plan}
        dirs=sorted({str(Path(rel).parent).replace('\\','/') for _i,_src,rel in plan})
        self.event(t['hash'],'INFO',f'normalized rclone upload plan: {len(plan)} file(s) to {remote}; dirs={dirs[:8]}')
        if not self.upload_plan_copy_and_check(t, plan):
            return False
        if not self.upload_cover_sidecars(t, files, indices, remote, dest_rels_by_index=dest_rels_by_index):
            return False
        return True

    def delete_torrent(self,h):
        if self.mode!='live':
            self.event(h,'DRYRUN','delete torrent deleteFiles=true'); return
        self.qpost('/api/v2/torrents/delete', {'hashes':h, 'deleteFiles':'true'})

    def readd_torrent(self,h):
        path=Path(self.qbt['torrent_store_container'])/f'{h}.torrent'
        if self.mode!='live':
            self.event(h,'DRYRUN',f're-add torrent {path}'); return
        self.qpost('/api/v2/torrents/add', {
            'savepath': self.qbt['save_path'],
            'category': self.qbt['category_auto'],
            'tags': self.qbt['tag_auto'],
            'paused': 'true',
            'stopped': 'true'
        }, files={'torrents':str(path)}, ok=(200,))

    def handle_huge(self,t,files,st):
        h=t['hash']; archived=set(json.loads(st.get('archived_indices') or '[]')); skipped=set(json.loads(st.get('skipped_indices') or '[]'))
        self.backup_torrent_file(h)
        # mark junk as skipped
        for i,f in enumerate(files):
            if i not in archived and self.junk(f['name'], f.get('size',0)): skipped.add(i)
        current=json.loads(st['current_batch']) if st.get('current_batch') else None
        remaining=[i for i in range(len(files)) if i not in archived and i not in skipped]
        if not remaining:
            self.event(h,'INFO','all non-junk files archived; deleting torrent if still present')
            self.delete_torrent(h)
            self.put_state(h, done=1, archived_indices=json.dumps(sorted(archived)), skipped_indices=json.dumps(sorted(skipped)), current_batch=None)
            return
        if not current:
            planned_budget=None
            if getattr(self, 'size_aware_enabled', False):
                if h not in getattr(self, 'planned_hashes', set()):
                    self.event(h,'INFO',f'not selected by size-aware planner; free={self.free_gb():.1f}G')
                    return
                planned_budget=getattr(self, 'planned_budgets', {}).get(h)
            else:
                limit=getattr(self, 'download_limit', self.dynamic_download_limit())
                if limit <= 0:
                    self.event(h,'INFO',f'dynamic download limit is 0; free={self.free_gb():.1f}G')
                    return
                if getattr(self, 'active_downloads', 0) >= limit:
                    self.event(h,'INFO',f'dynamic max active downloads reached: {self.active_downloads}/{limit}, free={self.free_gb():.1f}G')
                    return
            if self.free_gb() < self.cfg['disk']['pause_new_free_below_gb']:
                self.event(h,'INFO',f'free space too low for new batch: {self.free_gb():.1f}G')
                try: self.stop_torrent(h)
                except Exception: pass
                self.put_state(h, skipped_indices=json.dumps(sorted(skipped)))
                return
            batch=self.choose_batch(files, archived, skipped, budget_bytes=planned_budget)
            if not batch:
                self.put_state(h, skipped_indices=json.dumps(sorted(skipped)))
                if self.hold_space_insufficient_if_needed(t, files, self.get_state(h), budget_bytes=planned_budget):
                    return
                self.event(h,'INFO','no batch selected; waiting for space')
                return
            all_ids=list(range(len(files)))
            self.stop_torrent(h)
            self.set_file_prio(h, all_ids, 0)
            self.set_file_prio(h, batch, 1)
            if self.set_sequential_download(h, True):
                self.event(h,'INFO','enabled sequential download for current batch')
            self.start_torrent(h)
            self.active_downloads = getattr(self, 'active_downloads', 0) + 1
            bn=int(st.get('batch_no') or 0)+1
            self.put_state(h, mode='huge', batch_no=bn, current_batch=json.dumps(batch), seed_start=None, skipped_indices=json.dumps(sorted(skipped)), last_uploaded=int(t.get('uploaded') or 0), idle_since=None)
            self.event(h,'INFO',f'started batch {bn}: {len(batch)} files, free={self.free_gb():.1f}G')
            return
        # current batch exists
        try:
            self.set_sequential_download(h, True)
        except Exception as e:
            self.event(h,'ERROR',f'failed to keep sequential download enabled: {e}')
        extra_covers=self.related_cover_indices(files, current, archived, skipped)
        if extra_covers:
            current=sorted(set(current) | set(extra_covers))
            skipped.difference_update(extra_covers)
            try:
                self.set_file_prio(h, extra_covers, 1)
                self.put_state(h, current_batch=json.dumps(current), skipped_indices=json.dumps(sorted(skipped)))
                self.event(h,'INFO',f'attached cover files to current batch: {len(extra_covers)}')
            except Exception as e:
                self.event(h,'ERROR',f'failed to attach cover files: {e}')
        complete=all(files[i].get('progress',0) >= 0.999 for i in current if i < len(files))
        if not complete:
            self.event(h,'INFO',f'batch downloading {sum(1 for i in current if files[i].get("progress",0)>=0.999)}/{len(current)} complete')
            return
        if not st.get('seed_start'):
            self.put_state(h, seed_start=now(), last_uploaded=int(t.get('uploaded') or 0), idle_since=None)
            self.event(h,'INFO','batch complete; seed timer started')
            return
        policy='seed_long' if self.qbt['tag_seed_long'] in self.tags(t) else 'large_batch'
        due,why=self.release_due(t, self.get_state(h), policy)
        if not due:
            self.event(h,'INFO',f'batch seeding; {why}')
            return
        self.event(h,'INFO',f'batch release due: {why}')
        self.stop_torrent(h)
        if self.upload_and_check(t,files,current):
            archived.update(current)
            self.delete_torrent(h)
            self.put_state(h, archived_indices=json.dumps(sorted(archived)), skipped_indices=json.dumps(sorted(skipped)), current_batch=None, seed_start=None, last_uploaded=0, idle_since=None)
            # re-add for next batch if remaining non-junk files exist
            remaining=[i for i in range(len(files)) if i not in archived and i not in skipped]
            if remaining:
                time.sleep(3)
                self.readd_torrent(h)
                self.event(h,'INFO',f're-added for next batch, remaining={len(remaining)}')
            else:
                self.put_state(h, done=1)
                self.event(h,'INFO','huge torrent completed')

    def full_upload_indices(self, files):
        """Return non-junk indices for normal full-torrent upload.

        This prevents already-downloaded ad/link text files from being copied
        to Google Drive during the final full-torrent release.
        """
        return [i for i,f in enumerate(files) if not self.junk(f.get('name',''), f.get('size',0))]

    def handle_full(self,t,files,st):
        h=t['hash']
        progress_complete = float(t.get('progress') or 0) >= 0.999
        files_complete = bool(files) and all(f.get('progress',0) >= 0.999 for f in files)
        complete = progress_complete or files_complete
        if not complete:
            if st.get('seed_start') and not files and float(t.get('progress') or 0) < 0.999:
                self.put_state(h, seed_start=None, last_uploaded=int(t.get('uploaded') or 0), idle_since=None)
                self.event(h,'INFO','cleared stale seed timer for incomplete torrent with empty file list')
            return
        if not st.get('seed_start'):
            self.put_state(h, mode='full', seed_start=now(), last_uploaded=int(t.get('uploaded') or 0), idle_since=None)
            self.event(h,'INFO','torrent complete; seed timer started')
            return
        policy='seed_long' if self.qbt['tag_seed_long'] in self.tags(t) else 'default'
        due,why=self.release_due(t, self.get_state(h), policy)
        if not due:
            self.event(h,'INFO',f'full torrent seeding; {why}')
            return
        self.event(h,'INFO',f'full torrent release due: {why}')
        self.stop_torrent(h)
        indices=self.full_upload_indices(files)
        skipped=len(files)-len(indices)
        if skipped:
            self.event(h,'INFO',f'full upload excluding {skipped} junk file(s) by cleaning rules')
        if files and not indices:
            self.event(h,'WARN','full upload skipped: no non-junk files selected')
            return
        if self.upload_and_check(t,files,indices):
            self.delete_torrent(h)
            self.put_state(h, done=1, current_batch=None)
            self.event(h,'INFO','full torrent uploaded, checked, deleted via qBT')

    def pause_if_critical(self):
        free=self.free_gb()
        if free < self.cfg['disk']['pause_all_downloads_free_below_gb']:
            torrents=self.qjson('/api/v2/torrents/info') or []
            hashes='|'.join(t['hash'] for t in torrents if self.managed(t) and t.get('state','').lower() not in {'pausedup','pauseddl','stoppedup','stoppeddl'})
            if hashes:
                try:
                    self.stop_torrent(hashes)
                    self.log(f'critical free={free:.1f}G; stopped managed torrents')
                except Exception as e: self.log(f'WARN cannot pause all: {e}')

    def run(self):
        self.ensure_qbt_basics()
        self.pause_if_critical()
        torrents=self.qjson('/api/v2/torrents/info') or []
        for t in torrents:
            if 'observe' in self.tags(t):
                try:
                    self.promote_observe_if_ready(t)
                except Exception as e:
                    self.event(t.get('hash'),'ERROR',f'observe promotion failed: {e}')
        files_by_hash={}
        states_by_hash={}
        for t in torrents:
            if not self.managed(t) or self.qbt['tag_hold'] in self.tags(t):
                continue
            h=t['hash']
            st=self.get_state(h,t.get('name'),t.get('added_on',0))
            states_by_hash[h]=st
            try:
                files_by_hash[h]=self.qjson('/api/v2/torrents/files', {'hash':h}) or []
            except Exception as e:
                files_by_hash[h]=[]
                self.event(h,'ERROR',f'failed to read files for planning: {e}')
        for t in torrents:
            if not self.managed(t): continue
            h=t['hash']
            if h in states_by_hash:
                states_by_hash[h]=self.update_download_health(t, files_by_hash.get(h, []), states_by_hash[h])
        self.build_size_aware_plan(torrents, files_by_hash, states_by_hash)
        if getattr(self, 'size_aware_enabled', False):
            self.apply_size_aware_plan(torrents, files_by_hash)
        else:
            self.download_limit=getattr(self, 'download_limit', self.dynamic_download_limit())
            self.active_downloads=sum(1 for t in torrents if self.managed(t) and self.is_download_active(t))
        self.log(f'active_downloads={getattr(self,"active_downloads",0)}/{getattr(self,"download_limit",0)}, free={self.free_gb():.1f}G')
        for t in torrents:
            if not self.managed(t): continue
            h=t['hash']
            if self.qbt['tag_hold'] in self.tags(t): continue
            st=states_by_hash.get(h) or self.get_state(h,t.get('name'),t.get('added_on',0))
            if st.get('done'): continue
            try:
                files=files_by_hash.get(h)
                if files is None:
                    files=self.qjson('/api/v2/torrents/files', {'hash':h}) or []
                if self.is_huge(t,files):
                    self.handle_huge(t,files,st)
                else:
                    self.handle_full(t,files,st)
            except Exception as e:
                self.event(h,'ERROR',str(e))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--config', default='/etc/qbt-orchestrator/config.json')
    args=ap.parse_args()
    cfg=json.loads(Path(args.config).read_text())
    Orchestrator(cfg).run()
if __name__=='__main__': main()
