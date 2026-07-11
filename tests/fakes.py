class FakeQbtClient:
    def __init__(self, maindata=None, info=None):
        self.maindata = list(maindata or [])
        self.info = info or {}
        self.posts = []
        self.heavy_calls = []

    def get_maindata(self, rid):
        if not self.maindata:
            return {"rid": rid, "full_update": False, "torrents": {}}
        item = self.maindata.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def torrent_info(self, hash):
        return self.info.get(hash, {"hash": hash, "seq_dl": False})

    def post(self, path, payload):
        self.posts.append((path, payload))
        return "Ok."

    def torrents_files(self, hash):
        self.heavy_calls.append(("torrents/files", hash))
        return []


class FakeExecutor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


class FakeRclone:
    def __init__(self, copy_ok=True, remote_sizes=None, remote_listing=None):
        self.copy_ok = copy_ok
        self.remote_sizes = remote_sizes or {}
        self.remote_listing = remote_listing or []
        self.copies = []
        self.dir_copies = []

    def copyto(self, local, remote):
        self.copies.append((local, remote))
        return self.copy_ok

    def copy(self, local, remote):
        self.dir_copies.append((local, remote))
        return self.copy_ok

    def lsjson_size(self, remote):
        return self.remote_sizes.get(remote)

    def lsjson(self, remote, recursive=False):
        return self.remote_listing


class BudgetedQbtFake:
    """Virtual-time qBT fake for steady-state API budget assertions."""

    def __init__(self, snapshots, now):
        self.snapshots = snapshots
        self.now = now
        self.calls = []
        self.maindata_calls = 0
        self.delta_calls = 0

    def get_maindata(self, rid):
        self.maindata_calls += 1
        full = int(rid) == 0
        if not full:
            self.delta_calls += 1
        return {
            "rid": int(rid) + 1,
            "full_update": full,
            "torrents": self.snapshots if full else {},
            "server_state": {},
        }

    def torrent_files(self, torrent_hash):
        self.calls.append((int(self.now()), "torrents/files", str(torrent_hash)))
        return [{"index": 0, "name": f"{torrent_hash}.mp4", "size": 1024**3, "progress": 0.0, "priority": 0}]

    def torrent_properties(self, torrent_hash):
        self.calls.append((int(self.now()), "torrents/properties", str(torrent_hash)))
        return {"piece_size": 16 * 1024**2}

    def calls_per_minute(self, endpoint):
        buckets = {}
        for ts, called_endpoint, _hash in self.calls:
            if called_endpoint != endpoint:
                continue
            bucket = int(ts) // 60
            buckets[bucket] = buckets.get(bucket, 0) + 1
        return max(buckets.values(), default=0)

    @property
    def delta_ratio(self):
        return self.delta_calls / self.maindata_calls if self.maindata_calls else 0.0


class FakeBackfill:
    def __init__(self):
        self.calls = []

    def scrape_one(self, media_group_key, manifest_id):
        self.calls.append((media_group_key, manifest_id))
        return {"status": "sidecar_verified", "artifacts": ["nfo", "poster", "fanart"]}


class FakeUploadQueue:
    def __init__(self):
        self.jobs = []

    def enqueue(self, job_type, payload):
        self.jobs.append({"job_type": job_type, "payload": payload})
        return len(self.jobs)


class FakeEmby:
    def __init__(self):
        self.refreshes = []

    def media_updated(self, path):
        payload = {"Updates": [{"Path": path, "UpdateType": "Created"}]}
        self.refreshes.append(payload)
        return payload
