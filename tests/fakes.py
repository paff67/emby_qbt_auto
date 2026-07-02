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
    def __init__(self, copy_ok=True, remote_sizes=None):
        self.copy_ok = copy_ok
        self.remote_sizes = remote_sizes or {}
        self.copies = []

    def copyto(self, local, remote):
        self.copies.append((local, remote))
        return self.copy_ok

    def lsjson_size(self, remote):
        return self.remote_sizes.get(remote)


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
