import asyncio
import os
import time

from reddit.config import Settings
from reddit.metrics import Metrics
from reddit.seen_cache import SeenCache
from reddit.sender import Sender


def test_spool_expiry(tmp_path):
    async def go():
        s = Settings()
        s.data_dir = str(tmp_path)
        s.spool_max_age = 1800
        snd = Sender(s, SeenCache(100), Metrics(), None)
        snd._to_spool([{"external_id": "t1_a"}, {"external_id": "t1_b"}])
        snd._to_spool([{"external_id": "t1_c"}])
        old = sorted(snd._spool.glob("*.jsonl"))[0]
        t = time.time() - 1801
        os.utime(old, (t, t))
        snd._expire_spool()
        left = list(snd._spool.glob("*.jsonl"))
        assert len(left) == 1 and left[0] != old
        assert snd.m.c["spool_expired"] == 2
        s.spool_max_age = 0
        os.utime(left[0], (t, t))
        snd._expire_spool()
        assert len(list(snd._spool.glob("*.jsonl"))) == 1   # 0 = keep forever
        await snd._client.aclose()
    asyncio.run(go())
