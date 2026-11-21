import asyncio

from reddit.dashboard import Sampler
from reddit.metrics import Metrics


def test_sampler_and_server():
    from reddit.dashboard import serve

    async def go():
        m = Metrics()
        m.gauges["queue"] = lambda: 7
        sp = Sampler(m)
        m.inc("pages", 10); m.inc("pages_empty", 2); m.inc("unique", 50); m.drop("too_short")
        p = sp.sample()
        assert p["empty"] == 20.0 and p["new"] > 0
        task = asyncio.create_task(serve(sp, "127.0.0.1", 18765))
        await asyncio.sleep(0.2)
        r, w = await asyncio.open_connection("127.0.0.1", 18765)
        w.write(b"GET /api HTTP/1.1\r\nHost: x\r\n\r\n"); await w.drain()
        data = await r.read()
        assert b"200 OK" in data and b'"queue": "7"' in data and b"too_short" in data
        task.cancel()
    asyncio.run(go())
