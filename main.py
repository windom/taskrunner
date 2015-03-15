import logging
import asyncio as aio
import taskrunner as tk
import taskrepository as tr


logging.basicConfig(level=logging.INFO,
                   format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s")

log = logging.getLogger(__name__)

rn = tk.Runner()


@rn.task
def a():
    d1 = yield from x(100)
    d2 = yield from x(200)
    return d1 + d2


@rn.task
def aa(id):
    ds = yield from aio.gather(x(10), x(20))
    return sum(ds)

@rn.task
def slp():
    yield from aio.sleep(1)


@rn.task
def x(d):
    yield from slp()
    return d


@rn.task
def monster():
    ss = 0
    for _ in range(3):
        ss += yield from aa(_)
    return ss


try:
    loop = aio.get_event_loop()
    with tr.DbRepository('test.db') as repo:
        rn.repository = repo
        aio.async(monster())
        loop.run_until_complete(aio.gather(*aio.Task.all_tasks()))
except KeyboardInterrupt:
    log.info("** Interrupted **")
finally:
    loop.close()
