import asyncio as aio
from collections import defaultdict
import functools
import logging


log = logging.getLogger(__name__)


class Job:
    def __init__(self, func, args, kwargs, parent, done=False, result=None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.parent = parent
        self.done = done
        self.result = result

    def __str__(self):
        return "{}{}{}".format(self.func.__name__,
                               self.args or "",
                               self.kwargs or "")


class Repository:
    def get_job(self, func, args, kwargs, parent):
        return Job(func, args, kwargs, parent)

    def save_job(self, job):
        pass


class Runner:
    def __init__(self, repository=None):
        self.repository = repository
        self.task_stacks = defaultdict(list)

    def call_job(self, job):
        if job.done:
            log.info("Skipping %s => %s", job, job.result)
        else:
            self.current_stack().append(job)

            log.info("Beginning %s", job)
            job.result = yield from job.func(*job.args, **job.kwargs)
            job.done = True
            log.info("Ended %s => %s", job, job.result)

            assert self.current_stack().pop() == job
            self.repository.save_job(job)

        return job.result

    def current_stack(self):
        return self.task_stacks[aio.Task.current_task()]

    def task(self, func):
        func = aio.coroutine(func)
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            parent_job = (self.current_stack() or [None])[-1]
            job = self.repository.get_job(func, args, kwargs, parent_job)
            return self.call_job(job)
        return wrapped
