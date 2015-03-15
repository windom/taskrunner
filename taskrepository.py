import logging
import sqlite3
import pickle
import base64
import hashlib
import taskrunner as tk


log = logging.getLogger(__name__)


db_ddl = """
create table if not exists job (
    id integer primary key,
    parent_id integer,
    status integer not null,
    func text not null,
    args_hash text not null,
    args_dump text not null,
    result_dump text,
    foreign key (parent_id) references job(id)
);
"""


class DbJob(tk.Job):
    def __init__(self, func, args, kwargs, parent,
                 done=False, result=None,
                 id=None, args_hash=None, args_dump=None):
        super().__init__(func, args, kwargs, parent, done, result)
        self.id = id
        self.args_hash = args_hash
        self.args_dump = args_dump

    @staticmethod
    def dump(obj, include_hash=False):
        if obj is None:
            return None
        bdump = pickle.dumps(obj)
        dump = base64.b64encode(bdump).decode('ascii')
        if include_hash:
            hasher = hashlib.sha1()
            hasher.update(bdump)
            hdump = hasher.hexdigest()
            return (dump, hdump)
        else:
            return dump

    @staticmethod
    def load(dump):
        if dump is None:
            return None
        else:
            bdump = base64.b64decode(dump)
            return pickle.loads(bdump)


class DbRepository(tk.Repository):
    def __init__(self, dbfile):
        self.dbfile = dbfile

    def __enter__(self):
        log.info("Connecting to %s", self.dbfile)
        self.db = sqlite3.connect(self.dbfile)
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.cursor.executescript(db_ddl)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        log.info("Disconnecting from %s", self.dbfile)
        self.db.close()

    def get_job(self, func, args, kwargs, parent):
        #
        # Encode (args, kwargs)
        #
        args_dump, args_hash = DbJob.dump((args, kwargs), include_hash=True)

        #
        # Query for existing jobs
        #
        sql = "select * from job where func = ? and args_hash = ? and "
        qargs = [func.__name__, args_hash]
        if parent:
            sql += "parent_id = ? "
            qargs.append(parent.id)
        else:
            sql += "parent_id is null "

        #
        # Return existing job
        #
        for row in self.cursor.execute(sql, qargs):
            if (args, kwargs) == DbJob.load(row["args_dump"]):
                job = DbJob(func, args, kwargs, parent,
                            bool(row["status"]),
                            DbJob.load(row["result_dump"]),
                            row["id"],
                            args_hash, args_dump)
                log.info("Loaded %s", job)
                return job

        #
        # Create & save new job
        #
        job = DbJob(func, args, kwargs, parent,
                    args_hash=args_hash,
                    args_dump=args_dump)

        self.save_job(job)

        return job

    def save_job(self, job):
        if job.id:
            self.cursor.execute("""
                                update job set
                                status = ?,
                                result_dump = ?
                                where id = ?
                                """,
                                (int(job.done),
                                 DbJob.dump(job.result),
                                 job.id))

            log.info("Updated %s", job)
        else:
            self.cursor.execute("""
                                insert into job (parent_id, status, func,
                                                 args_hash, args_dump, result_dump)
                                values (?, ?, ?, ?, ?, ?)
                                """,
                                (job.parent.id if job.parent else None,
                                 int(job.done),
                                 job.func.__name__,
                                 job.args_hash,
                                 job.args_dump,
                                 DbJob.dump(job.result)))

            job.id = self.cursor.lastrowid

            log.info("Inserted %s", job)

        self.db.commit()

