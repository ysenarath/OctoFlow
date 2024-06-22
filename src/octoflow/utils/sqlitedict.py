import functools
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Generator, Optional, Union

from filelock import FileLock, Timeout
from sqlalchemy import JSON, Column, String, Table, create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import registry

mapper_registry = registry()

kvtable = Table(
    "kvitem",
    mapper_registry.metadata,
    Column("key", String, primary_key=True),
    Column("value", JSON, nullable=True),
)


class Lockable:
    lock: Optional[FileLock]


def with_lock(func, timeout: Optional[float] = 60, retries: int = 3):
    @functools.wraps(func)
    def wrapper(self: Lockable, *args, **kwargs):
        if self.lock is not None:
            for i in range(retries):
                try:
                    with self.lock.acquire(timeout=timeout):
                        return func(self, *args, **kwargs)
                except Timeout:
                    if i + 1 < retries:
                        continue
                    msg = "timeout waiting for lock"
                    raise Timeout(msg) from None
        else:
            return func(self, *args, **kwargs)

    return wrapper


class SQLiteDict(MutableMapping):
    def __init__(
        self,
        path: Union[Path, str, None] = None,
        verbose: bool = False,
    ):
        if path is None:
            url = "sqlite:///:memory:"
            path = None
        else:
            url = f"sqlite:///{path}"
            path = Path(path)
        engine = create_engine(url, echo=verbose)
        mapper_registry.metadata.create_all(engine)
        self.engine = engine
        lock = None
        if path is not None:
            lock = FileLock(path.with_suffix(path.suffix + ".lock"))
        self.lock = lock

    @with_lock
    def __getitem__(self, __key: Any) -> Any:
        query = kvtable.select().where(kvtable.c.key == __key)
        with self.engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            if row is None:
                raise KeyError(__key)
            return row[1]

    @with_lock
    def __setitem__(self, __key: Any, __value: Any) -> None:
        query = kvtable.insert().values(key=__key, value=__value)
        with self.engine.connect() as conn:
            try:
                conn.execute(query)
                conn.commit()
            except IntegrityError:
                query = (
                    kvtable.update()
                    .where(kvtable.c.key == __key)
                    .values(value=__value)
                )
                conn.execute(query)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

    @with_lock
    def __delitem__(self, __key: Any) -> None:
        query = kvtable.delete().where(kvtable.c.key == __key)
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    @with_lock
    def __iter__(self) -> Generator[str, None, None]:
        query = kvtable.select()
        with self.engine.connect() as conn:
            result = conn.execute(query)
            for row in result:
                yield row[0]

    @with_lock
    def __len__(self):
        count_query = select(func.count()).select_from(kvtable)
        with self.engine.connect() as conn:
            result = conn.execute(count_query).scalar()
            return result

    @with_lock
    def __contains__(self, __key: object) -> bool:
        query = kvtable.select().where(kvtable.c.key == __key)
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return result.fetchone() is not None

    def __repr__(self) -> str:
        url = self.engine.url.database
        length = len(self)
        return f'{self.__class__.__name__}(url="{url}", length={length})'
