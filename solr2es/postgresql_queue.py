from asyncio import ensure_future, wait_for, futures
from json import dumps, loads
import sqlalchemy as sa

from psycopg2.extras import execute_values
from sqlalchemy.dialects.postgresql import insert

POP_DOCS_SQL = 'UPDATE solr2es_queue SET done = \'t\' WHERE uid IN (' \
               'SELECT uid FROM solr2es_queue WHERE done = \'f\' ORDER BY uid ' \
               'FOR UPDATE SKIP LOCKED LIMIT 10) RETURNING json'

CREATE_TABLE_SQL = 'CREATE TABLE IF NOT EXISTS "solr2es_queue" (' \
                   'uid serial primary key,' \
                   'id varchar(64) not null UNIQUE,' \
                   'json text not null,' \
                   'done boolean default false )'

INSERT_SQL = 'INSERT INTO solr2es_queue (id, json) VALUES %s'
SIZE_SQL = 'SELECT COUNT(*) FROM solr2es_queue WHERE done = \'f\''

metadata = sa.MetaData()
queue_table = sa.Table('solr2es_queue', metadata,
               sa.Column('uid', sa.Integer, primary_key=True),
               sa.Column('id', sa.String(64)),
               sa.Column('json', sa.Text()))


class PostgresqlQueue(object):
    def __init__(self, connection, unique_id='id') -> None:
        self.unique_id = unique_id
        self.connection = connection
        self.connection.set_isolation_level(0)

    def push_loop(self, producer):
        self.create_table_if_not_exists()
        for results in producer():
            self.push(results)

    def push(self, value_list):
        cursor = self.connection.cursor()
        values = ((r[self.unique_id], dumps(r)) for r in value_list)
        execute_values(cursor, INSERT_SQL, values)
        cursor.close()

    def create_table_if_not_exists(self):
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        cursor.close()


class PostgresqlQueueAsync(object):
    def __init__(self, postgresql, unique_id='id') -> None:
        self.unique_id = unique_id
        self.postgresql = postgresql

    @classmethod
    async def create(cls, postgresql, unique_id='id'):
        self = cls(postgresql, unique_id=unique_id)
        await self.create_table_if_not_exists()
        return self

    async def push_loop(self, producer):
        async for results in producer():
            await self.push(results)

    async def push(self, value_list) -> None:
        async with self.postgresql.acquire() as conn:
            values = list(({'id': r[self.unique_id], 'json': dumps(r)} for r in value_list))
            await conn.execute(insert(queue_table).values(values).on_conflict_do_nothing(index_elements=['id']))
            await conn.execute('NOTIFY solr2es, \'notify\'')

    async def create_table_if_not_exists(self):
        async with self.postgresql.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)

    async def pop(self, timeout=1) -> list:
        async with self.postgresql.acquire() as conn:
            await conn.execute("LISTEN solr2es")
            while True:
                async with conn.begin():
                    result_proxy = await conn.execute(POP_DOCS_SQL)
                    if result_proxy.rowcount:
                        return list(map(lambda row: loads(row[0]), await result_proxy.fetchall()))
                notification = ensure_future(conn.connection.notifies.get())
                try:
                    await wait_for(notification, timeout)
                except futures.TimeoutError:
                    return []

    async def size(self) -> int:
        async with self.postgresql.acquire() as conn:
            result_proxy = await conn.execute(SIZE_SQL)
            return await result_proxy.scalar()
