from json import dumps

from psycopg2.extras import execute_values

CREATE_TABLE_SQL = 'CREATE TABLE IF NOT EXISTS "solr2es_queue" (' \
                   'id serial primary key,' \
                   'extract_id varchar(64) not null UNIQUE,' \
                   'json text not null,' \
                   'done boolean default false )'

INSERT_SQL = 'INSERT INTO solr2es_queue (extract_id, json) VALUES %s'


class PostgresqlQueue(object):
    def __init__(self, connection) -> None:
        self.connection = connection
        self.connection.set_isolation_level(0)

    def push_loop(self, producer):
        self.create_table_if_not_exists()
        for results in producer():
            self.push(results)

    def push(self, value_list):
        cursor = self.connection.cursor()
        values = ((r['extract_id'], dumps(r)) for r in value_list)
        execute_values(cursor, INSERT_SQL, values)
        cursor.close()

    def create_table_if_not_exists(self):
        cursor = self.connection.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        cursor.close()


class PostgresqlQueueAsync(object):
    def __init__(self, postgresql) -> None:
        self.postgresql = postgresql

    async def push_loop(self, producer):
        await self.create_table_if_not_exists()
        async for results in producer():
            await self.push(results)

    async def push(self, value_list):
        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                values = ('(%r, %r)' % (r['extract_id'], dumps(r)) for r in value_list)
                await cur.execute(INSERT_SQL % ','.join(values))

    async def create_table_if_not_exists(self):
        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(CREATE_TABLE_SQL)
