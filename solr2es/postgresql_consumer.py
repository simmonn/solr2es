from json import dumps

from psycopg2.extras import execute_values


class PostgresqlConsumer(object):
    def __init__(self, connection) -> None:
        self.connection = connection
        self.connection.set_isolation_level(0)

    def consume(self, producer):
        self.create_table_if_not_exists()
        for results in producer():
            cursor = self.connection.cursor()
            values = ((r['extract_id'], dumps(r)) for r in results)
            insert_query = 'INSERT INTO solr2es_queue (extract_id, json) VALUES %s'
            execute_values(cursor, insert_query, values)
            cursor.close()

    def create_table_if_not_exists(self):
        cursor = self.connection.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS "solr2es_queue" ('
                       'id serial primary key,'
                       'extract_id varchar(64) not null UNIQUE,'
                       'json text not null)')
        cursor.close()


class PostgresqlConsumerAsync(object):
    def __init__(self, postgresql) -> None:
        self.postgresql = postgresql

    async def consume(self, producer):
        await self.create_table_if_not_exists()
        async for results in producer():
            async with self.postgresql.acquire() as conn:
                async with conn.cursor() as cur:
                    values = ('(%r, %r)' % (r['extract_id'], dumps(r)) for r in results)
                    insert_query = 'INSERT INTO solr2es_queue (extract_id, json) VALUES %s' % ','.join(values)
                    await cur.execute(insert_query)

    async def create_table_if_not_exists(self):
        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('CREATE TABLE IF NOT EXISTS "solr2es_queue" ('
                                  'id serial primary key,'
                                  'extract_id varchar(64) not null UNIQUE,'
                                  'json text not null)')
