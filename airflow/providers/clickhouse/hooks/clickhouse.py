import clickhouse_driver
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.models import Connection



default_conn_name = 'clickhouse_default'

class ClickHouseHook(DbApiHook):
    conn_name_attr = 'clickhouse_conn_id'
    clickhouse_conn_id: str  # set by DbApiHook.__init__
    default_conn_name = default_conn_name

    def __init__(self, *args, schema: str = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema = schema

    def get_conn(self) -> clickhouse_driver.dbapi.Connection:
        airflow_conn = self.get_connection(self.clickhouse_conn_id)
        return clickhouse_driver.dbapi \
            .connect(**conn_to_kwargs(airflow_conn, self._schema))



def conn_to_kwargs(conn: Connection, database: t.Optional[str]) -> t.Dict[str, t.Any]:
    """ Translate Airflow Connection to clickhouse-driver Connection kwargs. """
    connection_kwargs = conn.extra_dejson.copy()
    # Connection attributes can be parsed to empty strings by urllib.unparse
    connection_kwargs['host'] = conn.host or 'localhost'
    if conn.port:
        connection_kwargs.update(port=conn.port)
    if conn.login:
        connection_kwargs.update(user=conn.login)
    if conn.password:
        connection_kwargs.update(password=conn.password)
    if database is not None:
        connection_kwargs.update(database=database)
    elif conn.schema:
        connection_kwargs.update(database=conn.schema)
    return connection_kwargs
