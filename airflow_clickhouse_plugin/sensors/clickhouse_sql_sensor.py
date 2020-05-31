from typing import Dict, Callable, Any

from airflow.exceptions import AirflowException
from airflow.sensors.sql_sensor import SqlSensor
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class ClickHouseSqlSensor(SqlSensor):
    def __init__(
        self,
        sql: str = None,
        clickhouse_conn_id: str = 'clickhouse_default',
        parameters: Dict[str, Any] = None,
        database: str = None,
        success: Callable[[Any], bool] = None,
        failure: Callable[[Any], bool] = None,
        fail_on_empty: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(
            conn_id=clickhouse_conn_id,
            sql=sql,
            parameters=parameters,
            success=success,
            failure=failure,
            fail_on_empty=fail_on_empty,
            *args,
            **kwargs,
        )
        self._database = database

    def poke(self, context):
        """
        https://github.com/apache/airflow/blob/1.10.10/airflow/sensors/sql_sensor.py#L86
        """

        hook = ClickHouseHook(clickhouse_conn_id=self.conn_id, database=self._database,)

        self.log.info('Poking: %s (with parameters %s)', self.sql, self.parameters)
        records = hook.get_records(self.sql, self.parameters)
        if not records:
            if self.fail_on_empty:
                raise AirflowException(
                    'No rows returned, raising as per fail_on_empty flag'
                )
            else:
                return False
        first_cell = records[0][0]
        if self.failure is not None:
            if callable(self.failure):
                if self.failure(first_cell):
                    raise AirflowException(
                        'Failure criteria met. self.failure({}) returned True'.format(
                            first_cell
                        )
                    )
            else:
                raise AirflowException(
                    'self.failure is present, but not callable -> {}'.format(
                        self.success
                    )
                )
        if self.success is not None:
            if callable(self.success):
                return self.success(first_cell)
            else:
                raise AirflowException(
                    'self.success is present, but not callable -> {}'.format(
                        self.success
                    )
                )
        return bool(first_cell)
