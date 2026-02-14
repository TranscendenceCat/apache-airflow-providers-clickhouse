from airflow.providers.common.sql.sensors.sql import SqlSensor

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseHookMixin


class ClickHouseSqlSensor(ClickHouseHookMixin, SqlSensor):
    def _get_hook(self) -> ClickHouseHook:
        return self._get_clickhouse_db_api_hook()
