from airflow.providers.common.sql.sensors.sql import SqlSensor

from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseHookMixin


class ClickHouseSqlSensor(ClickHouseHookMixin, SqlSensor):
    def _get_hook(self) -> ClickHouseHook:
        return self._get_clickhouse_hook()
