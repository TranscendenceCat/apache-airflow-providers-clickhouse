import typing as t

from airflow.providers.common.sql.operators import sql

from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook


class ClickHouseHookMixin(object):
    # these attributes are defined in both BaseSQLOperator and SqlSensor
    conn_id: str
    hook_params: t.Optional[dict]

    def _get_clickhouse_hook(self, **extra_hook_params) -> ClickHouseHook:
        hook_kwargs = {}
        if self.conn_id is not None:
            hook_kwargs['clickhouse_conn_id'] = self.conn_id
        if self.hook_params is not None:
            hook_kwargs.update(self.hook_params)
        hook_kwargs.update(extra_hook_params)
        return ClickHouseHook(**hook_kwargs)


class ClickHouseOperator(ClickHouseHookMixin, sql.BaseSQLOperator):
    def get_db_hook(self) -> ClickHouseHook:
        return self._get_clickhouse_hook(schema=self.database)


class ClickHouseSQLExecuteQueryOperator(
    ClickHouseOperator,
    sql.SQLExecuteQueryOperator,
):
    pass


class ClickHouseSQLColumnCheckOperator(
    ClickHouseOperator,
    sql.SQLColumnCheckOperator,
):
    pass


class ClickHouseSQLTableCheckOperator(
    ClickHouseOperator,
    sql.SQLTableCheckOperator,
):
    pass


class ClickHouseSQLCheckOperator(
    ClickHouseOperator,
    sql.SQLCheckOperator,
):
    pass


class ClickHouseSQLValueCheckOperator(
    ClickHouseOperator,
    sql.SQLValueCheckOperator,
):
    pass


class ClickHouseSQLIntervalCheckOperator(
    ClickHouseOperator,
    sql.SQLIntervalCheckOperator,
):
    pass


class ClickHouseSQLThresholdCheckOperator(
    ClickHouseOperator,
    sql.SQLThresholdCheckOperator,
):
    pass


class ClickHouseBranchSQLOperator(
    ClickHouseOperator,
    sql.BranchSQLOperator,
):
    pass
