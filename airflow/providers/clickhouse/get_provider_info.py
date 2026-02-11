# 
# 
# 

def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-clickhouse",
        "name": "ClickHouse",
        "description": "`ClickHouse <https://clickhouse.com/>`__\n",
        "integrations": [
            {
                "integration-name": "ClickHouse",
                "external-doc-url": "https://clickhouse.com/",
                "how-to-guide": ["/docs"],
                "logo": "/docs/img/ch_logo_docs.svg",
                "tags": ["service"],
            }
        ],
        # "dialects": [
        #     {
        #         "dialect-type": "clickhouse",
        #         "dialect-class-name": "airflow.providers.postgres.dialects.postgres.PostgresDialect",
        #     }
        # ],
        "hooks": [
            {
                "integration-name": "ClickHouse",
                "python-modules": ["airflow.providers.clickhouse.hooks.clickhouse"],
            }
        ],
        "operators": [
            {
                "integration-name": "ClickHouse",
                "python-modules": ["airflow.providers.clickhouse.operators.clickhouse"],
            },
        ],
        "sensors": [
            {
                "integration-name": "ClickHouse",
                "python-modules": ["airflow.providers.clickhouse.sensors.clickhouse"],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.clickhouse.hooks.clickhouse.ClickHouseHook",
                "connection-type": "clickhouse",
            }
        ],
        # "asset-uris": [
        #     {
        #         "schemes": ["postgres", "postgresql"],
        #         "handler": "airflow.providers.postgres.assets.postgres.sanitize_uri",
        #     }
        # ],
        # "dataset-uris": [
        #     {
        #         "schemes": ["postgres", "postgresql"],
        #         "handler": "airflow.providers.postgres.assets.postgres.sanitize_uri",
        #     }
        # ],
        "config": {
            "postgres": {
                "description": "Configuration for ClickHouse hooks and operators.\n",
                "options": {},
            }
        },
    }
