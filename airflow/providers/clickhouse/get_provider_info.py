# 
# 
# 

def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-clickhouse",
        "name": "ClickHouse",
        "description": "`ClickHouse <https://clickhouse.com/>`__\n",
        'versions': ['0.1.1'],
        "integrations": [
            {
                "integration-name": "ClickHouse",
                "external-doc-url": "https://clickhouse.com/",
                "how-to-guide": ["/docs"],
                "logo": "/docs/img/ch_logo_docs.svg",
                "tags": ["service"],
            }
        ],
        "hooks": [
            {
                "integration-name": "ClickHouse",
                "python-modules": ["airflow.providers.clickhouse.hooks.clickhouse"],
            }
        ],
        'hook-class-names': ['airflow.providers.clickhouse.hooks.clickhouse.ClickHouseHook'],
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
        "config": {
            "clickhouse": {
                "description": "Configuration for ClickHouse hooks and operators.\n",
                "options": {},
            }
        },
    }
