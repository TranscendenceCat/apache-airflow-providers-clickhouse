#
#
#

# from __future__ import annotations

# import packaging.version

# import importlib.metadata
# airflow_version = importlib.metadata.version('apache-airflow')

# # from airflow.sdk import __version__ as airflow_version

# __all__ = ["__version__"]

# __version__ = "0.0.1"

# if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
#     "3.0.0"
# ):
#     raise RuntimeError(
#         f"The package `apache-airflow-providers-clickhouse:{__version__}` needs Apache Airflow 3+"
#     )
