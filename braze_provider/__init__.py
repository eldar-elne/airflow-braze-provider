__version__ = "1.0.3"

import packaging.version
from airflow import __version__ as airflow_version

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
    "2.9.0"
):
    raise RuntimeError(
        f"The package `airflow-provider-braze` needs Apache Airflow 2.9.0+, "
        f"but the current version is {airflow_version}"
    )


def get_provider_info():
    return {
        "package-name": "airflow-provider-braze",
        "name": "Braze",
        "description": "A Braze provider for Apache Airflow.",
        "hook-class-names": ["braze_provider.hooks.braze.BrazeHook"],
        "connection-type": [
            {"hook-class-name": "braze_provider.hooks.braze.BrazeHook", "connection-type": "braze"}
        ],
    }
