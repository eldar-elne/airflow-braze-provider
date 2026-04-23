<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/d/de/AirflowLogo.png" alt="Airflow" height="60">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="https://phrase.com/wp-content/uploads/2024/12/Braze_Primary_logo_PURPLE_CROPPED-1024x516.png" alt="Braze" height="60">
</p>

<h1 align="center">airflow-provider-braze</h1>

<p align="center">
  An Apache Airflow provider for <a href="https://www.braze.com/">Braze</a> REST API.
</p>

<p align="center">
  <a href="https://pypi.org/project/airflow-provider-braze/"><img src="https://img.shields.io/pypi/v/airflow-provider-braze" alt="PyPI"></a>
  <a href="https://pypi.org/project/airflow-provider-braze/"><img src="https://img.shields.io/pypi/pyversions/airflow-provider-braze" alt="Python"></a>
  <a href="https://github.com/eldar-elne/airflow-braze-provider/blob/main/LICENSE"><img src="https://img.shields.io/github/license/eldar-elne/airflow-braze-provider" alt="License"></a>
  <a href="https://github.com/eldar-elne/airflow-braze-provider/actions/workflows/tests.yml"><img src="https://github.com/eldar-elne/airflow-braze-provider/actions/workflows/tests.yml/badge.svg" alt="Tests"</a>
</p>
---

## Installation

```bash
pip install airflow-provider-braze
```

Requires Apache Airflow >= 2.9.0.

## Connection Setup

In the Airflow UI, create a connection with:

| Field | Value |
|-------|-------|
| **Connection Type** | Braze |
| **Host** | Your Braze REST endpoint (e.g. `https://rest.iad-03.braze.com`) |
| **Password** | Your Braze REST API key |

## Usage

```python
from braze_provider.operators.braze_cdi import BrazeRunCDIJobOperator

sync_braze = BrazeRunCDIJobOperator(
    task_id="sync_braze_cdi",
    integration_id="your-integration-id",
    braze_conn_id="braze_default",
    poll_interval=30,
    timeout=3600,
    wait_for_completion=True,
)
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `integration_id` | — | Braze CDI integration ID (templatable) |
| `braze_conn_id` | `braze_default` | Airflow connection ID |
| `poll_interval` | `30` | Seconds between status checks |
| `timeout` | `3600` | Max seconds to wait |
| `wait_for_completion` | `True` | Set `False` to trigger and move on |

The operator returns the full job result dict via XCom, including `rows_synced` and `rows_failed_with_errors`.


## Contributing

Currently this provider only supports **Cloud Data Ingestion (CDI)**. The `BrazeHook` wraps the Braze REST API and can be extended to support additional endpoints. Contributions are welcome — feel free to open a PR!

## License

Apache 2.0
