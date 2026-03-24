from __future__ import annotations

import time
from enum import Enum
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class BrazeCDIJobStatus(Enum):
    """Enum for Braze CDI job statuses."""

    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"
    CONFIG_ERROR = "config_error"

    @classmethod
    def terminal_statuses(cls) -> set[str]:
        return {cls.SUCCESS.value, cls.PARTIAL.value, cls.ERROR.value, cls.CONFIG_ERROR.value}

    @classmethod
    def error_statuses(cls) -> set[str]:
        return {cls.ERROR.value, cls.CONFIG_ERROR.value}


class BrazeHook(HttpHook):
    """Hook for interacting with the Braze CDI REST API."""

    conn_name_attr = "braze_conn_id"
    default_conn_name = "braze_default"
    conn_type = "braze"
    hook_name = "Braze"

    def __init__(self, braze_conn_id: str = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(http_conn_id=braze_conn_id, *args, **kwargs)
        self.braze_conn_id = braze_conn_id

    def _get_headers(self) -> dict[str, str]:
        conn = self.get_connection(self.braze_conn_id)
        return {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
        }
        

    def _get_base_url(self) -> str:
        conn = self.get_connection(self.braze_conn_id)
        host = conn.host or ""
        if not host.startswith("https://"):
            host = f"https://{host}"
        return host.rstrip("/")

    def trigger_cdi_job_sync(self, integration_id: str) -> dict[str, Any]:
        """Trigger a CDI sync job for the given integration."""
        base_url = self._get_base_url()
        endpoint = f"/cdi/integrations/{integration_id}/sync"
        self.method = "POST"
        response = self.run(
            endpoint=endpoint,
            headers=self._get_headers(),
            extra_options={"check_response": True},
        )
        response.url = f"{base_url}{endpoint}"
        return response.json()

    def get_cdi_job_sync_status(self, integration_id: str) -> list[dict[str, Any]]:
        """Get the sync status for a CDI integration."""
        base_url = self._get_base_url()
        endpoint = f"/cdi/integrations/{integration_id}/job_sync_status"
        self.method = "GET"
        response = self.run(
            endpoint=endpoint,
            headers=self._get_headers(),
            extra_options={"check_response": True},
        )
        response.url = f"{base_url}{endpoint}"
        return response.json()

    def wait_for_cdi_job(
        self,
        integration_id: str,
        poll_interval: int = 30,
        timeout: int = 3600,
    ) -> dict[str, Any]:
        """Poll CDI job status until it reaches a terminal state."""
        start_time = time.monotonic()

        while True:
            results = self.get_cdi_job_sync_status(integration_id)
            if not results:
                raise AirflowException(
                    f"No sync status results found for integration {integration_id}"
                )

            job = results[0]
            status = job.get("job_status", "")
            self.log.info("CDI job status for integration %s: %s", integration_id, status)

            if status in BrazeCDIJobStatus.terminal_statuses():
                if status in BrazeCDIJobStatus.error_statuses():
                    raise AirflowException(
                        f"CDI job for integration {integration_id} failed with status '{status}': {job}"
                    )
                return job

            elapsed = time.monotonic() - start_time
            if elapsed + poll_interval > timeout:
                raise AirflowException(
                    f"CDI job for integration {integration_id} timed out after {timeout}s. "
                    f"Last status: '{status}'"
                )

            time.sleep(poll_interval)

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema", "port", "login", "extra"],
            "relabeling": {
                "host": "Braze REST Endpoint",
                "password": "Braze REST API Key",
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test the Braze connection."""
        try:
            conn = self.get_connection(self.braze_conn_id)
            if not conn.host:
                return False, "Missing Braze REST Endpoint (host)"
            if not conn.password:
                return False, "Missing Braze REST API Key (password)"
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
