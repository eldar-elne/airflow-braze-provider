from __future__ import annotations

import time
from datetime import datetime
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
            extra_options={"check_response": False},
        )
        response.url = f"{base_url}{endpoint}"
        if response.status_code == 429:
            body = response.json()
            message = body.get("message", "")
            raise AirflowException(message)
        response.raise_for_status()
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
        return response.json()["results"]

    def wait_for_cdi_job(
        self,
        integration_id: str,
        triggered_after: datetime,
        poll_interval: int = 30,
        timeout: int = 3600,
    ) -> dict[str, Any]:
        """Poll CDI job status until it reaches a terminal state.

        :param triggered_after: Only consider jobs whose sync_start_time is
            at or after this timestamp. This avoids picking up stale runs
            that were already visible before the trigger.
        """
        start_time = time.monotonic()

        while True:
            results = self.get_cdi_job_sync_status(integration_id)
            job = self._find_job_after(results, triggered_after)

            if job is None:
                elapsed = time.monotonic() - start_time
                if elapsed + poll_interval > timeout:
                    raise AirflowException(
                        f"CDI job for integration {integration_id} did not appear "
                        f"within {timeout}s after trigger."
                    )
                self.log.info(
                    "No CDI job found yet for integration %s triggered after %s, retrying...",
                    integration_id,
                    triggered_after.isoformat(),
                )
                time.sleep(poll_interval)
                continue

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

    @staticmethod
    def _find_job_after(
        results: list[dict[str, Any]], triggered_after: datetime
    ) -> dict[str, Any] | None:
        """Return the most recent job if its sync_start_time >= triggered_after."""
        if not results:
            return None
        job = results[0]
        sync_start = job.get("sync_start_time", "")
        if not sync_start:
            return None
        job_time = datetime.fromisoformat(sync_start.replace("Z", "+00:00"))
        return job if job_time >= triggered_after else None

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
