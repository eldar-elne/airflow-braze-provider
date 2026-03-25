from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from airflow.models import BaseOperator

from braze_provider.hooks.braze import BrazeHook


class BrazeRunCDIJobOperator(BaseOperator):
    """Trigger a Braze CDI sync job and wait for it to complete.

    :param integration_id: The Braze CDI integration ID to sync.
    :param braze_conn_id: The Airflow connection ID for Braze.
    :param poll_interval: Seconds between status checks (default: 30).
    :param timeout: Maximum seconds to wait for job completion (default: 3600).
    :param wait_for_completion: Whether to wait for the job to finish (default: True).
    """

    template_fields = ("integration_id",)

    def __init__(
        self,
        integration_id: str,
        braze_conn_id: str = BrazeHook.default_conn_name,
        poll_interval: int = 30,
        timeout: int = 3600,
        wait_for_completion: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.integration_id = integration_id
        self.braze_conn_id = braze_conn_id
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Any) -> dict[str, Any]:
        hook = BrazeHook(braze_conn_id=self.braze_conn_id)

        self.log.info("Triggering CDI sync for integration %s", self.integration_id)
        triggered_after = datetime.now(timezone.utc)
        trigger_response = hook.trigger_cdi_job_sync(self.integration_id)
        self.log.info("Trigger response: %s", trigger_response)

        if not self.wait_for_completion:
            self.log.info("Not waiting for CDI job to complete...")
            return trigger_response

        self.log.info("Waiting for CDI job to complete...")
        result = hook.wait_for_cdi_job(
            integration_id=self.integration_id,
            triggered_after=triggered_after,
            poll_interval=self.poll_interval,
            timeout=self.timeout,
        )
        self.log.info(
            "CDI job completed with status '%s' — rows synced: %s, rows failed: %s",
            result.get("job_status"),
            result.get("rows_synced"),
            result.get("rows_failed_with_errors"),
        )
        return result
