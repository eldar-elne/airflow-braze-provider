from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from braze_provider.operators.braze_cdi import BrazeRunCDIJobOperator


INTEGRATION_ID = "test-integration-123"


@pytest.fixture
def operator():
    return BrazeRunCDIJobOperator(
        task_id="test_cdi_sync",
        integration_id=INTEGRATION_ID,
        poll_interval=1,
        timeout=10,
    )


@pytest.fixture
def operator_no_wait():
    return BrazeRunCDIJobOperator(
        task_id="test_cdi_sync_no_wait",
        integration_id=INTEGRATION_ID,
        wait_for_completion=False,
    )


class TestBrazeRunCDIJobOperator:
    def test_template_fields(self):
        assert "integration_id" in BrazeRunCDIJobOperator.template_fields

    def test_execute_with_wait(self, operator):
        trigger_response = {"message": "success"}
        job_result = {"job_status": "success", "rows_synced": 150, "rows_failed_with_errors": 0}

        with patch("braze_provider.operators.braze_cdi.BrazeHook") as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook_cls.return_value = mock_hook
            mock_hook.trigger_cdi_job_sync.return_value = trigger_response
            mock_hook.wait_for_cdi_job.return_value = job_result

            result = operator.execute(context={})

        mock_hook.trigger_cdi_job_sync.assert_called_once_with(INTEGRATION_ID)
        mock_hook.wait_for_cdi_job.assert_called_once_with(
            integration_id=INTEGRATION_ID,
            poll_interval=1,
            timeout=10,
        )
        assert result == job_result

    def test_execute_without_wait(self, operator_no_wait):
        trigger_response = {"message": "success"}

        with patch("braze_provider.operators.braze_cdi.BrazeHook") as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook_cls.return_value = mock_hook
            mock_hook.trigger_cdi_job_sync.return_value = trigger_response

            result = operator_no_wait.execute(context={})

        mock_hook.trigger_cdi_job_sync.assert_called_once_with(INTEGRATION_ID)
        mock_hook.wait_for_cdi_job.assert_not_called()
        assert result == trigger_response

    def test_execute_error_propagates(self, operator):
        with patch("braze_provider.operators.braze_cdi.BrazeHook") as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook_cls.return_value = mock_hook
            mock_hook.trigger_cdi_job_sync.return_value = {"message": "success"}
            mock_hook.wait_for_cdi_job.side_effect = AirflowException("Job failed")

            with pytest.raises(AirflowException, match="Job failed"):
                operator.execute(context={})

    def test_execute_trigger_failure(self, operator):
        with patch("braze_provider.operators.braze_cdi.BrazeHook") as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook_cls.return_value = mock_hook
            mock_hook.trigger_cdi_job_sync.side_effect = AirflowException("401 Unauthorized")

            with pytest.raises(AirflowException, match="401 Unauthorized"):
                operator.execute(context={})

    def test_default_conn_id(self):
        op = BrazeRunCDIJobOperator(task_id="test", integration_id="id")
        assert op.braze_conn_id == "braze_default"

    def test_custom_conn_id(self):
        op = BrazeRunCDIJobOperator(task_id="test", integration_id="id", braze_conn_id="my_braze")
        assert op.braze_conn_id == "my_braze"
