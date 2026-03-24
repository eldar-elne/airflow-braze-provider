from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from braze_provider.hooks.braze import BrazeCDIJobStatus, BrazeHook


INTEGRATION_ID = "test-integration-123"


@pytest.fixture
def mock_connection():
    return Connection(
        conn_id="braze_default",
        conn_type="braze",
        host="https://rest.iad-03.braze.com",
        password="test-api-key",
    )


@pytest.fixture
def hook(mock_connection):
    with patch.object(BrazeHook, "get_connection", return_value=mock_connection):
        h = BrazeHook()
        yield h


class TestBrazeCDIJobStatus:
    def test_terminal_statuses(self):
        expected = {"success", "partial", "error", "config_error"}
        assert BrazeCDIJobStatus.terminal_statuses() == expected

    def test_error_statuses(self):
        expected = {"error", "config_error"}
        assert BrazeCDIJobStatus.error_statuses() == expected

    def test_enum_values(self):
        assert BrazeCDIJobStatus.RUNNING.value == "running"
        assert BrazeCDIJobStatus.SUCCESS.value == "success"
        assert BrazeCDIJobStatus.PARTIAL.value == "partial"
        assert BrazeCDIJobStatus.ERROR.value == "error"
        assert BrazeCDIJobStatus.CONFIG_ERROR.value == "config_error"


class TestBrazeHookConnection:
    def test_default_conn_name(self):
        assert BrazeHook.default_conn_name == "braze_default"

    def test_conn_type(self):
        assert BrazeHook.conn_type == "braze"

    def test_hook_name(self):
        assert BrazeHook.hook_name == "Braze"

    def test_get_headers(self, hook, mock_connection):
        with patch.object(hook, "get_connection", return_value=mock_connection):
            headers = hook._get_headers()
        assert headers == {
            "Authorization": "Bearer test-api-key",
            "Content-Type": "application/json",
        }

    def test_get_base_url(self, hook, mock_connection):
        with patch.object(hook, "get_connection", return_value=mock_connection):
            assert hook._get_base_url() == "https://rest.iad-03.braze.com"

    def test_get_base_url_without_https(self, hook):
        conn = Connection(
            conn_id="braze_default",
            conn_type="braze",
            host="rest.iad-03.braze.com",
            password="test-api-key",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            assert hook._get_base_url() == "https://rest.iad-03.braze.com"

    def test_get_base_url_strips_trailing_slash(self, hook):
        conn = Connection(
            conn_id="braze_default",
            conn_type="braze",
            host="https://rest.iad-03.braze.com/",
            password="test-api-key",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            assert hook._get_base_url() == "https://rest.iad-03.braze.com"


class TestBrazeHookUI:
    def test_get_ui_field_behaviour(self):
        behaviour = BrazeHook.get_ui_field_behaviour()
        assert "schema" in behaviour["hidden_fields"]
        assert "port" in behaviour["hidden_fields"]
        assert "login" in behaviour["hidden_fields"]
        assert "extra" in behaviour["hidden_fields"]
        assert behaviour["relabeling"]["host"] == "Braze REST Endpoint"
        assert behaviour["relabeling"]["password"] == "Braze REST API Key"


class TestBrazeHookTestConnection:
    def test_test_connection_success(self, hook, mock_connection):
        with patch.object(hook, "get_connection", return_value=mock_connection):
            result, msg = hook.test_connection()
        assert result is True
        assert msg == "Connection successfully tested"

    def test_test_connection_missing_host(self, hook):
        conn = Connection(conn_id="braze_default", conn_type="braze", password="key")
        with patch.object(hook, "get_connection", return_value=conn):
            result, msg = hook.test_connection()
        assert result is False
        assert "Missing Braze REST Endpoint" in msg

    def test_test_connection_missing_password(self, hook):
        conn = Connection(conn_id="braze_default", conn_type="braze", host="https://rest.braze.com")
        with patch.object(hook, "get_connection", return_value=conn):
            result, msg = hook.test_connection()
        assert result is False
        assert "Missing Braze REST API Key" in msg


class TestTriggerCDIJobSync:
    def test_trigger_success(self, hook, mock_connection):
        mock_response = MagicMock()
        mock_response.json.return_value = {"message": "success"}
        with patch.object(hook, "get_connection", return_value=mock_connection), patch.object(
            hook, "run", return_value=mock_response
        ) as mock_run:
            result = hook.trigger_cdi_job_sync(INTEGRATION_ID)

        assert result == {"message": "success"}
        mock_run.assert_called_once_with(
            endpoint=f"/cdi/integrations/{INTEGRATION_ID}/sync",
            headers={
                "Authorization": "Bearer test-api-key",
                "Content-Type": "application/json",
            },
            extra_options={"check_response": True},
        )

    def test_trigger_failure(self, hook, mock_connection):
        with patch.object(hook, "get_connection", return_value=mock_connection), patch.object(
            hook, "run", side_effect=AirflowException("401 Unauthorized")
        ):
            with pytest.raises(AirflowException, match="401 Unauthorized"):
                hook.trigger_cdi_job_sync(INTEGRATION_ID)


class TestGetCDIJobSyncStatus:
    def test_get_status_success(self, hook, mock_connection):
        status_data = [
            {
                "job_status": "success",
                "rows_synced": 150,
                "rows_failed_with_errors": 0,
                "sync_start_time": "2024-01-01T00:00:00Z",
                "sync_finish_time": "2024-01-01T00:05:00Z",
                "last_timestamp_synced": "2024-01-01T00:04:00Z",
            }
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = status_data
        with patch.object(hook, "get_connection", return_value=mock_connection), patch.object(
            hook, "run", return_value=mock_response
        ):
            result = hook.get_cdi_job_sync_status(INTEGRATION_ID)

        assert result == status_data


class TestWaitForCDIJob:
    def test_wait_success_immediate(self, hook):
        job_result = {"job_status": "success", "rows_synced": 100, "rows_failed_with_errors": 0}
        with patch.object(hook, "get_cdi_job_sync_status", return_value=[job_result]):
            result = hook.wait_for_cdi_job(INTEGRATION_ID)
        assert result == job_result

    def test_wait_partial_success(self, hook):
        job_result = {"job_status": "partial", "rows_synced": 80, "rows_failed_with_errors": 20}
        with patch.object(hook, "get_cdi_job_sync_status", return_value=[job_result]):
            result = hook.wait_for_cdi_job(INTEGRATION_ID)
        assert result == job_result

    def test_wait_polls_then_succeeds(self, hook):
        running = [{"job_status": "running"}]
        success = [{"job_status": "success", "rows_synced": 100, "rows_failed_with_errors": 0}]
        with patch.object(
            hook, "get_cdi_job_sync_status", side_effect=[running, success]
        ), patch("braze_provider.hooks.braze.time.sleep"):
            result = hook.wait_for_cdi_job(INTEGRATION_ID, poll_interval=1, timeout=10)
        assert result["job_status"] == "success"

    def test_wait_error_raises(self, hook):
        error_job = [{"job_status": "error", "message": "Something went wrong"}]
        with patch.object(hook, "get_cdi_job_sync_status", return_value=error_job):
            with pytest.raises(AirflowException, match="failed with status 'error'"):
                hook.wait_for_cdi_job(INTEGRATION_ID)

    def test_wait_config_error_raises(self, hook):
        error_job = [{"job_status": "config_error"}]
        with patch.object(hook, "get_cdi_job_sync_status", return_value=error_job):
            with pytest.raises(AirflowException, match="failed with status 'config_error'"):
                hook.wait_for_cdi_job(INTEGRATION_ID)

    def test_wait_timeout(self, hook):
        running = [{"job_status": "running"}]
        with patch.object(hook, "get_cdi_job_sync_status", return_value=running), patch(
            "braze_provider.hooks.braze.time.monotonic", side_effect=[0, 3601]
        ):
            with pytest.raises(AirflowException, match="timed out"):
                hook.wait_for_cdi_job(INTEGRATION_ID, poll_interval=1, timeout=3600)

    def test_wait_empty_results_raises(self, hook):
        with patch.object(hook, "get_cdi_job_sync_status", return_value=[]):
            with pytest.raises(AirflowException, match="No sync status results found"):
                hook.wait_for_cdi_job(INTEGRATION_ID)
