import attr
from typing import Callable
from helpers.utils import task_failure_alert_email


@attr.s
class GeneralDagsConfigs:
    dag_type: str = attr.ib(kw_only=True)
    schedule_interval: str = attr.ib(kw_only=True)
    max_active_runs: int = attr.ib(kw_only=True)
    catchup: bool = attr.ib(kw_only=True)
    dagrun_timeout: int = attr.ib(kw_only=True)
    owner: str = attr.ib(kw_only=True)
    retries: int = attr.ib(kw_only=True)
    on_failure_callback: Callable = attr.ib(
        kw_only=True, default=task_failure_alert_email
    )


@attr.s
class GCPConfigs:
    gcp_conn_id: str = attr.ib(kw_only=True)
    gcs_bucket_name: str = attr.ib(kw_only=True)
    gcp_project_id: str = attr.ib(kw_only=True)
    gcp_location: str = attr.ib(kw_only=True)
    dataproc_cluster_name: str = attr.ib(kw_only=True)
