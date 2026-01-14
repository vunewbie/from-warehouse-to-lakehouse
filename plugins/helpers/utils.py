"""Utility functions for Airflow DAGs."""

from airflow.utils.email import send_email
from airflow.models import Variable


def task_failure_alert_email(context):
    """
    Callback function to send email notification when a task fails.

    Args:
        context: Airflow context dictionary containing task instance information
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")

    notification_email = Variable.get("de_notification_email", default_var=None)
    if not notification_email:
        return

    subject = f"❌ [Airflow] Task Failed: {dag_id}.{task_id}"
    body = (
        f"Task <b>{task_id}</b> in DAG <b>{dag_id}</b> has failed.<br><br>"
        f"<b>Execution Date:</b> {execution_date}<br>"
        f"<b>Log URL:</b> {task_instance.log_url}<br>"
    )

    send_email(to=[notification_email], subject=subject, html_content=body)
