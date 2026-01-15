from typing import List, Dict

from airflow.utils.email import send_email
from airflow.models import Variable


def task_failure_alert_email(context):
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


def fetch_information_schema_columns(
    hook,
    project_id,
    dataset_id,
    table_name,
    select_mode="full",
    table_schema_filter=None,
):
    if select_mode == "full":
        select_clause = """
          table_catalog,
          table_schema,
          table_name,
          column_name,
          data_type,
          is_nullable
        """.strip()
    elif select_mode == "ddl":
        select_clause = """
          column_name,
          data_type
        """.strip()
    else:
        raise ValueError(f"Unsupported select_mode: {select_mode}")

    schema_filter_sql = ""
    if table_schema_filter:
        schema_filter_sql = f" AND table_schema = '{table_schema_filter}'"

    query = f"""
    SELECT
      {select_clause}
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
      {schema_filter_sql}
    ORDER BY ordinal_position
    """.strip()

    job = hook.insert_job(
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
    )
    job.result()

    rows: List[Dict] = []
    try:
        df = job.to_dataframe()
        if df.empty:
            return None
        rows = df.to_dict("records")
    except (AttributeError, ImportError):
        for row in job:
            if select_mode == "full":
                rows.append(
                    {
                        "table_catalog": row.get("table_catalog"),
                        "table_schema": row.get("table_schema"),
                        "table_name": row.get("table_name"),
                        "column_name": row.get("column_name"),
                        "data_type": row.get("data_type"),
                        "is_nullable": row.get("is_nullable"),
                    }
                )
            else:
                rows.append(
                    {
                        "column_name": row.get("column_name"),
                        "data_type": row.get("data_type"),
                    }
                )

    return rows if rows else None
