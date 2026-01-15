import json
from typing import Sequence, Optional, List, Dict

from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.email import send_email

from helpers.utils import fetch_information_schema_columns


class LandingToBronzeOperator(BaseOperator):
    """Create or replace Bronze external table in BigQuery from Parquet files on GCS."""

    template_fields: Sequence[str] = (
        "bronze_table_id",
        "bronze_external_uris",
        "hive_partition_uri_prefix",
    )

    def __init__(
        self,
        gcp_conn_id,
        project_id,
        location,
        bronze_table_id,
        bronze_external_uris,
        hive_partition_uri_prefix,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.location = location
        self.bronze_table_id = bronze_table_id
        self.bronze_external_uris = bronze_external_uris
        self.hive_partition_uri_prefix = hive_partition_uri_prefix

    def _format_bq_string_array(self, values):
        escaped = [v.replace("'", "''") for v in values]
        return "[" + ", ".join(f"'{v}'" for v in escaped) + "]"

    def _build_create_external_table_ddl(self):
        uris_sql = self._format_bq_string_array(self.bronze_external_uris)
        ddl = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{self.bronze_table_id}`
        WITH PARTITION COLUMNS (
          date DATE
        )
        OPTIONS (
          format = 'PARQUET',
          uris = {uris_sql},
          hive_partition_uri_prefix = '{self.hive_partition_uri_prefix}'
        )
        """
        return ddl.strip()

    def _parse_table_id(self):
        parts = self.bronze_table_id.split(".")
        if len(parts) >= 2:
            dataset_id = parts[-2]
            table_name = parts[-1]
            return dataset_id, table_name
        raise ValueError(f"Invalid bronze_table_id format: {self.bronze_table_id}")

    def _get_schema_from_information_schema(self, hook):
        dataset_id, table_name = self._parse_table_id()
        self.log.info(
            f"Querying INFORMATION_SCHEMA for table {self.bronze_table_id}..."
        )
        try:
            schema = fetch_information_schema_columns(
                hook=hook,
                project_id=self.project_id,
                dataset_id=dataset_id,
                table_name=table_name,
                select_mode="full",
            )
            if not schema:
                self.log.info(
                    f"Table {self.bronze_table_id} does not exist in INFORMATION_SCHEMA."
                )
                return None
            self.log.info(f"Retrieved {len(schema)} columns from INFORMATION_SCHEMA.")
            return schema
        except Exception as e:
            self.log.warning(f"Error querying INFORMATION_SCHEMA: {e}")
            return None

    def _get_old_schema(self, hook):
        """Get old schema before CREATE OR REPLACE."""
        return self._get_schema_from_information_schema(hook)

    def _get_new_schema(self, hook):
        """Get new schema after CREATE OR REPLACE."""
        schema = self._get_schema_from_information_schema(hook)
        if schema is None:
            raise ValueError(
                f"Could not retrieve schema for {self.bronze_table_id} after CREATE OR REPLACE."
            )
        return schema

    def _compare_schemas(self, old_schema, new_schema):
        """Compare old and new schemas to detect changes."""
        if old_schema is None:
            return {
                "is_new_table": True,
                "has_changes": True,
                "added_columns": [col["column_name"] for col in new_schema],
                "removed_columns": [],
                "modified_columns": [],
            }

        old_cols = {col["column_name"]: col for col in old_schema}
        new_cols = {col["column_name"]: col for col in new_schema}

        added_columns = [name for name in new_cols if name not in old_cols]
        removed_columns = [name for name in old_cols if name not in new_cols]
        modified_columns = []

        for col_name in old_cols.keys() & new_cols.keys():
            old_col = old_cols[col_name]
            new_col = new_cols[col_name]
            if (
                old_col["data_type"] != new_col["data_type"]
                or old_col["is_nullable"] != new_col["is_nullable"]
            ):
                modified_columns.append(col_name)

        has_changes = bool(added_columns or removed_columns or modified_columns)

        return {
            "is_new_table": False,
            "has_changes": has_changes,
            "added_columns": added_columns,
            "removed_columns": removed_columns,
            "modified_columns": modified_columns,
        }

    def _send_schema_notification(self, comparison_result, context):
        """Send email notification if schema has changed."""
        notification_email = Variable.get("de_notification_email", default_var=None)
        if not notification_email:
            self.log.info(
                "No notification email configured. Skipping schema notification."
            )
            return

        if not comparison_result["has_changes"]:
            self.log.info("No schema changes detected. Skipping notification.")
            return

        dag_id = context.get("dag").dag_id
        task_id = self.task_id
        execution_date = context.get("execution_date")

        if comparison_result["is_new_table"]:
            subject = f"🆕 [Airflow] New Bronze Table Created: {self.bronze_table_id}"
            body = (
                f"<h3>New Bronze External Table Created</h3>"
                f"<p><b>Table:</b> {self.bronze_table_id}</p>"
                f"<p><b>DAG:</b> {dag_id}</p>"
                f"<p><b>Task:</b> {task_id}</p>"
                f"<p><b>Execution Date:</b> {execution_date}</p>"
                f"<h4>Schema:</h4>"
                f"<pre>{json.dumps(comparison_result['added_columns'], indent=2)}</pre>"
            )
        else:
            subject = f"⚠️ [Airflow] Schema Changed: {self.bronze_table_id}"
            changes = []
            if comparison_result["added_columns"]:
                changes.append(
                    f"<li><b>Added:</b> {', '.join(comparison_result['added_columns'])}</li>"
                )
            if comparison_result["removed_columns"]:
                changes.append(
                    f"<li><b>Removed:</b> {', '.join(comparison_result['removed_columns'])}</li>"
                )
            if comparison_result["modified_columns"]:
                changes.append(
                    f"<li><b>Modified:</b> {', '.join(comparison_result['modified_columns'])}</li>"
                )
            body = (
                f"<h3>Bronze Table Schema Changed</h3>"
                f"<p><b>Table:</b> {self.bronze_table_id}</p>"
                f"<p><b>DAG:</b> {dag_id}</p>"
                f"<p><b>Task:</b> {task_id}</p>"
                f"<p><b>Execution Date:</b> {execution_date}</p>"
                f"<h4>Changes:</h4>"
                f"<ul>{''.join(changes)}</ul>"
            )

        try:
            send_email(to=[notification_email], subject=subject, html_content=body)
            self.log.info(f"Schema notification email sent to {notification_email}.")
        except Exception as e:
            self.log.error(f"Failed to send schema notification email: {e}")

    def execute(self, context):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, location=self.location)

        self.log.info("Retrieving old schema from INFORMATION_SCHEMA...")
        old_schema = self._get_old_schema(hook)

        ddl = self._build_create_external_table_ddl()
        self.log.info("Step 2: Executing DDL to create/replace external table...")
        hook.insert_job(
            configuration={
                "query": {
                    "query": ddl,
                    "useLegacySql": False,
                }
            },
            project_id=self.project_id,
        )
        self.log.info(
            f"External table {self.bronze_table_id} created/replaced successfully."
        )

        self.log.info("Retrieving new schema from INFORMATION_SCHEMA...")
        new_schema = self._get_new_schema(hook)

        self.log.info("Comparing schemas...")
        comparison_result = self._compare_schemas(old_schema, new_schema)
        self.log.info(
            f"Schema comparison result:\n{json.dumps(comparison_result, indent=2)}"
        )

        self.log.info(
            f"Current schema for {self.bronze_table_id}:\n{json.dumps(new_schema, indent=2)}"
        )

        self._send_schema_notification(comparison_result, context)
