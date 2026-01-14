# vunewbie-data-engineering/plugins/operators/landing_to_bronze.py

import json
from typing import Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.email import send_email
from airflow.models import Variable
from google.cloud.exceptions import NotFound


class LandingToBronzeOperator(BaseOperator):
    """
    Get Old Schema From BigQuery
    -> If not exists, create new one Else Get New Schema From GCS(.parquet metadata)
    -> Compare Schemas
    -> Send notification if changed/newly created
    """

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
        OPTIONS (
          format = 'PARQUET',
          uris = {uris_sql},
          hive_partition_uri_prefix = '{self.hive_partition_uri_prefix}'
        )
        """
        return ddl.strip()

    def _get_old_schema(self, hook):
        dataset_id, table_name = self.bronze_table_id.split(".")[-2:]
        try:
            schema_from_bq = hook.get_schema(
                dataset_id=dataset_id, table_id=table_name, project_id=self.project_id
            )
            old_schema = schema_from_bq.get("fields")
            self.log.info(f"Found existing schema for {self.bronze_table_id}")
            return old_schema
        except NotFound:
            self.log.info(
                f"Bronze table {self.bronze_table_id} does not exist. A new one will be created."
            )
            return None

    def _get_new_schema_from_gcs(self, hook):
        external_config = {
            "sourceUris": self.bronze_external_uris,
            "sourceFormat": "PARQUET",
            "autodetect": True,
            "hivePartitioningOptions": {
                "mode": "AUTO",
                "sourceUriPrefix": self.hive_partition_uri_prefix,
            },
        }
        job_config = {
            "query": {"query": "SELECT 1", "useLegacySql": False},
            "externalDataConfiguration": external_config,
            "dryRun": True,
        }
        job = hook.insert_job(configuration=job_config, project_id=self.project_id)
        self.log.info(f"Job: {job}")
        job_resource = job.to_api_repr()
        self.log.info(f"Job resource: {job_resource}")
        schema_fields = job.schema
        self.log.info(f"Job.schema: {schema_fields}")

        new_schema = [field.to_api_repr() for field in schema_fields]
        self.log.info(f"Inferred new schema from GCS for {self.bronze_table_id}")
        return new_schema

    def _send_schema_notification(self, old_schema, new_schema):
        notification_email = Variable.get("de_notification_email", default_var=None)
        if not notification_email:
            self.log.warning(
                "Airflow Variable 'de_notification_email' not set. Skipping notification."
            )
            return

        if old_schema is None:
            subject = f"✅ [Airflow] New Bronze Table Created: {self.bronze_table_id}"
            body = (
                f"A new Bronze external table has been created successfully."
                f"<br><br><b>Table ID:</b> {self.bronze_table_id}"
                f"<br><b>Schema:</b><br><pre>{json.dumps(new_schema, indent=2)}</pre>"
            )
            send_email(to=[notification_email], subject=subject, html_content=body)
            self.log.info(
                f"Sent notification for new table creation to {notification_email}"
            )
        else:
            old_schema_set = {(d.get("name"), d.get("type")) for d in old_schema}
            new_schema_set = {(d.get("name"), d.get("type")) for d in new_schema}

            if old_schema_set != new_schema_set:
                subject = f"⚠️ [Airflow] Schema Change Detected for Bronze Table: {self.bronze_table_id}"
                body = (
                    f"Schema has changed...<br><b>Old:</b><br>"
                    f"<pre>{json.dumps(old_schema, indent=2)}</pre>"
                    f"<br><b>New:</b><br>"
                    f"<pre>{json.dumps(new_schema, indent=2)}</pre>"
                )
                send_email(to=[notification_email], subject=subject, html_content=body)
                self.log.info(
                    f"Schema changed. Sent notification to {notification_email}"
                )
            else:
                self.log.info("Schema is unchanged. No notification sent.")

    def execute(self, context):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, location=self.location)

        old_schema = self._get_old_schema(hook)
        new_schema = self._get_new_schema_from_gcs(hook)

        ddl = self._build_create_external_table_ddl()
        self.log.info("Executing DDL to create/replace external table...")
        hook.run_query(sql=ddl, use_legacy_sql=False)
        self.log.info(
            f"External table {self.bronze_table_id} created/replaced successfully."
        )

        self._send_schema_notification(old_schema, new_schema)
