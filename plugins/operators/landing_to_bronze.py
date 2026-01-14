# vunewbie-data-engineering/plugins/operators/landing_to_bronze.py

import json
from typing import Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


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
        OPTIONS (
          format = 'PARQUET',
          uris = {uris_sql},
          hive_partition_uri_prefix = '{self.hive_partition_uri_prefix}'
        )
        """
        return ddl.strip()

    def execute(self, context):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, location=self.location)

        ddl = self._build_create_external_table_ddl()
        self.log.info("Executing DDL to create/replace external table...")
        hook.run_query(sql=ddl, use_legacy_sql=False)
        self.log.info(
            f"External table {self.bronze_table_id} created/replaced successfully."
        )

        dataset_id, table_name = self.bronze_table_id.split(".")[-2:]
        schema_from_bq = hook.get_schema(
            dataset_id=dataset_id, table_id=table_name, project_id=self.project_id
        )
        current_schema = schema_from_bq.get("fields")
        self.log.info(
            f"Current schema for {self.bronze_table_id}:\n{json.dumps(current_schema, indent=2)}"
        )
