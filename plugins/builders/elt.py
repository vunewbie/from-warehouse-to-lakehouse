from operators.source_to_landing import SourceToLandingOperator
from operators.bronze_to_silver_staging import BronzeToSilverStagingOperator
from operators.landing_to_bronze import LandingToBronzeOperator

from builders.base import BaseBuilder

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class ELTBuilder(BaseBuilder):
    # General configs
    @property
    def dag_id(self):
        if self.model.source_type == "mysql":
            return "__".join(
                [
                    self.model.dag_type,
                    self.model.source_database,
                    self.model.table_name,
                ]
            )
        elif self.model.source_type == "postgres":
            return "__".join(
                [
                    self.model.dag_type,
                    self.model.source_schema,
                    self.model.table_name,
                ]
            )
        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

    @property
    def dag_tags(self):
        if self.model.source_type == "mysql":
            return [
                self.model.dag_type,
                self.model.source_database,
                self.model.table_name,
                self.model.source_type,
            ]
        elif self.model.source_type == "postgres":
            return [
                self.model.dag_type,
                self.model.source_schema,
                self.model.table_name,
                self.model.source_type,
            ]
        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

    @property
    def dag_parameters(self):
        return {
            "dag_id": self.dag_id,
            "schedule_interval": self.model.schedule_interval,
            "max_active_runs": self.model.max_active_runs,
            "catchup": self.model.catchup,
            "dagrun_timeout": self.model.dagrun_timeout,
            "default_args": {
                "owner": self.model.owner,
                "retries": self.model.retries,
                "start_date": self.model.start_date,
                "on_failure_callback": self.model.on_failure_callback,
            },
            "tags": self.dag_tags,
        }

    # Source to Landing
    @property
    def gcs_landing_file_name(self):
        if self.model.source_type == "mysql":
            base_path = f"data/{self.model.dag_type}/{self.model.source_database}/{self.model.table_name}"

        elif self.model.source_type == "postgres":
            base_path = f"data/{self.model.dag_type}/{self.model.source_schema}/{self.model.table_name}"

        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

        return base_path + "/date={{ data_interval_start.format('YYYY-MM-DD') }}"

    @property
    def source_to_landing_job_file_uri(self):
        return f"gs://{self.model.gcs_bucket_name}/pyspark/{self.model.dag_type}/workflows/source_to_landing.py"

    def _get_source_to_landing_task(self):
        if self.model.source_type == "mysql":
            jdbc_schema_or_database = self.model.source_database

        elif self.model.source_type == "postgres":
            jdbc_schema_or_database = self.model.source_schema
        
        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

        return SourceToLandingOperator(
            task_id="source_to_landing",
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.gcp_project_id,
            region=self.gcp_location,
            cluster_name=self.model.dataproc_cluster_name,
            source_type=self.model.source_type,
            source_conn_id=self.model.source_conn_id,
            jdbc_schema_or_database=jdbc_schema_or_database,
            table_name=self.model.table_name,
            gcs_bucket_name=self.gcs_bucket_name,
            dag_type=self.model.dag_type,
            main_python_file_uri=self.source_to_landing_job_file_uri,
            extract_conditions=self.model.extract_conditions,
            dag_id=self.dag_id,
            gcs_output_path=f"gs://{self.gcs_bucket_name}/{self.gcs_landing_file_name}",
        )

    # Landing to Bronze
    @property
    def bronze_table_id(self):
        if self.model.source_type == "mysql":
            return f"{self.gcp_project_id}.bronze.{self.model.source_database}__{self.model.table_name}"

        elif self.model.source_type == "postgres":
            return f"{self.gcp_project_id}.bronze.{self.model.source_schema}__{self.model.table_name}"

        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

    @property
    def bronze_external_uris(self):
        if self.model.source_type == "mysql":
            return [
                f"gs://{self.gcs_bucket_name}/data/{self.model.dag_type}/"
                f"{self.model.source_database}/{self.model.table_name}/*.parquet"
            ]
        elif self.model.source_type == "postgres":
            return [
                f"gs://{self.gcs_bucket_name}/data/{self.model.dag_type}/"
                f"{self.model.source_schema}/{self.model.table_name}/*.parquet"
            ]
        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

    @property
    def hive_partition_uri_prefix(self):
        if self.model.source_type == "mysql":
            return (
                f"gs://{self.gcs_bucket_name}/data/{self.model.dag_type}/"
                f"{self.model.source_database}/{self.model.table_name}/"
            )
        elif self.model.source_type == "postgres":
            return (
                f"gs://{self.gcs_bucket_name}/data/{self.model.dag_type}/"
                f"{self.model.source_schema}/{self.model.table_name}/"
            )
        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

    def _get_landing_to_bronze_task(self):
        return LandingToBronzeOperator(
            task_id="landing_to_bronze",
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.gcp_project_id,
            location=self.gcp_location,
            bronze_table_id=self.bronze_table_id,
            bronze_external_uris=self.bronze_external_uris,
            hive_partition_uri_prefix=self.hive_partition_uri_prefix,
        )

    # Bronze to Silver staging
    @property
    def silver_staging_table_id(self):
        if self.model.source_type == "mysql":
            return f"{self.gcp_project_id}.silver_staging.{self.model.source_database}__{self.model.table_name}"

        elif self.model.source_type == "postgres":
            return f"{self.gcp_project_id}.silver_staging.{self.model.source_schema}__{self.model.table_name}"

        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

    @property
    def silver_staging_gcs_uri(self):
        if self.model.source_type == "mysql":
            return f"gs://{self.gcs_bucket_name}/silver_staging/{self.model.source_database}/{self.model.table_name}"

        elif self.model.source_type == "postgres":
            return f"gs://{self.gcs_bucket_name}/silver_staging/{self.model.source_schema}/{self.model.table_name}"

        else:
            raise ValueError(f"Unsupported source_type: {self.model.source_type}")

    def _get_bronze_to_silver_staging_task(self):
        return BronzeToSilverStagingOperator(
            task_id="bronze_to_silver_staging",
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.gcp_project_id,
            location=self.gcp_location,
            bronze_table_id=self.bronze_table_id,
            silver_staging_table_id=self.silver_staging_table_id,
            silver_staging_gcs_uri=self.silver_staging_gcs_uri,
            primary_keys=self.model.primary_keys,
            cluster_keys=self.model.clustered_by,
        )

    def _set_last_extracted_variable(self):
        if self.is_extracted_full:
            return None

        if not self.model.clustered_by or len(self.model.clustered_by) == 0:
            raise ValueError(
                f"clustered_by must be provided to compute watermark for {self.dag_id}"
            )

        # Use the max() of clustered_by fields as requested
        watermark_column = max(self.model.clustered_by)

        def _set_var(**context):
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id, location=self.gcp_location
            )

            query = f"""
            SELECT
              FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',
                TIMESTAMP_SUB(CAST(MAX(`{watermark_column}`) AS TIMESTAMP), INTERVAL 1 DAY)
              ) AS watermark
            FROM `{self.silver_staging_table_id}`
            """.strip()

            job = hook.insert_job(
                configuration={
                    "query": {
                        "query": query,
                        "useLegacySql": False,
                    }
                },
                project_id=self.gcp_project_id,
            )
            job.result()

            watermark_value = None
            for row in job:
                watermark_value = row.get("watermark")
                break

            if not watermark_value:
                self.log.warning(
                    f"No watermark could be computed from {self.silver_staging_table_id}. Skipping last_extraction update."
                )
                return

            dag_id = context["dag"].dag_id
            Variable.set(f"{dag_id}__last_extraction", watermark_value)
            self.log.info(
                f"Set {dag_id}__last_extraction = {watermark_value} (MAX({watermark_column}) - 1 day from {self.silver_staging_table_id})"
            )

        return PythonOperator(
            task_id="set_last_extracted_variable",
            python_callable=_set_var,
        )
