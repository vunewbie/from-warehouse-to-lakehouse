# vunewbie-data-engineering/plugins/operators/bronze_to_silver_staging.py

from typing import Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.exceptions import NotFound


class BronzeToSilverStagingOperator(BaseOperator):
    """Upsert data from a Bronze external table to a Silver Staging Iceberg table.
    Creates the table if it doesn't exist.
    """

    template_fields: Sequence[str] = ("bronze_table_id", "silver_staging_table_id")

    def __init__(
        self,
        gcp_conn_id,
        location,
        project_id,
        bronze_table_id,
        silver_staging_table_id,
        silver_staging_gcs_uri,
        primary_keys,
        cluster_keys,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.project_id = project_id
        self.bronze_table_id = bronze_table_id
        self.silver_staging_table_id = silver_staging_table_id
        self.silver_staging_gcs_uri = silver_staging_gcs_uri
        self.primary_keys = primary_keys
        self.cluster_keys = cluster_keys or primary_keys

    def _get_bronze_schema(self, hook):
        """Retrieves schema from the Bronze table."""
        bronze_dataset_id, bronze_table_name = self.bronze_table_id.split(".")[-2:]
        bronze_schema = hook.get_schema(
            dataset_id=bronze_dataset_id,
            table_id=bronze_table_name,
            project_id=self.project_id,
        )
        return bronze_schema

    def _build_create_iceberg_table_ddl(self, bronze_schema):
        """Builds the CREATE TABLE DDL statement for Iceberg table."""
        columns_ddl = ",\n".join(
            [f"`{field['name']}` {field['type']}" for field in bronze_schema["fields"]]
        )

        create_ddl = f"""
        CREATE TABLE `{self.silver_staging_table_id}` (
            {columns_ddl}
        )
        PARTITION BY DATE(_extract_date_)
        CLUSTER BY {", ".join(self.cluster_keys)}
        OPTIONS (
            table_format = 'ICEBERG',
            table_uri = '{self.silver_staging_gcs_uri}'
        );
        """
        return create_ddl.strip()

    def _create_silver_staging_table_if_not_exists(self, hook):
        """Creates the Silver Staging Iceberg table if it doesn't exist."""
        silver_dataset_id, silver_table_name = self.silver_staging_table_id.split(".")[
            -2:
        ]

        try:
            hook.get_schema(
                dataset_id=silver_dataset_id,
                table_id=silver_table_name,
                project_id=self.project_id,
            )
            self.log.info(
                f"Silver Staging table {self.silver_staging_table_id} already exists."
            )
        except NotFound:
            self.log.info(
                f"Silver Staging table {self.silver_staging_table_id} does not exist. Creating it now..."
            )

            bronze_schema = self._get_bronze_schema(hook)
            create_ddl = self._build_create_iceberg_table_ddl(bronze_schema)

            self.log.info("Executing CREATE TABLE DDL for Iceberg table...")
            hook.insert_job(
                configuration={
                    "query": {
                        "query": create_ddl,
                        "useLegacySql": False,
                    }
                },
                project_id=self.project_id,
            )
            self.log.info(
                f"Successfully created Iceberg table: {self.silver_staging_table_id}"
            )

    def _build_merge_sql(self, bronze_schema):
        """Builds the MERGE SQL statement to deduplicate and upsert data with date_list optimization."""
        column_names = [field["name"] for field in bronze_schema["fields"]]

        update_columns = [col for col in column_names if col not in self.primary_keys]
        update_set_clause = ", ".join(
            [f"T.`{col}` = S.`{col}`" for col in update_columns]
        )
        all_columns_string = ", ".join([f"`{col}`" for col in column_names])
        source_columns_string = ", ".join([f"S.`{col}`" for col in column_names])
        merge_join_condition = " AND ".join(
            [f"T.`{pk}` = S.`{pk}`" for pk in self.primary_keys]
        )

        # Build extract condition: filter data from last 2 hours to handle late-arriving data
        extract_condition_stm = (
            "_extract_date_ >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)"
        )

        # Build date_list optimization if cluster_keys exist
        filter_date_stm = ""
        if self.cluster_keys and len(self.cluster_keys) > 0:
            # Use first cluster key for optimization (most impactful)
            first_cluster_key = self.cluster_keys[0]
            filter_date_stm = f"""
                DECLARE date_list ARRAY<DATE>;
                SET date_list = (
                    SELECT ARRAY_AGG(DISTINCT CAST(`{first_cluster_key}` AS DATE))
                    FROM `{self.bronze_table_id}`
                    WHERE {extract_condition_stm}
                );
            """
            # Add date filter to merge join condition for performance optimization
            merge_join_condition += (
                f" AND CAST(T.`{first_cluster_key}` AS DATE) IN UNNEST(date_list)"
            )

        # Assemble MERGE SQL with optional BEGIN/END block if date_list is used
        if filter_date_stm:
            merge_sql = f"""
            BEGIN
            {filter_date_stm}

            MERGE `{self.silver_staging_table_id}` AS T
            USING (
                SELECT *
                FROM `{self.bronze_table_id}`
                WHERE {extract_condition_stm}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY {", ".join(self.primary_keys)}
                    ORDER BY _extract_date_ DESC
                ) = 1
            ) AS S
            ON {merge_join_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({all_columns_string}) VALUES ({source_columns_string});

            END;
            """
        else:
            # No date_list optimization, simpler MERGE without BEGIN/END
            merge_sql = f"""
            MERGE `{self.silver_staging_table_id}` AS T
            USING (
                SELECT *
                FROM `{self.bronze_table_id}`
                WHERE {extract_condition_stm}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY {", ".join(self.primary_keys)}
                    ORDER BY _extract_date_ DESC
                ) = 1
            ) AS S
            ON {merge_join_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({all_columns_string}) VALUES ({source_columns_string})
            """

        return merge_sql.strip()

    def _execute_merge(self, hook):
        """Executes the MERGE statement to deduplicate and upsert data."""
        bronze_schema = self._get_bronze_schema(hook)
        merge_sql = self._build_merge_sql(bronze_schema)

        self.log.info("Executing MERGE statement into Silver Staging table...")
        hook.insert_job(
            configuration={
                "query": {
                    "query": merge_sql,
                    "useLegacySql": False,
                }
            },
            project_id=self.project_id,
        )
        self.log.info("MERGE operation completed successfully.")

    def execute(self, context):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, location=self.location)

        self._create_silver_staging_table_if_not_exists(hook)
        self._execute_merge(hook)
