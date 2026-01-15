from typing import Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class SourceToLandingOperator(BaseOperator):
    """
    Extract data from a JDBC source (MySQL/Postgres) and load it into GCS
    as Parquet by submitting a PySpark job to an existing Dataproc cluster.
    """

    template_fields: Sequence[str] = ("gcs_output_path",)

    def __init__(
        self,
        gcp_conn_id,
        project_id,
        region,
        cluster_name,
        source_type,
        source_conn_id,
        jdbc_schema,
        table_name,
        gcs_bucket_name,
        dag_type,
        main_python_file_uri,
        extract_conditions,
        dag_id,
        gcs_output_path,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.source_type = source_type
        self.source_conn_id = source_conn_id
        self.jdbc_schema = jdbc_schema
        self.table_name = table_name
        self.gcs_bucket_name = gcs_bucket_name
        self.dag_type = dag_type
        self.main_python_file_uri = main_python_file_uri
        self.extract_conditions = extract_conditions
        self._dag_id = dag_id  # Store as private attribute to avoid conflict with BaseOperator.dag_id property
        self.gcs_output_path = gcs_output_path

    def _build_jar_file_uris(self):
        if self.source_type == "mysql":
            jar_file = "mysql-connector-j-8.0.33.jar"
        elif self.source_type == "postgres":
            jar_file = "postgresql-42.7.3.jar"
        else:
            raise NotImplementedError(
                f"Source type '{self.source_type}' is not implemented."
            )

        return [f"gs://{self.gcs_bucket_name}/pyspark/packages/{jar_file}"]

    def _build_python_file_uris(self):
        return [f"gs://{self.gcs_bucket_name}/pyspark/{self.dag_type}/libs/aw.zip"]

    def _build_extract_query(self):
        select_statement = "SELECT *\n"
        from_statement = f"FROM {self.jdbc_schema}.{self.table_name}\n"

        is_extracted_full = (
            self.extract_conditions is None or len(self.extract_conditions) == 0
        )

        if is_extracted_full:
            return select_statement + from_statement

        conditions = []
        for condition in self.extract_conditions:
            key, operator, value = condition

            if value == "last_extracted_variable_key":
                dag_id_value = self.dag.dag_id if self.dag else self._dag_id
                replaced_value = Variable.get(
                    f"{dag_id_value}__last_extraction", default_var=None
                )
                if replaced_value is None or replaced_value == "":
                    replaced_value = "1970-01-01 00:00:00"
                value = f"'{replaced_value}'"

            conditions.append(f"{key} {operator} {value}")

        where_statement = f"WHERE {' AND '.join(conditions)}"
        return select_statement + from_statement + where_statement

    def _get_jdbc_connection_info(self):
        if self.source_type == "mysql":
            hook = MySqlHook(mysql_conn_id=self.source_conn_id)
            jdbc_driver = "com.mysql.cj.jdbc.Driver"
        elif self.source_type == "postgres":
            hook = PostgresHook(postgres_conn_id=self.source_conn_id)
            jdbc_driver = "org.postgresql.Driver"
        else:
            raise ValueError(f"Unsupported source_type: {self.source_type}")

        conn = hook.get_connection(self.source_conn_id)
        port = conn.port
        database = conn.schema
        user = conn.login
        password = conn.password

        if self.source_type == "mysql":
            jdbc_url = f"jdbc:mysql://{conn.host}:{port}/{database}"
        else:
            jdbc_url = f"jdbc:postgresql://{conn.host}:{port}/{database}"

        return {
            "jdbc_driver": jdbc_driver,
            "jdbc_url": jdbc_url,
            "jdbc_user": user,
            "jdbc_password": password,
        }

    def _build_dataproc_job(self, query, jdbc_info):
        return {
            "placement": {"cluster_name": self.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": self.main_python_file_uri,
                "python_file_uris": self._build_python_file_uris(),
                "jar_file_uris": self._build_jar_file_uris(),
                "args": [
                    "--jdbc_url",
                    jdbc_info["jdbc_url"],
                    "--jdbc_driver",
                    jdbc_info["jdbc_driver"],
                    "--jdbc_user",
                    jdbc_info["jdbc_user"],
                    "--jdbc_password",
                    jdbc_info["jdbc_password"],
                    "--jdbc_schema",
                    self.jdbc_schema,
                    "--table_name",
                    self.table_name,
                    "--query",
                    query,
                    "--output_path",
                    self.gcs_output_path,
                    "--source_type",
                    self.source_type,
                ],
            },
        }

    def execute(self, context):
        self.log.info(
            f"Starting {self.source_type} extract for table {self.jdbc_schema}.{self.table_name}"
        )
        self.log.info(f"Cluster name: {self.cluster_name}")
        self.log.info(f"Rendered output path: {self.gcs_output_path}")

        jar_file_uris = self._build_jar_file_uris()
        python_file_uris = self._build_python_file_uris()
        query = self._build_extract_query()

        self.log.info(f"JAR file URIs: {jar_file_uris}")
        self.log.info(f"Python file URIs: {python_file_uris}")
        self.log.info(f"Built query: {query}")

        jdbc_info = self._get_jdbc_connection_info()
        job = self._build_dataproc_job(query=query, jdbc_info=jdbc_info)
        dataproc_hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)

        self.log.info("Submitting Dataproc job to existing cluster...")
        
        submitted_job = dataproc_hook.submit_job(
            project_id=self.project_id,
            region=self.region,
            job=job,
        )
        job_id = submitted_job.reference.job_id

        self.log.info(f"Job submitted successfully. Job ID: {job_id}")
        self.log.info("Waiting for Dataproc job to complete...")

        dataproc_hook.wait_for_job(
            job_id=job_id, project_id=self.project_id, region=self.region, timeout=3600
        )

        self.log.info("Dataproc job completed successfully.")
