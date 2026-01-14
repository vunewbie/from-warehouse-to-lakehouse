from airflow import DAG

from generators.base import BaseGenerator
from datetime import timedelta, datetime
from models.elt import ELTModel
from builders.elt import ELTBuilder


class ELTGenerator(BaseGenerator):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

    def _build_general_dags_configs(self, raw_dict_configs):
        general_configs = raw_dict_configs.get("general", {})
        dags_configs = general_configs.get("dags_configs", {})
        source_configs = general_configs.get("source_configs", {})
        gcp_configs = general_configs.get("gcp_configs", {})
        default_table_configs = general_configs.get("default_table_configs", {})

        return {
            # DAGs configs
            "dag_type": dags_configs.get("dag_type"),
            "schedule_interval": dags_configs.get("schedule_interval"),
            "max_active_runs": int(dags_configs.get("max_active_runs")),
            "catchup": dags_configs.get("catchup"),
            "dagrun_timeout": timedelta(
                seconds=int(dags_configs.get("dagrun_timeout"))
            ),
            "owner": dags_configs.get("owner"),
            "retries": int(dags_configs.get("retries")),
            # Source configs
            "source_type": source_configs.get("source_type"),
            "source_conn_id": source_configs.get("source_conn_id"),
            "source_schema": source_configs.get("source_schema"),
            # GCP configs
            "gcp_conn_id": gcp_configs.get("gcp_conn_id"),
            "gcs_bucket_name": gcp_configs.get("gcs_bucket_name"),
            "gcp_project_id": gcp_configs.get("gcp_project_id"),
            "gcp_location": gcp_configs.get("gcp_location"),
            "dataproc_cluster_name": gcp_configs.get("dataproc_cluster_name"),
            # Default table configs
            "is_disabled": default_table_configs.get("is_disabled"),
            "look_back": timedelta(seconds=int(default_table_configs.get("look_back"))),
            "primary_keys": default_table_configs.get("primary_keys"),
            "clustered_by": default_table_configs.get("clustered_by"),
            "extract_conditions": default_table_configs.get("extract_conditions"),
            "start_date": datetime.strptime(
                default_table_configs.get("start_date"), "%Y%m%d"
            ),
        }

    def _build_entity_configs(self, general_dags_configs, entity_config):
        return {
            "is_disabled": self._get_with_default(
                "is_disabled", general_dags_configs, entity_config
            ),
            "look_back": self._get_with_default(
                "look_back", general_dags_configs, entity_config
            ),
            "table_name": self._get_with_default(
                "table_name", general_dags_configs, entity_config
            ),
            "primary_keys": self._get_with_default(
                "primary_keys", general_dags_configs, entity_config
            ),
            "clustered_by": self._get_with_default(
                "clustered_by", general_dags_configs, entity_config
            ),
            "extract_conditions": self._get_with_default(
                "extract_conditions", general_dags_configs, entity_config
            ),
            "start_date": self._get_with_default(
                "start_date", general_dags_configs, entity_config
            ),
        }

    def _gen_model(self, dag_configs):
        return ELTModel(
            **dag_configs,
        )

    def _gen_dag(self, model):
        """
        Generates a DAG instance for a specific table model, including the full pipeline.
        """
        builder = ELTBuilder(model)
        dag_id = builder.dag_id

        # Skip DAG generation if it's disabled in the config
        if model.is_disabled:
            return None, None

        with DAG(**builder.dag_parameters) as dag:
            # Get task objects from the builder
            source_to_landing_task = builder._get_source_to_landing_task()
            landing_to_bronze_task = builder._get_landing_to_bronze_task()
            bronze_to_silver_staging_task = builder._get_bronze_to_silver_staging_task()

            # Define the task dependency chain
            (
                source_to_landing_task
                >> landing_to_bronze_task
                >> bronze_to_silver_staging_task
            )

        return (dag_id, dag)
