from airflow.utils.log.logging_mixin import LoggingMixin


class BaseBuilder(LoggingMixin):
    def __init__(self, model, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = model

    @property
    def is_extracted_full(self):
        return (
            self.model.extract_conditions is None
            or len(self.model.extract_conditions) == 0
        )

    @property
    def gcp_conn_id(self):
        return self.model.gcp_conn_id

    @property
    def gcs_bucket_name(self):
        return self.model.gcs_bucket_name

    @property
    def gcp_project_id(self):
        return self.model.gcp_project_id

    @property
    def gcp_location(self):
        return self.model.gcp_location

    def _get_source_to_landing_task(self):
        raise NotImplementedError("Not implement for this dag type.")

    def _get_landing_to_bronze_task(self):
        raise NotImplementedError("Not implement for this dag type.")

    def _get_bronze_to_silver_staging_task(self):
        raise NotImplementedError("Not implement for this dag type.")

    def _set_last_extracted_variable(self):
        pass

    def build_dag(self):
        pass
