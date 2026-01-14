from airflow.utils.log.logging_mixin import LoggingMixin
from setproctitle import getproctitle
import json
import yaml
import os
import sys


class BaseGenerator(LoggingMixin):
    def __init__(
        self,
        base_config_path,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.base_config_path = base_config_path

    def _load_config_paths(self):
        files_names = os.listdir(self.base_config_path)
        configs_paths = [
            os.path.join(self.base_config_path, file_name)
            for file_name in files_names
            if file_name.endswith(".yaml") or file_name.endswith(".yml")
        ]

        return configs_paths

    def _load_config_file(self, configs_path):
        with open(configs_path, "r") as file_config:
            return yaml.safe_load(file_config)

    def _build_general_dags_configs(self, raw_dict_configs):
        raise NotImplementedError("Not implement for this dag type.")

    def _get_with_default(self, key, general_dags_configs, entity_config):
        if entity_config.get(key):
            return entity_config.get(key)
        return general_dags_configs.get(key)

    def _build_entity_configs(self, general_dags_configs, entity_config):
        raise NotImplementedError("Not implement for this dag type.")

    def _build_dag_configs(self, general_dags_configs, entity_dag_configs):
        return {
            **general_dags_configs,
            **entity_dag_configs,
        }

    def _gen_model(self, dag_configs):
        raise NotImplementedError("Not implement for this dag type.")

    def _gen_dag(self, model):
        raise NotImplementedError("Not implement for this dag type.")

    def _get_airflow_params(self):
        """
        When Airflow runs on a task executor, it changes the process title to contain the entire command line.
        Ex: airflow task supervisor:
            ['/bin/airflow', 'tasks', 'run', 'elt__human_resources__department', 'extract_data', '--interactive']
            1. Notice "airflow" and "task supervisor" in the title.
            2. Remove the prefix:
                ['/bin/airflow', 'tasks', 'run', 'elt__human_resources__department', 'extract_data', '--interactive']
            3. Replace ' with " to be a valid JSON:
                ["/bin/airflow", "tasks", "run", "elt__human_resources__department", "extract_data", "--interactive"]
            4. json.loads(...) converts the string to a List argv:
                ["/bin/airflow", "tasks", "run", "elt__human_resources__department", "extract_data", "--interactive"]
            5. Check the index:
                argv[1] = "tasks"
                argv[2] = "run"
                argv[3] = "elt__human_resources__department"
        """
        subcommand = None
        dag_id = None
        argv = sys.argv
        proctitle = getproctitle()

        if "airflow" in proctitle and "task supervisor" in proctitle:
            try:
                argv_json = proctitle.replace("airflow task supervisor: ", "").replace(
                    "'", '"'
                )
                argv = json.loads(argv_json)
            except json.JSONDecodeError:
                self.log.warning("Could not parse process title to get DAG ID.")

        if len(argv) >= 4 and "airflow" in argv[0]:
            if argv[1] == "tasks" and argv[2] == "run":
                subcommand = "run"
                dag_id = argv[3]

        if dag_id:
            return {
                "subcommmand": subcommand,
                "dag_id": dag_id,
            }

        return {}

    def load_dags(self, __globals__):
        configs_paths = (
            self._load_config_paths()
        )  # Load all config file paths to a list
        dag_tuples = []

        for configs_path in configs_paths:
            raw_dict_configs = self._load_config_file(
                configs_path
            )  # Configs in each file is loaded to a nested dictionary
            general_dags_configs = self._build_general_dags_configs(
                raw_dict_configs
            )  # Flatten general configs
            entities_configs = raw_dict_configs.get(
                "entities", []
            )  # Get entities(tables/collections) configs from the file

            for entity_config in entities_configs:
                entity_dag_configs = self._build_entity_configs(
                    general_dags_configs, entity_config
                )  # Replace default configs with entity own configs
                dag_configs = self._build_dag_configs(
                    general_dags_configs, entity_dag_configs
                )  # Merge general and entity configs
                model = self._gen_model(
                    dag_configs
                )  # Generate model to prepare for DAG generation
                dag_tuple = self._gen_dag(model)  # Build DAG object

                if dag_tuple:
                    dag_tuples.append(
                        dag_tuple
                    )  # Add DAG tuple(dag_id, dag object) to the list

        airflow_args = (
            self._get_airflow_params()
        )  # Get airflow params to determine the execution context

        if airflow_args.get("subcommmand") == "run":
            run_dag_id = airflow_args.get(
                "dag_id"
            )  # Worker is executing a specific task
            for dag_id, dag in dag_tuples:
                if run_dag_id == dag_id:
                    __globals__.update(
                        {dag_id: dag}
                    )  # Register only task being executed
        else:
            for dag_id, dag in dag_tuples:  # Scheduler/Webserver is loading all DAGs
                __globals__.update({dag_id: dag})  # Register all DAGs
