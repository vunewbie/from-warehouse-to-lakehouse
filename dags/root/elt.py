from generators.elt import ELTGenerator
import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
ELT_GENERATOR = ELTGenerator(
    base_config_path=os.path.join(AIRFLOW_HOME, "dags/configs/elt")
).load_dags(globals())
