"""This module contains DAGs for project."""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum



def _generate_shell_command(path_to_script: str) -> str:
    """adds cd /work/ && python -um in front of the shell command

    Adds  cd /work/ && python -um in front of the shell command in front of the command
    so that we set the working directory and force python to empty buffer so we can see the logs
    in airflow.

    Args:
        path_to_script (str): path to python script

    Returns:
        str: command containing correct working directory, python3 parameters and path to script
    """

    return f"cd /work/ && python -um {path_to_script}"


def generate_bash_operator(dag: DAG, task_id: str, path_to_script: str) -> BashOperator:
    """generates bash operator

    Generates bash operator using path to python script as a shell command.

    Args:
        dag (DAG): dag this operator is used in
        task_id (str): unique id of the operator
        path_to_script (str): path to python script

    Returns:
        BashOperator: airflow bash operator
    """
    return BashOperator(
        dag=dag,
        task_id=task_id,
        bash_command=_generate_shell_command(path_to_script=path_to_script),
    )


with DAG(
    dag_id="bnt_rfp_mini_project",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["complete_etl", "proposal"],
    description="bnt p",
) as dag:

    with TaskGroup(group_id="ingestion") as task_group_ingestion:
        ingestion = generate_bash_operator(
            dag=dag,
            task_id="ingest_covid_variants_country_indicators",
            path_to_script="bnt_rfp_mini_project.ingestion.ingest_covid_variants_country_indicators",
        )

        check_for_covid_variants_evolution_csv = FileSensor(
            dag=dag,
            task_id="check_for_covid",
            filepath="/work/data/raw/covid_variants_country_indicators/data_Data1_Covid Variants evolution.csv",
            timeout=10,
        )

        check_for_country_indicators_xlsx = FileSensor(
            dag=dag,
            task_id="check_for_country",
            filepath="/work/data/raw/covid_variants_country_indicators/data_Data2_Large set of country indicators.xlsx",
            timeout=10,
        )
        ingestion >> [
            check_for_country_indicators_xlsx,
            check_for_covid_variants_evolution_csv,
        ]
    # execute etl
    with TaskGroup(group_id="cleansing") as task_group_cleansing:
        raw2cleansed_data1_covid_variants = generate_bash_operator(
            dag=dag,
            task_id="raw2cleansed_data1_covid_variants",
            path_to_script=" bnt_rfp_mini_project.transformation.raw2cleansed.raw2cleansed_data1_covid_variants",
        )

        raw2cleansed_data2_country_indicators = generate_bash_operator(
            dag=dag,
            task_id="raw2cleansed_data2_country_indicators",
            path_to_script="bnt_rfp_mini_project.transformation.raw2cleansed.raw2cleansed_data2_country_indicators",
        )

    with TaskGroup(group_id="processing") as task_group_processing:
        cleansed2processed_data1_covid_variants = generate_bash_operator(
            dag=dag,
            task_id="cleansed2processed_data1_covid_variants",
            path_to_script="bnt_rfp_mini_project.transformation.cleansed2processed.cleansed2processed_data1_covid_variants",
        )

        cleansed2processed_data2_country_indicators = generate_bash_operator(
            dag=dag,
            task_id="cleansed2processed_data2_country_indicators",
            path_to_script="bnt_rfp_mini_project.transformation.cleansed2processed.cleansed2processed_data2_country_indicators",
        )

        cleansed2processed_join_data1_data2 = generate_bash_operator(
            dag=dag,
            task_id="cleansed2processed_join_data1_data2",
            path_to_script="bnt_rfp_mini_project.transformation.cleansed2processed.cleansed2processed_join_data1_data2",
        )
        [
            cleansed2processed_data1_covid_variants,
            cleansed2processed_data2_country_indicators,
        ] >> cleansed2processed_join_data1_data2

    with TaskGroup(group_id="final_output") as final_output:
        processed2output_exercise_1 = generate_bash_operator(
            dag=dag,
            task_id="processed2output_exercise_1",
            path_to_script="bnt_rfp_mini_project.transformation.processed2output.processed2output_exercise_1",
        )
        processed2output_exercise_2 = generate_bash_operator(
            dag=dag,
            task_id="processed2output_exercise_2",
            path_to_script="bnt_rfp_mini_project.transformation.processed2output.processed2output_exercise_2",
        )

    with TaskGroup(group_id="rejected") as rejected:
        check_for_rejected_file_data1 = FileSensor(
            dag=dag,
            task_id="rejected_file_check_covid_variants_evolution",
            filepath="/work/data/rejected/covid_variants_country_indicators/data_data1_covid_variants_evolution_rejected.csv",
            soft_fail=True,
            timeout=1,
        )
        rejected_file_flag_data1 = EmptyOperator(
            dag=dag,
            task_id="rejected_file_flagcovid_variants_evolution_flag",
        )

        check_for_rejected_file_data2 = FileSensor(
            dag=dag,
            task_id="rejected_file_check_large_set_of_country_indicators",
            filepath="/work/data/rejected/covid_variants_country_indicators/data_data2_large_set_of_country_indicators_rejected.csv",
            soft_fail=True,
            timeout=1,
        )
        rejected_file_flag_data2 = EmptyOperator(
            dag=dag,
            task_id="rejected_file_flag_large_set_of_country_indicators_flag"
        )

        check_for_rejected_file_data1 >> rejected_file_flag_data1
        check_for_rejected_file_data2 >> rejected_file_flag_data2

    raw2cleansed_data1_covid_variants >> check_for_rejected_file_data1
    raw2cleansed_data2_country_indicators >> check_for_rejected_file_data2

    (
        task_group_ingestion
        >> task_group_cleansing
        >> task_group_processing
        >> final_output
    )
