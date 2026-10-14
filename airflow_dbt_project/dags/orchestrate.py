import time
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

@dag
def orchestrate():

    @task(
    retries=2,
    retry_delay=timedelta(minutes=2)
)
    def run_databricks_job():
        ws = WorkspaceClient()

        try:
            print("Triggering Databricks ingestion job...")

            job_trigger = ws.jobs.run_now(
                job_id="940385257864024"
            )

            run_id = job_trigger.run_id

            print(f"Databricks job triggered successfully. Run ID: {run_id}")

        except Exception as e:
            raise RuntimeError(
                f"Failed to trigger Databricks job: {e}"
            ) from e

        max_wait_seconds = 45 * 60
        poll_interval_seconds = 5
        start_time = time.monotonic()

        while True:

            # Prevent infinite polling
            if time.monotonic() - start_time > max_wait_seconds:
                raise TimeoutError(
                    f"Databricks job run {run_id} exceeded "
                    f"the maximum wait time of {max_wait_seconds // 60} minutes."
                )

            try:
                job_run = ws.jobs.get_run(run_id)

            except Exception as e:
                raise RuntimeError(
                    f"Failed while checking Databricks job run "
                    f"{run_id}: {e}"
                ) from e

            lifecycle_state = job_run.state.life_cycle_state
            result_state = job_run.state.result_state

            print(
                f"Databricks run {run_id} - "
                f"lifecycle_state={lifecycle_state}, "
                f"result_state={result_state}"
            )

            if lifecycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR
            ]:

                if result_state == RunResultState.SUCCESS:
                    print(
                        f"Databricks job run {run_id} completed successfully."
                    )

                    return (
                        "Databricks ingestion job completed successfully"
                    )

                raise RuntimeError(
                    f"Databricks job run {run_id} failed. "
                    f"Lifecycle state: {lifecycle_state}, "
                    f"Result state: {result_state}"
                )

            time.sleep(poll_interval_seconds)
    
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_dbt_project/target && rm -rf /opt/airflow/walmart_dbt_project/logs"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_dbt_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select silver_t",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )

    silver_technical_tests=BashOperator(
        task_id="silver_technical_tests",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt test --select silver_t",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select silver_b",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )

    silver_business_tests=BashOperator(
        task_id="silver_business_tests",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt test --select silver_b",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )

    gold_ephemeral=BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select gold/ephemeral",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )

    gold_dimensions=BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt snapshot",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )

    gold_facts=BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select gold/fact",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )

    run_databricks_job() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

orchestrate_dag=orchestrate()