import os
from datetime import datetime
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from include.get_workspaces import get_workspaces
from include.create_backup_workspaces import create_backup_workspaces, map_source_workpaces_to_backup
from include.create_backup_deployments import create_backup_deployments, get_source_deployments_payload
from include.manage_backup_hibernation import manage_backup_hibernation
from include.deploy_to_backup_deployments import replicate_deploy_to_backup
from include.migrate_with_starship import (
    migrate_variables,
    migrate_pools,
    migrate_dag_runs,
    migrate_task_instances,
    flip_dags_state
)

@dag(
    dag_id="dr_maintenance_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "astro",
        "retries": 3,
        "retry_delay": 60,
    },
    params={
        "failover": Param(
            False,
            type="boolean",
            description="Whether to failover to backup deployments."
        )
    },
    description="Disaster Recovery DAG.",
    tags=["Disaster Recovery", "Example DAG"],
)
def dr_maintenance_dag():
    @task(task_id="get_source_workspaces")
    def get_source_workspaces_task():
        return get_workspaces()
    
    @task(task_id="map_source_workspaces_to_backup")
    def map_source_workpaces_to_backup_task(workspaces):
        mapped_workspaces = map_source_workpaces_to_backup(workspaces)
        return mapped_workspaces

    @task(map_index_template="{{ source_workspace_id }}")
    def create_backup_workspaces_task(workspace):
        context = get_current_context()
        source_workspace_id = workspace.get("source_workspace_id")
        backup_workspace_name = workspace.get("backup_workspace_name")
        context["source_workspace_id"] = f"Backup for Workspace ID - {source_workspace_id}"
        create_backup_workspaces(source_workspace_id, backup_workspace_name, context)

    @task(trigger_rule="none_failed", map_index_template="{{ source_workspace_id }}")
    def get_source_deployments_task(workspace_ids):
        context = get_current_context()
        source_workspace_id = workspace_ids.get("source_workspace_id")
        backup_workspace_id = workspace_ids.get("backup_workspace_id")
        context["source_workspace_id"] = f"Deployments for Workspace ID - {source_workspace_id}"
        workspace_deployments_mapping = get_source_deployments_payload(source_workspace_id, backup_workspace_id, context)
        return workspace_deployments_mapping
    
    @task
    def create_deployment_payloads(nested: list[list[dict]]) -> list[dict]:
        return [item for sublist in nested for item in sublist]

    @task(trigger_rule="none_failed", map_index_template="{{ source_deployment_id }}")
    def create_backup_deployments_task(deployment):
        context = get_current_context()
        source_deployment_id = deployment.get("source_deployment_id")
        deployment_payload = deployment.get("deployment_payload")
        context["source_deployment_id"] = f"Backup for Deployment ID - {source_deployment_id}"
        create_backup_deployments(deployment_payload, source_deployment_id, context)

    @task(trigger_rule="none_failed", map_index_template="{{ backup_deployment_id }}")
    def manage_backup_hibernation_task(action, deployments):
        context = get_current_context()
        backup_deployment_id = deployments.get("backup_deployment_id")
        context["backup_deployment_id"] = f"{backup_deployment_id} - Un/Hibernate"
        manage_backup_hibernation(backup_deployment_id, action)

    @task(trigger_rule="none_failed", map_index_template="{{ backup_deployment_id }}")
    def replicate_deploy_to_backup_task(deployments):
        context = get_current_context()
        source_deployment_id = deployments.get("source_deployment_id")
        backup_deployment_id = deployments.get("backup_deployment_id")
        context["backup_deployment_id"] = f"{backup_deployment_id} - Deploy"
        replicate_deploy_to_backup(source_deployment_id, backup_deployment_id, context)

    @task.branch
    def check_failover_condition():
        context = get_current_context()
        failover = bool(context["params"].get("failover", False))
        if failover:
            return "failover_to_backup_deployment_task"
        else:
            return "starship_migration_task"
    
    @task(trigger_rule="none_failed", map_index_template="{{ backup_deployment_id }}")
    def failover_to_backup_deployment_task(deployments):
        context = get_current_context()
        ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]
        source_deployment_id = deployments.get("source_deployment_id")
        backup_deployment_id = deployments.get("backup_deployment_id")
        context["backup_deployment_id"] = f"{source_deployment_id} to {backup_deployment_id} - Failover/Pause Source DAGs"

        flip_dags_state(
            dag_state='pause',
            deployment_url=f"{ORG_ID}.astronomer.run/d{source_deployment_id[-7:]}"
        )

    @task(trigger_rule="all_done", map_index_template="{{ backup_deployment_id }}")
    def starship_migration_task(deployments):
        ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]
        source_deployment_id = deployments.get("source_deployment_id")
        backup_deployment_id = deployments.get("backup_deployment_id")
        source_deployment_url = f"{ORG_ID}.astronomer.run/d{source_deployment_id[-7:]}"
        target_deployment_url = f"{ORG_ID}.astronomer.run/d{backup_deployment_id[-7:]}"

        context = get_current_context()
        context["backup_deployment_id"] = f"{backup_deployment_id} - Migrate History"

        migrate_variables(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )

        migrate_pools(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )

        migrate_dag_runs(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )

        migrate_task_instances(
            source_deployment_url=source_deployment_url,
            target_deployment_url=target_deployment_url,
        )

    @task(trigger_rule="all_done", map_index_template="{{ backup_deployment_id }}")
    def post_migration_activities(deployments):
        context = get_current_context()
        ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]
        backup_deployment_id = deployments.get("backup_deployment_id")
        context["backup_deployment_id"] = f"{backup_deployment_id} - Unpause Backup DAGs"
        failover = bool(context["params"].get("failover", False))
        if not failover:
            # manage_backup_hibernation(backup_deployment_id, action="hibernate")
            pass
        else:
            flip_dags_state(
                dag_state='unpause',
                deployment_url=f"{ORG_ID}.astronomer.run/d{backup_deployment_id[-7:]}"
            )

    workspaces = get_source_workspaces_task()
    mapped_workspaces = map_source_workpaces_to_backup_task(workspaces)

    workspace_id_mapping = create_backup_workspaces_task.expand(workspace=mapped_workspaces)
    deployments = get_source_deployments_task.expand(workspace_ids=workspace_id_mapping)

    deployments_payload = create_deployment_payloads(deployments)

    created_deployments = create_backup_deployments_task.expand(deployment=deployments_payload)

    unhibernate_task = manage_backup_hibernation_task.override(task_id="unhibernate_backup_deployments").partial(action="unhibernate").expand(deployments=created_deployments)

    replicate_deploy = replicate_deploy_to_backup_task.expand(deployments=created_deployments)

    is_failover = check_failover_condition()

    failover = failover_to_backup_deployment_task.expand(deployments=created_deployments)

    starship_migration = starship_migration_task.expand(deployments=created_deployments)

    post_migration = post_migration_activities.expand(deployments=created_deployments)

    unhibernate_task >> replicate_deploy >> is_failover >> starship_migration >> post_migration
    is_failover >> failover >> starship_migration

dr_maintenance_dag()
