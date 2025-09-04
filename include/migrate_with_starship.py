import os
import logging
from typing import Dict, List
import datetime
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

MAX_OBJ_FETCH_NUM_PER_REQ = int(os.environ.get("MAX_OBJ_FETCH_NUM_PER_REQ", "100"))
MAX_OBJ_POST_NUM_PER_REQ = int(os.environ.get("MAX_OBJ_FETCH_NUM_PER_REQ", "50"))
env_value = os.environ.get("DAG_RUNS_START_DATE_FOR_MIGRATION")

if env_value:
    DAG_RUNS_START_DATE_FOR_MIGRATION = datetime.datetime.strptime(
        env_value,
        "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=datetime.timezone.utc)
else:
    DAG_RUNS_START_DATE_FOR_MIGRATION = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(hours=48)
    )

MIGRATE_DAGS_FROM_DATE = os.environ.get("MIGRATE_DAGS_FROM_DATE", "YES")

def _get_header(token: str = None) -> Dict[str, str]:
    astro_api_token = token if token else os.getenv("ASTRO_API_TOKEN")
    if not astro_api_token:
        raise ValueError("ASTRO_API_TOKEN environment variable is not set.")

    return {
        "Authorization": f"Bearer {astro_api_token}",
    }


def _post_header(token: str = None) -> Dict[str, str]:
    post_header = _get_header(token)
    post_header["Content-Type"] = "application/json"
    return post_header


###########
# VARIABLES
###########
def get_starship_variables(deployment_url: str, token: str = None) -> List[Dict[str, str]]:
    url = f"https://{deployment_url}/api/starship/variables"
    response = requests.get(
        url,
        headers=_get_header(token),
        timeout=30,
    )
    response.raise_for_status()
    log.info(f"Successfully fetched variables from {deployment_url}")
    return response.json()

def set_starship_variable(deployment_url: str, variable: Dict[str, str], token: str = None) -> Dict[str, str]:
    url = f"https://{deployment_url}/api/starship/variables"
    try:
        response = requests.post(
            url,
            json=variable,
            headers=_post_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(
                    f"Variable `{variable['key']}` already exists in target Airflow, skipping existing variables.")
            else:
                log.error(f"Failed to post variable {variable['key']} to target Airflow: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting variables: {e}")
            raise


def migrate_variables(source_deployment_url: str, target_deployment_url: str, token: str = None) -> None:
    source_variables = get_starship_variables(source_deployment_url, token)

    i = 0
    for variable in source_variables:
        set_starship_variable(target_deployment_url, variable)
        i += 1
        log.info(f"Completed {(i / len(source_variables) * 100):.2f}% for variables")
    log.info("Completed 100% for variables")
    log.info(f"Synced {len(source_variables)} variables to target Airflow.")
    log.info("-" * 80)


###########
# POOLS
###########
def get_starship_pools(deployment_url: str, token: str = None) -> List[Dict[str, str]]:
    url = f"https://{deployment_url}/api/starship/pools"
    response = requests.get(
        url,
        headers=_get_header(token),
        timeout=30,
    )
    response.raise_for_status()
    log.info(f"Successfully fetched pools from {deployment_url}")
    return response.json()


def set_starship_pool(deployment_url: str, pool: Dict[str, str], token: str = None) -> Dict[str, str]:
    url = f"https://{deployment_url}/api/starship/pools"
    for key in ("deferred_slots", "occupied_slots", "open_slots", "queued_slots", "scheduled_slots"):
        pool.pop(key, None)

    try:
        response = requests.post(
            url,
            json=pool,
            headers=_get_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(f"Pool `{pool['name']}` already exists in target Airflow, skipping existing pools.")
            else:
                log.error(f"Failed to post pool {pool['name']} to target Airflow: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting pools: {e}")
            raise


def migrate_pools(source_deployment_url: str, target_deployment_url: str, token: str = None) -> None:
    source_pools = get_starship_pools(source_deployment_url, token)

    i = 0
    for pool in source_pools:
        set_starship_pool(target_deployment_url, pool)
        i += 1
        log.info(f"Completed {(i / len(source_pools) * 100):.2f}% for pools")
    log.info("Completed 100% for pools")
    log.info(f"Synced {len(source_pools)} pools to target Airflow.")
    log.info("-" * 80)


##########
# DAG History
##########
def get_all_dags(deployment_url: str, token: str) -> List[str]:
    url = f"https://{deployment_url}/api/starship/dags"
    response = requests.get(
        url,
        headers=_get_header(token),
        timeout=60,
    )
    response.raise_for_status()
    log.info(f"Successfully fetched DAGs from {deployment_url}")
    result = response.json()
    return [dag["dag_id"] for dag in result]


def get_dag_runs(deployment_url, dag_id, offset, token: str = None):
    url = f"https://{deployment_url}/api/starship/dag_runs"
    params = {"dag_id": dag_id, "offset": offset, "limit": MAX_OBJ_FETCH_NUM_PER_REQ}
    response = requests.get(
        url,
        params=params,
        headers=_get_header(token),
        timeout=60,
    )
    response.raise_for_status()
    result = response.json()
    return result.get("dag_runs", [])

def start_dt_from_dag_run(dag_run: dict) -> datetime:
    return datetime.datetime.fromisoformat(dag_run["data_interval_start"])

def get_all_dag_runs(deployment_url, dag_id, token: str = None):
    offset = 0
    all_dag_runs = []
    while True:
        log.info(f"Getting dag runs for dag_id: {dag_id}, offset: {offset}")
        dag_runs = get_dag_runs(deployment_url, dag_id, offset, token)
        all_dag_runs.extend(dag_runs)
        if MIGRATE_DAGS_FROM_DATE == "YES":
            if len(dag_runs) < MAX_OBJ_FETCH_NUM_PER_REQ or start_dt_from_dag_run(dag_runs[-1]) < DAG_RUNS_START_DATE_FOR_MIGRATION:
                break
        else:
            if len(dag_runs) < MAX_OBJ_FETCH_NUM_PER_REQ:
                break
        offset += MAX_OBJ_FETCH_NUM_PER_REQ

    if MIGRATE_DAGS_FROM_DATE == "YES":
        return [dag_run for dag_run in all_dag_runs if start_dt_from_dag_run(dag_run) >= DAG_RUNS_START_DATE_FOR_MIGRATION]

    return all_dag_runs


def post_dag_run(deployment_url, dag_id, dag_runs, token: str = None):
    url = f"https://{deployment_url}/api/starship/dag_runs"
    data = {"dag_runs": dag_runs}
    try:
        response = requests.post(
            url,
            json=data,
            headers=_post_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(
                    f"A few or all Dag runs already exists in target Airflow for dag_id: {dag_id}. Skipping them!")
            else:
                log.error(f"Failed to post individual dag run for dag_id: {dag_id}: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting dag runs for {dag_id}: {e}")
            raise


def migrate_dag_runs(source_deployment_url, target_deployment_url, token: str = None):
    all_dag_ids = get_all_dags(source_deployment_url, token)

    for dag_id in all_dag_ids:
        log.info(f"Getting all dag runs for dag_id: {dag_id}")
        dag_runs = get_all_dag_runs(source_deployment_url, dag_id)
        if not dag_runs:
            log.info(f"No DAG Runs to sync for dag_id: {dag_id}")
            continue
        
        for i in range(0, len(dag_runs), MAX_OBJ_POST_NUM_PER_REQ):
            post_dag_run(target_deployment_url, dag_id, dag_runs[i : i + MAX_OBJ_POST_NUM_PER_REQ], token)
            log.info(f"Posted {i}/{len(dag_runs)} dag runs for dag_id: {dag_id}")
        log.info(f"Completed 100% for DAG Runs of dag_id: {dag_id}")
        log.info(f"Synced {len(dag_runs)} DAG Runs to target Airflow")
        log.info("-" * 80)

##########
# Task History
##########
def get_task_instances(deployment_url, dag_id, offset, token: str = None):
    url = f"https://{deployment_url}/api/starship/task_instances"
    params = {"dag_id": dag_id, "offset": offset, "limit": MAX_OBJ_FETCH_NUM_PER_REQ}
    response = requests.get(
        url,
        params=params,
        headers=_get_header(token),
        timeout=60,
    )
    response.raise_for_status()
    result = response.json()
    return result.get("task_instances", [])

def start_dt_from_task_instance(ti: dict) -> datetime:
    run_id = ti["run_id"]
    if run_id is None or "__" not in run_id:
        run_dt = datetime.datetime(1979, 1, 1)
        return run_dt.astimezone(datetime.timezone.utc)

    timestamp = run_id.split("__")[1]

    return datetime.datetime.fromisoformat(timestamp)

def get_all_task_instances(deployment_url, dag_id, token: str = None):
    offset = 0
    all_task_instances = []
    while True:
        log.info(f"Getting Task Instances for dag_id: {dag_id}, offset: {offset}")
        task_instances = get_task_instances(deployment_url, dag_id, offset, token)
        all_task_instances.extend(task_instances)
        if MIGRATE_DAGS_FROM_DATE == "YES":
            if len(task_instances) < MAX_OBJ_FETCH_NUM_PER_REQ or start_dt_from_task_instance(task_instances[-1]) < DAG_RUNS_START_DATE_FOR_MIGRATION:
                break
        else:
            if len(task_instances) < MAX_OBJ_FETCH_NUM_PER_REQ:
                break

        offset += MAX_OBJ_FETCH_NUM_PER_REQ

    if MIGRATE_DAGS_FROM_DATE == "YES":
        return [task_instance for task_instance in all_task_instances if start_dt_from_task_instance(task_instance) >= DAG_RUNS_START_DATE_FOR_MIGRATION]
    return all_task_instances


def post_task_instance(deployment_url, dag_id, task_instances, token: str = None):
    url = f"https://{deployment_url}/api/starship/task_instances"
    data = {"task_instances": task_instances}
    try:
        response = requests.post(
            url,
            json=data,
            headers=_post_header(token),
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            if e.response.status_code == 409:
                log.warning(
                    f"A few or all task instanaces already exists in target Airflow for dag_id: {dag_id}. Skipping them!")
            else:
                log.error(f"Failed to post individual task instance for dag_id: {dag_id}: {e}")
                if e.response:
                    log.error(f"Response: {e.response.text}")
                raise
        else:
            log.error(f"An unexpected error occurred while posting Task Instances for {dag_id}: {e}")
            raise


def migrate_task_instances(source_deployment_url, target_deployment_url, token: str = None):
    all_dag_ids = get_all_dags(source_deployment_url, token)

    for dag_id in all_dag_ids:
        log.info(f"Getting all Task Instances for dag_id: {dag_id}")
        task_instances = get_all_task_instances(source_deployment_url, dag_id)
        if not task_instances:
            log.info(f"No Task Instances to sync for dag_id: {dag_id}")
            continue
        
        for i in range(0, len(task_instances), MAX_OBJ_POST_NUM_PER_REQ):
            post_task_instance(target_deployment_url, dag_id, task_instances[i : i + MAX_OBJ_POST_NUM_PER_REQ], token)
            log.info(f"Posted {i}/{len(task_instances)} task instances for dag_id: {dag_id}")
        log.info(f"Completed 100% for task instances of dag_id: {dag_id}")
        log.info(f"Synced {len(task_instances)} task instances to target Airflow")
        log.info("-" * 80)

#####
# UN/PAUSE DAG 
#####
def flip_dags_state(dag_state: str, deployment_url: str) -> None:
    if dag_state not in ['pause', 'unpause']:
        raise ValueError("dag_state must be either 'pause' or 'unpause'.")
    dags = get_all_dags(deployment_url=deployment_url, token=None) 
    url = f"https://{deployment_url}/api/starship/dags"

    if dag_state == 'pause':
        payload = {"is_paused": True}
        log.info("Pausing DAGs in source deployment...")
    else:
        payload = {"is_paused": False}
        log.info("Unpausing DAGs in source deployment...")

    for dag_id in dags:
        payload["dag_id"] = dag_id
        response = requests.patch(
            url,
            json=payload,
            headers=_get_header(token=None),
            verify=False,
            timeout=60,
        )
        response.raise_for_status()
