import os
import requests
import json
import logging
from airflow.exceptions import AirflowException, AirflowSkipException

log = logging.getLogger(__name__)
ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ASTRO_ORGANIZATION_ID = os.getenv("ASTRO_ORGANIZATION_ID")

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}

WORKSPACES_JSON_PATH = os.path.join(os.path.dirname(__file__), "workspaces_to_backup.json")

def create_backup_workspaces(source_workspace_id, backup_workspace_name, context):
    existing_url = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
    existing_resp = requests.get(existing_url, headers=HEADERS, params={"limit": 1000})
    existing_resp.raise_for_status()
    existing_workspaces = existing_resp.json().get("workspaces", [])

    existing_workspace = next(
        (ws for ws in existing_workspaces if ws.get("id") == source_workspace_id),
        None,
    )

    backup_workspace = next(
        (ws for ws in existing_workspaces if ws.get("name") == backup_workspace_name),
        None,
    )

    if backup_workspace:
        ti = context['ti']
        ti.xcom_push(key="return_value", value={"source_workspace_id": source_workspace_id, "backup_workspace_id": backup_workspace.get("id")})
        raise AirflowSkipException(f"Backup workspace '{backup_workspace_name}' already exists. Skipping creation.")

    if existing_workspace:
        source_workspace_description = existing_workspace.get("description", "")
        source_workspace_cicd_default = existing_workspace.get("cicdEnforcedDefault", True)
        log.info(f"Creating backup workspace for {source_workspace_id} as {backup_workspace_name}")

        payload = {
            "name": backup_workspace_name,
            "description": f"Backup for workspace {source_workspace_id} - {source_workspace_description}",
            "cicdEnforcedDefault": source_workspace_cicd_default
        }

        url = f"https://api.astronomer.io/platform/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/workspaces"
        response = requests.post(url, headers=HEADERS, json=payload)
        
        if response.status_code in (201, 200):
            backup_workspace_id = response.json().get("id")
            log.info(f"Successfully created backup workspace: {backup_workspace_name}")
            ti = context['ti']
            ti.xcom_push(key="return_value", value={"source_workspace_id": source_workspace_id, "backup_workspace_id": backup_workspace_id})
        else:
            log.info(f"Failed to create backup workspace {backup_workspace_name}. Status: {response.status_code}, Message: {response.text}")

        get_tokens_url = f"https://api.astronomer.io/iam/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/tokens?workspaceId={source_workspace_id}"
        response = requests.get(get_tokens_url, headers=HEADERS)
        response.raise_for_status()
        tokens = response.json().get("tokens", [])

        for token in tokens:
            token_name = token.get("name")
            token_description = token.get("description")
            token_roles = token.get("roles")
            token_type = token.get("type")
            for role in token_roles:
                token_payload = {
                    "name": f"Backup for {token_name}",
                    "description": f"Backup for {token_description}",
                    "role": role.get("role"),
                    "type": token_type,
                    "entityId": backup_workspace_id
                }
                create_token_url = f"https://api.astronomer.io/iam/v1beta1/organizations/{ASTRO_ORGANIZATION_ID}/tokens"
                token_response = requests.post(create_token_url, headers=HEADERS, json=token_payload)
                if token_response.status_code in (201, 200):
                    log.info(f"Successfully created token: {token_name} for backup workspace {backup_workspace_name}")
                else:
                    log.info(f"Failed to create token {token_name} for backup workspace {backup_workspace_name}. Status: {token_response.status_code}, Message: {token_response.text}")

    else:
        raise AirflowException(f"Source workspace {source_workspace_id} not found in existing workspaces. Cannot create backup.")


def map_source_workpaces_to_backup(workspaces):
    with open(WORKSPACES_JSON_PATH, "r", encoding="utf-8") as f:
        workspace_entries = json.load(f)

    mapped_workspaces = []

    for entry in workspace_entries:
        workspace_source_id = entry["source_workspace_id"]
        workspace_backup_name = entry["backup_workspace_name"]

        if workspace_source_id in workspaces:
            mapped_workspaces.append({
                "source_workspace_id": workspace_source_id,
                "backup_workspace_name": workspace_backup_name
            })
        else:
            log.info(f"Source workspace {workspace_source_id} not found in provided workspaces.")

    return mapped_workspaces
