import requests
import os
import subprocess
import logging
from datetime import datetime

log = logging.getLogger(__name__)

ASTRO_API_URL = "https://api.astronomer.io/platform/v1beta1"
ASTRO_TOKEN = os.environ["ASTRO_API_TOKEN"]
ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]

HEADERS = {
    "Authorization": f"Bearer {ASTRO_TOKEN}",
    "Content-Type": "application/json",
}
def do_image_deploy(source_image_tag, source_registry_link, description, source_deployment_id, backup_deployment_id):
    log.info(f"Initiating Image Deploy Process for deployment {backup_deployment_id}")
    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys"
    payload = {
        "type": "IMAGE_AND_DAG",
        "description": description
    }
    response = requests.post(deploy_url, headers=HEADERS, json=payload)
    response.raise_for_status()
    deploy_initialized = response.json()
    backup_registry_link = deploy_initialized.get("imageRepository")
    backup_image_tag = deploy_initialized.get("imageTag")
    dags_upload_url = deploy_initialized.get("dagsUploadUrl")
    deploy_id = deploy_initialized.get("id")

    cmd = [
        "skopeo", "copy",
        "--src-creds",  f"cli:{ASTRO_TOKEN}",
        "--dest-creds", f"cli:{ASTRO_TOKEN}",
        f"docker://{source_registry_link}:{source_image_tag}",
        f"docker://{backup_registry_link}:{backup_image_tag}"
    ]

    log.info("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    log.info("Image successfully copied via skopeo")

    log.info("Downloading DAGs...")
    # This is a placeholder URL; you should replace it with the actual URL where your DAGs tarball is hosted
    # This example is for Public S3 Bucket. For Private S3 Buckets, you may need to use a signed URL or other authentication methods.
    url = f"https://af-demo-dags-bucket.s3.amazonaws.com/{source_deployment_id}.tar.gz"
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with open(f"{source_deployment_id}.tar.gz", "wb") as f:
        for chunk in resp.iter_content(1024 * 64):
            f.write(chunk)

    log.info(f"Uploading tar file {source_deployment_id}.tar.gz")
    put_headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/x-gtar"
    }

    put_resp = requests.put(dags_upload_url, headers=put_headers, data=open(f"{source_deployment_id}.tar.gz", "rb"))
    put_resp.raise_for_status()
    version_id = put_resp.headers.get("x-ms-version-id")
    log.info(f"DAGs uploaded successfully. Version ID: {version_id}")

    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys/{deploy_id}/finalize"
    response = requests.post(deploy_url, headers=HEADERS, json={"dagTarballVersion": version_id})
    response.raise_for_status()
    response = response.json()
    log.info(f"Image & DAG deploy for deployment {backup_deployment_id} completed successfully.")
    log.info(response)


def do_dag_deploy(description, source_deployment_id, backup_deployment_id):
    log.info(f"Initiating DAG-ONLY Deploy Process for deployment {backup_deployment_id}")
    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys"
    payload = {
        "type": "DAG_ONLY",
        "description": description
    }
    response = requests.post(deploy_url, headers=HEADERS, json=payload)
    response.raise_for_status()
    deploy_initialized = response.json()
    dags_upload_url = deploy_initialized.get("dagsUploadUrl")
    deploy_id = deploy_initialized.get("id")
    
    log.info("Downloading DAGs...")

    url = f"https://af-demo-dags-bucket.s3.amazonaws.com/{source_deployment_id}.tar.gz"
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with open(f"{source_deployment_id}.tar.gz", "wb") as f:
        for chunk in resp.iter_content(1024 * 64):
            f.write(chunk)

    log.info(f"Uploading tar file {source_deployment_id}.tar.gz")
    put_headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/x-gtar"
    }

    put_resp = requests.put(dags_upload_url, headers=put_headers, data=open(f"{source_deployment_id}.tar.gz", "rb"))
    put_resp.raise_for_status()
    version_id = put_resp.headers.get("x-ms-version-id")
    log.info(f"DAGs uploaded successfully. Version ID: {version_id}")

    deploy_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys/{deploy_id}/finalize"
    response = requests.post(deploy_url, headers=HEADERS, json={"dagTarballVersion": version_id})
    response.raise_for_status()
    response = response.json()
    log.info("DAG deploy finalized successfully.")

def parse_iso(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp ending with 'Z' into a datetime."""
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


def replicate_deploy_to_backup(source_deployment_id: str, backup_deployment_id: str, context):
    source_deployment_deploy_detail_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{source_deployment_id}/deploys"
    backup_deployment_deploy_detail_url = f"{ASTRO_API_URL}/organizations/{ORG_ID}/deployments/{backup_deployment_id}/deploys"
    
    response = requests.get(source_deployment_deploy_detail_url, headers=HEADERS)
    response.raise_for_status()
    source_deployment_deploy_details = response.json()

    response = requests.get(backup_deployment_deploy_detail_url, headers=HEADERS)
    response.raise_for_status()
    backup_deployment_deploy_details = response.json()

    latest_source_deploy = {}
    for d in source_deployment_deploy_details['deploys']:
        if d["status"] != "DEPLOYED":
            continue
        deploy_type = d['type']
        created_at = parse_iso(d['createdAt'])
        if deploy_type not in latest_source_deploy or created_at > latest_source_deploy[deploy_type][0]:
            latest_source_deploy[deploy_type] = (created_at, d)
    latest_deploy = {t: entry[1] for t, entry in latest_source_deploy.items()}
    latest_dag_deploy_source = latest_deploy.get("DAG_ONLY")
    latest_image_deploy_source = latest_deploy.get("IMAGE_AND_DAG")

    latest_backup_deploy = {}
    for d in backup_deployment_deploy_details['deploys']:
        if d["status"] != "DEPLOYED":
            continue
        deploy_type = d['type']
        created_at = parse_iso(d['createdAt'])
        if deploy_type not in latest_backup_deploy or created_at > latest_backup_deploy[deploy_type][0]:
            latest_backup_deploy[deploy_type] = (created_at, d)
    latest_deploy = {t: entry[1] for t, entry in latest_backup_deploy.items()}
    latest_dag_deploy_backup = latest_deploy.get("DAG_ONLY")
    latest_image_deploy_backup = latest_deploy.get("IMAGE_AND_DAG")


    if not latest_image_deploy_backup or (latest_image_deploy_source.get("description") != latest_image_deploy_backup.get("description")):
        description = latest_image_deploy_source.get("description")
        source_registry_link = latest_image_deploy_source.get("imageRepository")
        source_image_tag = latest_image_deploy_source.get("imageTag")
        log.info("Mismatch in Image Deploys! Deploying image to backup deployment...")
        do_image_deploy(source_image_tag, source_registry_link, description, source_deployment_id, backup_deployment_id)
    else:
        log.info("Image Deploys are in sync.")

    if not latest_dag_deploy_backup or (latest_dag_deploy_source.get("description") != latest_dag_deploy_backup.get("description")):
        description = latest_dag_deploy_source.get("description")
        log.info("Mismatch in DAG Deploys! Deploying DAG to backup deployment...")
        do_dag_deploy(description, source_deployment_id, backup_deployment_id)
    else:
        log.info("DAG Deploys are in sync.")
