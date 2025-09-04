import os
import time
import requests
import logging

log = logging.getLogger(__name__)
ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ORG_ID = os.getenv("ASTRO_ORGANIZATION_ID")

if not ASTRO_API_TOKEN or not ORG_ID:
    raise EnvironmentError("Missing ASTRO_API_TOKEN or ASTRO_ORGANIZATION_ID in environment.")

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json",
}

BASE_URL = "https://api.astronomer.io/platform/v1beta1"


def wait_for_deployment_state(deployment_id, status, max_attempts=10, delay=15):
    """Polls until the deployment reaches one of the expected status values or fails."""
    if isinstance(status, str):
        target_states = [status]
    else:
        target_states = status

    for attempt in range(max_attempts):
        url = f"{BASE_URL}/organizations/{ORG_ID}/deployments/{deployment_id}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            log.info(f"⚠️ Could not check status for deployment {deployment_id}")
            return False
        current = resp.json().get("status")
        log.info(f"⏳ Deployment {deployment_id} status: {current}")
        if current in target_states:
            return True
        if current == "FAILED":
            log.info(f"❌ Deployment {deployment_id} entered FAILED state.")
            return False
        time.sleep(delay)

    log.info(f"❌ Deployment {deployment_id} did not reach one of {target_states} after polling.")
    return False


def manage_backup_hibernation(deployment_id, action):
        hibernation_url = f"{BASE_URL}/organizations/{ORG_ID}/deployments/{deployment_id}/hibernation-override"
        try:
            response = requests.post(
                hibernation_url,
                headers=HEADERS,
                json={"isHibernating": action == "hibernate"}
            )

            if response.status_code == 200:
                log.info(f"Triggered {action} for {deployment_id}")
            elif "already hibernating" in response.text and action == "hibernate":
                log.info(f"{deployment_id} is already hibernating.")
            else:
                log.info(f"Failed to trigger {action} for {deployment_id}")

        except Exception as e:
            log.info(f"🔥 Exception while attempting to {action} {deployment_id}: {str(e)}")

        # Wait for final expected state
        target = ["HIBERNATING"] if action == "hibernate" else ["HEALTHY", "READY"]
        if wait_for_deployment_state(deployment_id, status=target):
            log.info(f"✅ {deployment_id} reached target state {target}")
        else:
            log.info(f"❌ {deployment_id} did not reach target state {target}")
