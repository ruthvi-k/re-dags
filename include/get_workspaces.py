import os
import json
import requests

ASTRO_API_TOKEN = os.getenv("ASTRO_API_TOKEN")
ORG_ID = os.getenv("ASTRO_ORGANIZATION_ID")

WORKSPACES_URL = f"https://api.astronomer.io/platform/v1beta1/organizations/{ORG_ID}/workspaces"

HEADERS = {
    "Authorization": f"Bearer {ASTRO_API_TOKEN}",
    "Content-Type": "application/json"
}

def get_workspaces():
    workspaces = requests.get(WORKSPACES_URL, headers=HEADERS, params={"limit": 1000})
    workspaces.raise_for_status()
    all_workspaces = workspaces.json()["workspaces"]
    return {workspace["id"] for workspace in all_workspaces}
