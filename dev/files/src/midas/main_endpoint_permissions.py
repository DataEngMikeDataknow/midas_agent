import argparse
import logging

import requests
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def _get_bearer_token(workspace_client: WorkspaceClient) -> str:
    try:
        auth_headers = workspace_client.config.authenticate()
    except TypeError:
        auth_headers = {}
        workspace_client.config.authenticate(auth_headers)
    return auth_headers.get("Authorization", "Bearer ").split()[-1]


def grant_endpoint_manage_access(
    workspace_client: WorkspaceClient,
    endpoint_name: str,
    user_name: str,
):
    if not user_name:
        raise ValueError("Debe informarse --endpoint_manager_user")

    token = _get_bearer_token(workspace_client)
    endpoint = workspace_client.serving_endpoints.get(endpoint_name)
    endpoint_id = endpoint.id
    url = (
        f"{workspace_client.config.host.rstrip('/')}"
        f"/api/2.0/permissions/serving-endpoints/{endpoint_id}"
    )
    payload = {
        "access_control_list": [
            {
                "user_name": user_name,
                "permission_level": "CAN_MANAGE",
            }
        ]
    }

    log.info(
        "Asignando permiso CAN_MANAGE sobre endpoint=%s (id=%s) a user=%s",
        endpoint_name,
        endpoint_id,
        user_name,
    )
    response = requests.patch(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError:
        log.error("Permissions API error body: %s", response.text)
        raise
    log.info("Permiso aplicado exitosamente.")


def main():
    parser = argparse.ArgumentParser(description="Grant endpoint permissions after endpoint deploy")
    parser.add_argument("--endpoint_name", required=True)
    parser.add_argument("--endpoint_manager_user", required=True)
    args = parser.parse_args()

    workspace_client = WorkspaceClient()
    grant_endpoint_manage_access(
        workspace_client,
        args.endpoint_name,
        args.endpoint_manager_user,
    )


if __name__ == "__main__":
    main()
