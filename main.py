import logging
import os
from typing import Dict

import requests

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


def get_token() -> str:
    """
    Get the access token from Keycloak using client credentials.
    :return:
    """
    url = f"{os.environ.get('KEYCLOAK_SERVER')}auth/realms/{os.environ.get('REALM')}/protocol/openid-connect/token"
    data = f"grant_type=client_credentials&client_id={os.environ.get('CLIENT_ID')}&client_secret={os.environ.get('CLIENT_SECRET')}&scope=roles"
    header = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url=url, data=data, headers=header)
    return response.json().get("access_token")


def get_request_json() -> Dict:
    """
    Builds the request JSON for scheduling a job at the exporter backend.
    Includes the source system ID and export type from environment variables.
    :return:
    """
    return {
        "data": {
            "type": "export-job",
            "attributes": {
                "searchParams": [
                    {
                        "inputField": "$['ods:sourceSystemID']",
                        "inputValue": f"{os.environ.get('SOURCE_SYSTEM_ID')}",
                    }
                ],
                "targetType": "https://doi.org/21.T11148/894b1e6cad57e921764e",
                "exportType": f"{os.environ.get('EXPORT_TYPE')}",
                "isSourceSystemJob": "true",
            },
        }
    }


def schedule_job_at_exporter_backend() -> None:
    """
    Schedule a job at the exporter backend by sending a POST request with the export job details.
    :return:
    """
    header = {"Authorization": "Bearer " + get_token()}
    request_json = get_request_json()
    url = f"https://{os.environ.get('DISSCO_DOMAIN')}/api/data-export/v1/schedule"
    response = requests.post(url=url, json=request_json, headers=header)
    if response.status_code != 202:
        logging.error(f"Failed to schedule job: {response.status_code} - {response.text}")
    else:
        logging.info(f"Job scheduled successfully: {response.status_code} - {response.text}")

if __name__ == "__main__":
    schedule_job_at_exporter_backend()
