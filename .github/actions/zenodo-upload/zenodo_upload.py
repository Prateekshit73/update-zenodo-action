# update-zenodo-action/zenodo_upload.py
import os
import requests
import yaml
from typing import Dict, List


def get_metadata(metadata_path: str) -> Dict:
    with open(metadata_path, 'r') as f:
        return yaml.safe_load(f)


def create_new_version(api_token: str, concept_id, sandbox: bool = True) -> str:
    base_url = "https://zenodo.org" if not sandbox else "https://sandbox.zenodo.org"
    headers = {"Authorization": f"Bearer {api_token}"}

    # Create new version
    response = requests.post(
        f"{base_url}/api/deposit/depositions/{concept_id}/actions/newversion",
        headers=headers
    )
    response.raise_for_status()
    return response.json()["links"]["latest_draft"]


def upload_files(deposition_url: str, api_token: str, files: List[str]):
    headers = {"Authorization": f"Bearer {api_token}"}

    # Upload each file
    for file_path in files:
        with open(file_path, 'rb') as f:
            response = requests.post(
                f"{deposition_url}/files",
                data={'name': os.path.basename(file_path)},
                files={'file': f},
                headers=headers
            )
        response.raise_for_status()


def main():
    # Configuration
    api_token = os.environ["ZENODO_TOKEN"]
    concept_id = os.environ["ZENODO_CONCEPT_ID"]
    files = os.environ["ZENODO_FILES"].split(',')
    metadata_file = os.environ["ZENODO_METADATA_FILE"]
    sandbox = os.environ.get("ZENODO_SANDBOX", "true").lower() == "true"

    try:
        # Create new deposition version
        deposition_url = create_new_version(api_token, concept_id, sandbox)

        # Upload files
        upload_files(deposition_url, api_token, files)

        # Publish deposition
        publish_url = f"{deposition_url}/actions/publish"
        response = requests.post(publish_url, headers={"Authorization": f"Bearer {api_token}"})
        response.raise_for_status()

        print(f"::set-output name=doi::{response.json()['doi']}")

    except Exception as e:
        print(f"::error::Zenodo upload failed: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()