import os
from typing import Any

import requests
import yaml

ZENODO_URL = "https://zenodo.org"
ZENODO_SANDBOX_URL = "https://sandbox.zenodo.org"


class ZenodoAPI:
    """Client for interacting with the Zenodo API.

    Supports both production and sandbox environments for managing deposits.
    """

    def __init__(self, auth_token: str, sandbox: bool = False, metadata_file: str | None = None):
        """Initialize the Zenodo API client.

        Args:
            auth_token:
                Zenodo API access token.

            sandbox:
                If True, use sandbox.zenodo.org; otherwise, use zenodo.org.
                Defaults to False.

            metadata_file:
                Path to CITATION.cff file.
        """
        self.base_url = f"{ZENODO_SANDBOX_URL if sandbox else ZENODO_URL}/api/deposit/depositions"
        self.headers = {
            "Authorization": f"Bearer {auth_token}",
        }
        if metadata_file:
            with open(metadata_file, encoding="utf-8") as f:
                self.matadata = yaml.safe_load(f)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        json: dict | None = None,
        files: dict | None = None,
    ) -> Any:
        """Perform an HTTP request to the Zenodo API.

        Args:
            method:
                HTTP method ('GET', 'POST', 'PUT', 'DELETE').

            endpoint:
                API endpoint (relative to base_url).

            json:
                JSON data for POST/PUT requests. Defaults to None.

            files:
                Files for multipart/form-data POST requests. Defaults to None.

        Returns:
            Parsed JSON response or None for DELETE requests.

        Raises:
            requests.HTTPError: If the request fails.
        """
        url = f"{self.base_url}{endpoint}"
        headers = self.headers

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
            files=files,
        )
        response.raise_for_status()

        return response.json() if response.content and method != "DELETE" else None

    def create_concept(self) -> str:
        """Create a new concept.

        Returns:
            Deposition ID of the new concept.

        Raises:
            requests.HTTPError: If the API request fails.
            KeyError: If the DOI are missing in metadata file.
        """
        response = self._make_request("POST", "")
        new_deposition_id = response["id"]

        return new_deposition_id

    def create_version(self, concept_id: str | None = None) -> str:
        """Create a new concept version based on concept_id (specified or from metadata file).

        Returns:
            Deposition ID of the new concept version.

        Raises:
            requests.HTTPError: If the API request fails.
            KeyError: If the DOI are missing in metadata file.
        """
        concept_id = concept_id or self.matadata["doi"].split(".")[-1]
        response = self._make_request("GET", f"?q=conceptrecid:{concept_id}")
        deposition_id = response[0]["id"]
        response = self._make_request("POST", f"/{deposition_id}/actions/newversion")
        new_deposition_id = response["id"]

        return new_deposition_id

    def update_metadata(self, deposition_id: str) -> None:
        """Update deposit metadata based on metadata file data.

        Args:
            deposition_id:
                ID of the deposit.

        Raises:
            requests.HTTPError: If the API request fails.
            KeyError: If required fields are missing in metadata file.
        """
        metadata = self.matadata

        new_metadata = {}
        if m_title := metadata.get("title"):
            new_metadata["title"] = m_title
        if m_upload_type := metadata.get("type"):
            new_metadata["upload_type"] = m_upload_type
        if m_authors := metadata.get("authors"):
            new_metadata["creators"] = [
                {"name": f"{author['family-names']}, {author['given-names']}"}
                for author in m_authors
            ]
        if m_description := metadata.get("abstract"):
            new_metadata["description"] = m_description
        if m_keywords := metadata.get("keywords"):
            new_metadata["keywords"] = m_keywords
        if m_license := metadata.get("license"):
            new_metadata["license"] = {"id": m_license}
        if m_version := metadata.get("version"):
            new_metadata["version"] = m_version
        if m_publication_date := metadata.get("date-released"):
            new_metadata["publication_date"] = m_publication_date
        if m_repository_code := metadata.get("repository-code"):
            new_metadata["custom"] = {
                "code:codeRepository": m_repository_code,
                # TODO: make custom fileds?
                "code:programmingLanguage": [{"id": "python", "title": {"en": "Python"}}],
                "code:developmentStatus": {"id": "active", "title": {"en": "Active"}},
            }

        response = self._make_request("GET", f"/{deposition_id}")
        updated_metadata = {"metadata": response["metadata"] | new_metadata}
        self._make_request("PUT", f"/{deposition_id}", json=updated_metadata)

    def delete_files(self, deposition_id: str) -> None:
        """Delete all files from a deposit version.

        Args:
            deposition_id:
                ID of the deposit.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        files = self._make_request("GET", f"/{deposition_id}/files") or []
        for file in files:
            file_id = file["id"]
            self._make_request("DELETE", f"/{deposition_id}/files/{file_id}")

    def upload_files(self, deposition_id: str, file_paths: list[str]) -> None:
        """Upload files to a deposit version.

        Args:
            deposition_id:
                ID of the deposit.

            file_paths:
                List of file paths to upload.

        Raises:
            requests.HTTPError: If the API request fails.
            FileNotFoundError: If a file path is invalid.
        """
        for file_path in file_paths:
            with open(file_path, "rb") as f:
                files = {"file": (os.path.basename(file_path), f)}
                self._make_request("POST", f"/{deposition_id}/files", files=files)

    def publish_version(self, deposition_id: str) -> None:
        """Publish a deposit version.

        Args:
            deposition_id:
                ID of the deposit.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        self._make_request("POST", f"/{deposition_id}/actions/publish")
