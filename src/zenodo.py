import logging
import os
from typing import Any, Optional
import requests
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
ZENODO_URL = "https://zenodo.org"
ZENODO_SANDBOX_URL = "https://sandbox.zenodo.org"


class ZenodoAPI:
    """Client for interacting with the Zenodo API."""

    def __init__(self, auth_token: str, sandbox: bool = False, metadata_file: Optional[str] = None):
        self.base_url = f"{ZENODO_SANDBOX_URL if sandbox else ZENODO_URL}/api/deposit/depositions"
        self.headers = {"Authorization": f"Bearer {auth_token}"}
        self.metadata = {}
        logger.debug("Initializing ZenodoAPI with sandbox=%s", sandbox)

        if metadata_file:

            try:
                with open(metadata_file, encoding="utf-8") as f:
                    self.metadata = yaml.safe_load(f)
                logger.info("Loaded metadata from %s", metadata_file)

            except Exception as e:
                logger.error("Metadata load failed: %s", str(e))
                raise

    def _make_request(
            self,
            method: str,
            endpoint: str,
            json: Optional[dict] = None,
            files: Optional[dict] = None
    ) -> Any:

        """Perform an HTTP request to the Zenodo API."""
        url = f"{self.base_url}{endpoint}"
        logger.debug("%s %s", method.upper(), url)

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=json,
                files=files,
                timeout=30
            )
            response.raise_for_status()
            return response.json() if response.content else None

        except requests.HTTPError as e:
            logger.error("HTTP %d: %s", e.response.status_code, e.response.text)
            raise

        except Exception as e:
            logger.error("Request failed: %s", str(e))
            raise

    def create_concept(self) -> str:
        """Create a new concept."""
        response = self._make_request("POST", "")
        new_deposition_id = response["id"]
        logger.info("Created new concept with deposition ID: %s", new_deposition_id)
        return new_deposition_id

    def create_version(self) -> str:
        """Create a new version based on concept ID from metadata."""
        try:
            concept_id = self.metadata.get("concept_id") or self.metadata["doi"].split(".")[-1]
            response = self._make_request("GET", f"?q=conceptrecid:{concept_id}")
            deposition_id = response[0]["id"]
            response = self._make_request("POST", f"/{deposition_id}/actions/newversion")
            new_deposition_id = response["id"]
            logger.info("Created new version with deposition ID: %s", new_deposition_id)
            return new_deposition_id

        except KeyError as e:
            logger.error("Missing required field in metadata: %s", str(e))
            raise


    def update_metadata(self, deposition_id: str) -> None:
        """Update deposit metadata based on metadata file data."""
        logger.info("Updating metadata for deposition: %s", deposition_id)

        metadata = self.metadata

        new_metadata = {}

        if title := metadata.get("title"):
            new_metadata["title"] = title

        if upload_type := metadata.get("type"):
            new_metadata["upload_type"] = upload_type

        if authors := metadata.get("authors"):
            new_metadata["creators"] = [
                {"name": f"{author['family-names']}, {author['given-names']}"}
                for author in authors
            ]

        if description := metadata.get("abstract"):
            new_metadata["description"] = description

        if keywords := metadata.get("keywords"):
            new_metadata["keywords"] = keywords

        if license_id := metadata.get("license"):
            new_metadata["license"] = {"id": license_id}

        if version := metadata.get("version"):
            new_metadata["version"] = version

        if publication_date := metadata.get("date-released"):
            new_metadata["publication_date"] = publication_date

        if repository_code := metadata.get("repository-code"):
            new_metadata["custom"] = {
                "code:codeRepository": repository_code,
                "code:programmingLanguage": [{"id": "python", "title": {"en": "Python"}}],
                "code:developmentStatus": {"id": "active", "title": {"en": "Active"}},
            }

        response = self._make_request("GET", f"/{deposition_id}")
        updated_metadata = {"metadata": {**response["metadata"], **new_metadata}}
        self._make_request("PUT", f"/{deposition_id}", json=updated_metadata)
        logger.info("Metadata updated successfully for deposition: %s", deposition_id)

    def full_upload_flow(self, file_paths: list[str]) -> str:
        """Complete upload workflow with error recovery."""

        try:
            deposition_id = self.create_version()
            self.delete_files(deposition_id)
            self.upload_files(deposition_id, file_paths)
            self.update_metadata(deposition_id)
            self.publish_version(deposition_id)
            return deposition_id

        except Exception as e:
            logger.error("Upload workflow failed: %s", str(e))
            raise

    def delete_files(self, deposition_id: str) -> None:
        """Delete all files from a deposit version."""
        logger.info("Deleting files from deposition: %s", deposition_id)

        try:
            files = self._make_request("GET", f"/{deposition_id}/files") or []
            logger.debug("Found %d files to delete", len(files))

            for file in files:
                file_id = file["id"]
                logger.debug("Deleting file ID: %s", file_id)
                self._make_request("DELETE", f"/{deposition_id}/files/{file_id}")
            logger.info("All files deleted successfully")

        except Exception as e:
            logger.error("File deletion failed: %s", str(e))
            raise

    def upload_files(self, deposition_id: str, file_paths: list[str]) -> None:
        """Upload files to a deposit version."""
        logger.info("Uploading %d files to deposition: %s", len(file_paths), deposition_id)

        for file_path in file_paths:
            try:
                logger.debug("Uploading file: %s", file_path)

                with open(file_path, "rb") as f:
                    files = {"file": (os.path.basename(file_path), f)}

                    self._make_request("POST", f"/{deposition_id}/files", files=files)
                logger.debug("Successfully uploaded: %s", file_path)

            except FileNotFoundError:
                logger.error("File not found: %s", file_path)
                raise

            except Exception as e:
                logger.error("Failed to upload %s: %s", file_path, str(e))
                raise

        logger.info("All files uploaded successfully")

    def publish_version(self, deposition_id: str) -> None:
        """Publish a deposit version."""
        logger.info("Publishing deposition: %s", deposition_id)

        try:
            self._make_request("POST", f"/{deposition_id}/actions/publish")
            logger.info("Deposition published successfully")

        except Exception as e:
            logger.error("Publication failed: %s", str(e))
            raise
