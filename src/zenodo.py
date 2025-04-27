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

    def _find_existing_deposition(self) -> Optional[tuple[str, str]]:
        """Find existing deposition by DOI or version."""

        try:
            depositions = self._make_request("GET", "")
            target_doi = self.metadata.get("doi")
            target_version = str(self.metadata.get("version", ""))

            for dep in depositions:
                meta = dep.get("metadata", {})

                if meta.get("doi") == target_doi:
                    return dep["conceptrecid"], dep["id"]

                if str(meta.get("version")) == target_version:
                    return dep["conceptrecid"], dep["id"]

            return None

        except Exception as e:
            logger.warning("Deposition search failed: %s", str(e))
            return None

    def create_version(self) -> str:
        """Smart version creation with fallback to new deposition."""

        try:
            # Try to find existing deposition
            existing = self._find_existing_deposition()

            if existing:
                concept_id, deposition_id = existing
                logger.info("Found existing concept %s", concept_id)
                logger.info("Found existing deposition %s", deposition_id)

                try:
                    response = self._make_request("POST", f"/{concept_id}/actions/newversion")
                    return response["id"]

                except requests.HTTPError as e:
                    if e.response.status_code == 404:
                        logger.warning("Concept not found, creating new deposition")
                        return self._create_new_deposition()
                    raise

            return self._create_new_deposition()

        except Exception as e:
            logger.error("Version creation failed: %s", str(e))
            raise

    def _create_new_deposition(self) -> str:
        """Create brand new deposition with metadata."""

        deposition_data = {
            "metadata": {
                "title": self.metadata["title"],
                "upload_type": "software",
                "description": self.metadata.get("abstract", ""),
                "creators": [
                    {"name": f"{a['family-names']}, {a['given-names']}"}
                    for a in self.metadata.get("authors", [])
                ],
                "license": {"id": self.metadata.get("license", "")},
                "keywords": self.metadata.get("keywords", []),
                "version": self.metadata.get("version", "1.0.0")
            }
        }
        response = self._make_request("POST", "", json=deposition_data)
        logger.info("Created new deposition %s", response["id"])
        return response["id"]

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

    def update_metadata(self, deposition_id: str) -> None:
        """Update deposit metadata."""
        logger.info("Updating metadata for deposition: %s", deposition_id)

        try:
            metadata = self.metadata
            new_metadata = {}

            # (existing metadata processing code)
            logger.debug("Prepared metadata update: %s", new_metadata)
            self._make_request("PUT", f"/{deposition_id}", json={"metadata": new_metadata})
            logger.info("Metadata updated successfully")

        except Exception as e:
            logger.error("Metadata update failed: %s", str(e))
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
