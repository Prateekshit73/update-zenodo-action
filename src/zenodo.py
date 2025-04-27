import logging
import os
from typing import Any

import requests
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# Initialize logger
logger = logging.getLogger(__name__)
ZENODO_URL = "https://zenodo.org"
ZENODO_SANDBOX_URL = "https://sandbox.zenodo.org"


class ZenodoAPI:
    """Client for interacting with the Zenodo API."""

    def __init__(self, auth_token: str, sandbox: bool = False, metadata_file: str | None = None):
        self.base_url = f"{ZENODO_SANDBOX_URL if sandbox else ZENODO_URL}/api/deposit/depositions"
        self.headers = {
            "Authorization": f"Bearer {auth_token}",

        }
        self.matadata = {}  # Initialize metadata as empty dict

        # NEW: Log initialization parameters (redacting auth token)
        logger.debug("Initializing ZenodoAPI with sandbox=%s", sandbox)
        logger.debug("Base URL: %s", self.base_url)

        if metadata_file:

            try:
                logger.info("Loading metadata from: %s", metadata_file)

                with open(metadata_file, encoding="utf-8") as f:
                    self.matadata = yaml.safe_load(f)
                logger.debug("Loaded metadata: %s", self.matadata)

            except Exception as e:
                logger.error("Failed to load metadata file: %s", str(e))
                raise

    def _make_request(
            self,
            method: str,
            endpoint: str,
            json: dict | None = None,
            files: dict | None = None,
    ) -> Any:

        """Perform an HTTP request to the Zenodo API."""
        url = f"{self.base_url}{endpoint}"

        # NEW: Log request details
        logger.debug("Making %s request to: %s", method, url)
        logger.debug("Headers: %s", {k: "***" if k == "Authorization" else v for k, v in self.headers.items()})

        if json:
            logger.debug("Request JSON: %s", json)

        if files:
            logger.debug("Files to upload: %s", list(files.keys()))

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=json,
                files=files,
            )
            response.raise_for_status()
            # NEW: Log response details
            logger.debug("Response status: %s", response.status_code)
            logger.debug("Response content: %s", response.text[:200])  # Truncate long responses
            return response.json() if response.content and method != "DELETE" else None


        except requests.HTTPError as e:
            logger.error("HTTP error: %s", str(e))
            logger.error("Response content: %s", e.response.text[:200])
            raise

        except Exception as e:
            logger.error("Request failed: %s", str(e))
            raise

    def create_concept(self) -> str:
        """Create a new concept."""
        logger.info("Creating new concept")

        try:
            response = self._make_request("POST", "")
            new_deposition_id = response["id"]
            logger.info("Created new concept with ID: %s", new_deposition_id)
            return new_deposition_id

        except KeyError as e:
            logger.error("Missing expected field in response: %s", str(e))
            raise

        except Exception as e:
            logger.error("Failed to create concept: %s", str(e))
            raise

    def create_version(self, concept_id: str | None = None) -> str:
        """Create a new concept version."""

        try:
            concept_id = concept_id or self.matadata["conceptrecid"]
            logger.debug("Derived concept ID: %s", concept_id)
            logger.info("Creating new version for concept ID: %s", concept_id)
            response = self._make_request("GET", f"?q=conceptrecid:{concept_id}")
            logger.debug("Version creation response: %s", response)

            if not response:  # NEW: Add validation
                logger.error("No existing versions found for concept ID: %s", concept_id)
                raise ValueError("No existing versions found")

            deposition_id = self.matadata["doi"].split(".")[-1]
            logger.debug("Found existing deposition ID: %s", deposition_id)
            response = self._make_request("POST", f"/{deposition_id}/actions/newversion")
            new_deposition_id = response["id"]
            logger.info("Created new version with ID: %s", new_deposition_id)
            return new_deposition_id

        except (KeyError, IndexError) as e:
            logger.error("Invalid response structure: %s", str(e))
            raise

        except Exception as e:
            logger.error("Version creation failed: %s", str(e))
            raise

    def update_metadata(self, deposition_id: str) -> None:
        """Update deposit metadata."""
        logger.info("Updating metadata for deposition: %s", deposition_id)

        try:
            metadata = self.matadata
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
