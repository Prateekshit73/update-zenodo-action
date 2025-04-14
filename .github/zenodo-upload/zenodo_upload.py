import logging
import os
import re
import time
import yaml
from urllib.parse import urljoin
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ZenodoPublisher:
    """Handle Zenodo publications with dynamic configuration"""

    def __init__(self):
        self.base_url = os.getenv("FILENAMES")
        self.version = self._get_normalized_version()
        self.zenodo_token = os.getenv("ZENODO_TOKEN")
        self.filenames = os.getenv("FILENAMES", "").split()
        self.metadata = self._load_citation_metadata()
        self.session = requests.Session()

    def _get_normalized_version(self) -> str:
        """Get version from GitHub tag or CITATION.cff"""
        tag_ref = os.getenv("GITHUB_REF", "")
        if version := re.sub(r'^refs/tags/v?', '', tag_ref):
            return version
        return self.metadata.get("version", "0.0.0").lstrip("v")

    def _load_citation_metadata(self) -> dict:
        """Load metadata from CITATION.cff if available"""
        try:
            with open("CITATION.cff", "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Couldn't load CITATION.cff: {str(e)}")
            return {}

    def _zenodo_request(self, method, endpoint, **kwargs):
        """Universal Zenodo API handler"""
        url = f"https://zenodo.org/api/deposit/depositions{endpoint}"
        headers = kwargs.pop("headers", {"Content-Type": "application/json"})
        params = {"access_token": self.zenodo_token}

        for attempt in range(3):
            try:
                response = requests.request(
                    method, url, headers=headers, params=params, **kwargs
                )
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < 2:
                    logger.warning(f"Retrying {method} {endpoint} (attempt {attempt + 1})")
                    time.sleep(5)
                else:
                    raise

    def _update_metadata(self, deposition_id):
        """Update Zenodo metadata from CITATION.cff"""
        metadata = {
            "metadata": {
                "title": self.metadata.get("title", f"Holidays v{self.version}"),
                "upload_type": "software",
                "description": self.metadata.get("abstract", ""),
                "creators": [
                    {"name": f"{author['family-names']}, {author['given-names']}"}
                    for author in self.metadata.get("authors", [])
                ],
                "keywords": self.metadata.get("keywords", []),
                "license": {"id": self.metadata.get("license", "mit")},
                "version": f"v{self.version}",
                "publication_date": self.metadata.get("date-released", ""),
                "custom": {
                    "code:codeRepository": self.metadata.get(
                        "repository-code",
                        f"https://github.com/{os.getenv('GITHUB_REPOSITORY', 'vacanza/holidays')}"
                    )
                }
            }
        }
        self._zenodo_request("PUT", f"/{deposition_id}", json=metadata)

    def _handle_artifacts(self, deposition_id):
        """Process file uploads from environment variable"""
        # Clear existing files
        for file in self._zenodo_request("GET", f"/{deposition_id}/files").json():
            self._zenodo_request("DELETE", f"/{deposition_id}/files/{file['id']}")

        # Upload new files
        for artifact in self.filenames:
            artifact_name = os.path.basename(artifact)
            download_url = urljoin(f"{self.base_url}/v{self.version}/", artifact_name)

            with self.session.get(download_url, stream=True) as response:
                response.raise_for_status()
                self._zenodo_request(
                    "POST", f"/{deposition_id}/files",
                    files={"file": (artifact_name, response.content)}
                )
                logger.info(f"Uploaded {artifact_name}")

    def publish(self):
        """Main publication workflow"""
        if not self.zenodo_token:
            logger.warning("Skipping Zenodo upload - no token provided")
            return

        try:
            # Create new deposition
            response = self._zenodo_request("POST", "")
            deposition_id = response.json()["id"]

            self._update_metadata(deposition_id)
            self._handle_artifacts(deposition_id)

            # Final publish
            self._zenodo_request("POST", f"/{deposition_id}/actions/publish")
            logger.info("Successfully published to Zenodo")

        except Exception as e:
            logger.error(f"Publication failed: {str(e)}")
            raise


if __name__ == "__main__":
    ZenodoPublisher().publish()
