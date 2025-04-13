#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see AUTHORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

import logging
import os
import re
import time
from urllib.parse import urljoin
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "REPO_OWNER": "vacanza",
    "REPO_NAME": "holidays",
    "GITHUB_API": "https://api.github.com/repos",
    "BASE_DOWNLOAD_URL": "https://github.com/vacanza/holidays/releases/download",
    "ZENODO_API": "https://zenodo.org/api/deposit/depositions",
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 5
}


class PublicReleaseDownloader:
    """Manage public GitHub releases and Zenodo depositions"""

    def __init__(self):
        self.version = self._get_latest_version()
        self.session = requests.Session()
        self.zenodo_token = os.getenv("ZENODO_TOKEN")

    def _get_latest_version(self) -> str:
        """Get latest release version from GitHub API without authentication"""
        try:
            url = f"{CONFIG['GITHUB_API']}/{CONFIG['REPO_OWNER']}/{CONFIG['REPO_NAME']}/releases/latest"
            response = requests.get(url)
            response.raise_for_status()

            tag_name = response.json()["tag_name"]
            version = re.sub(r'^v', '', tag_name)  # Normalize version
            logger.info(f"Detected latest version: {version}")
            return version

        except requests.RequestException as e:
            logger.error(f"Failed to fetch releases: {str(e)}")
            raise
        except KeyError:
            logger.error("Unexpected API response format")
            raise

    def _zenodo_operation(self, method: str, url: str, **kwargs) -> requests.Response:
        """Perform Zenodo API operation with retries"""
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                response = requests.request(
                    method,
                    url,
                    params={"access_token": self.zenodo_token},
                    headers={"Content-Type": "application/json"},
                    **kwargs
                )
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < CONFIG["MAX_RETRIES"] - 1:
                    logger.warning(f"Retrying {method} {url} (attempt {attempt + 1})")
                    time.sleep(CONFIG["RETRY_DELAY"])
                else:
                    logger.error(f"Zenodo operation failed: {str(e)}")
                    raise

    def download_artifact(self, artifact_name: str) -> str:
        """Download release artifact from public repository"""
        try:
            url = urljoin(
                f"{CONFIG['BASE_DOWNLOAD_URL']}/v{self.version}/",
                artifact_name
            )

            logger.info(f"Downloading {artifact_name}")
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()

            local_path = f"holidays-{self.version}{os.path.splitext(artifact_name)[1]}"

            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        f.write(chunk)

            logger.info(f"Successfully downloaded {local_path}")
            return local_path

        except requests.RequestException as e:
            logger.error(f"Download failed: {str(e)}")
            raise

    def update_zenodo(self) -> None:
        """Update Zenodo deposition with new version"""
        if not self.zenodo_token:
            logger.warning("Zenodo token not found, skipping Zenodo update")
            return

        try:
            # Get existing deposition
            response = self._zenodo_operation("GET", CONFIG["ZENODO_API"])

            # Find latest concept ID for versioning
            concept_id = next(
                (dep["conceptrecid"] for dep in response.json()
                 if dep["metadata"]["version"] == self.version),
                None
            )

            if concept_id:
                logger.info(f"Updating existing Zenodo record {concept_id}")
                response = self._zenodo_operation(
                    "POST",
                    f"{CONFIG['ZENODO_API']}/{concept_id}/actions/newversion"
                )
                deposition_id = response.json()["id"]
            else:
                logger.info("Creating new Zenodo record")
                response = self._zenodo_operation(
                    "POST", CONFIG["ZENODO_API"],
                    json={
                        "metadata": {
                            "title": f"Holidays {self.version}",
                            "version": self.version,
                            "upload_type": "software",
                            "description": "Country-specific holiday management library",
                            "creators": [{
                                "name": "Vacanza Team",
                                "affiliation": "Open Source Community"
                            }]
                        }
                    }
                )
                deposition_id = response.json()["id"]

            # Upload artifacts
            artifacts = [
                f"holidays-{self.version}-sbom.json",
                f"v{self.version}.tar.gz",
                f"v{self.version}.whl"
            ]

            for artifact in artifacts:
                local_path = self.download_artifact(artifact)
                self._zenodo_operation(
                    "POST",
                    f"{CONFIG['ZENODO_API']}/{deposition_id}/files",
                    files={"file": open(local_path, "rb")}
                )
                os.remove(local_path)  # Cleanup

            # Publish deposition
            self._zenodo_operation(
                "POST",
                f"{CONFIG['ZENODO_API']}/{deposition_id}/actions/publish"
            )
            logger.info("Zenodo update completed successfully")

        except Exception as e:
            logger.error(f"Zenodo update failed: {str(e)}")
            raise


def main() -> None:
    """Main execution flow"""
    try:
        downloader = PublicReleaseDownloader()

        # Download standard artifacts
        artifacts = [
            f"holidays-{downloader.version}-sbom.json",  # SBOM file
            f"holidays-{downloader.version}.tar.gz",      # Tarball
            f"holidays-{downloader.version}-py3-none-any.whl"  # Wheel file
        ]

        for artifact in artifacts:
            downloader.download_artifact(artifact)

        # Update Zenodo if token is available
        downloader.update_zenodo()

        logger.info("All operations completed successfully")

    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()