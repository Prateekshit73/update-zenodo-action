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
import requests
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "ZENODO_API": os.getenv("ZENODO_API", "https://zenodo.org/api/deposit/depositions"),
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 5
}


class ZenodoUploader:
    """Manage Zenodo depositions"""

    def __init__(self):
        self.zenodo_token = os.getenv("ZENODO_TOKEN")
        self.version = self._get_latest_version()
        self.zenodo_version = f"v{self.version}"  # Maintain 'v' prefix for Zenodo

    def _get_latest_version(self) -> str:
        """Get the latest version from the environment variable or other source"""
        # Assuming version is passed as an environment variable for CI/CD
        return os.getenv("VERSION", "0.0.0")

    def _zenodo_operation(self, method: str, url: str, **kwargs) -> requests.Response:
        """Perform Zenodo API operation with retries"""
        headers = kwargs.pop("headers", {"Content-Type": "application/json"})

        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                response = requests.request(
                    method,
                    url,
                    params={"access_token": self.zenodo_token},
                    headers=headers,
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

    def _parse_citation(self) -> dict:
        """Parse CITATION.cff file for metadata"""
        try:
            cff_path = "CITATION.cff"  # Assuming CITATION.cff is in the current directory
            logger.info(f"Looking for CITATION.cff at: {cff_path}")

            if not os.path.exists(cff_path):
                raise FileNotFoundError(f"CITATION.cff not found at {cff_path}")

            with open(cff_path, "r") as f:
                citation = yaml.safe_load(f)

            logger.info(f"Successfully parsed CITATION.cff from {cff_path}")
            return {
                "title": citation.get("title", f"Holidays {self.zenodo_version}"),
                "description": citation.get("abstract", "Country-specific holiday management library"),
                "creators": [{"name": f"{a['given-names']} {a['family-names']}".strip()}
                             for a in citation.get("authors", [])],
                "license": citation.get("license", "mit").lower(),
                "keywords": citation.get("keywords", []),
                "doi": citation.get("doi", "")
            }
        except Exception as e:
            logger.error(f"Failed to process CITATION.cff: {str(e)}")
            raise

    def update_zenodo(self, files: list) -> None:
        """Update Zenodo deposition with new version"""
        if not self.zenodo_token:
            logger.warning("Zenodo token not found, skipping Zenodo update")
            return

        try:
            citation_meta = self._parse_citation()
            response = self._zenodo_operation("GET", CONFIG["ZENODO_API"])
            depositions = response.json()

            # Find existing deposition by DOI or version
            concept_id = None
            deposition_id = None
            for dep in depositions:
                if dep.get("metadata", {}).get("doi") == citation_meta["doi"]:
                    concept_id = dep["conceptrecid"]
                    deposition_id = dep["id"]
                    break
                elif dep.get("metadata", {}).get("version") == self.zenodo_version:
                    concept_id = dep["conceptrecid"]
                    deposition_id = dep["id"]
                    break

            # Create new deposition with cff metadata
            logger.info("Creating new Zenodo record")
            deposition_data = {
                "metadata": {
                    "title": citation_meta["title"],
                    "version": self.zenodo_version,
                    "upload_type": "software",
                    "description": citation_meta["description"],
                    "creators": citation_meta["creators"],
                    "license": citation_meta["license"],
                    "keywords": citation_meta["keywords"],
                    "doi": citation_meta["doi"]
                }
            }

            response = self._zenodo_operation(
                "POST", CONFIG["ZENODO_API"],
                json=deposition_data
            )
            deposition_id = response.json()["id"]

            # Upload artifacts with correct filenames
            for artifact in files:
                local_path = artifact  # Assuming files are already available in the current directory
                try:
                    with open(local_path, "rb") as f:
                        self._zenodo_operation(
                            "POST",
                            f"{CONFIG['ZENODO_API']}/{deposition_id}/files",
                            headers={},  # Let requests set Content-Type
                            data={"name": os.path.basename(local_path)},
                            files={"file": (os.path.basename(local_path), f)}
                        )
                except Exception as e:
                    logger.error(f"Failed to upload {local_path}: {str(e)}")
                    raise

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
        uploader = ZenodoUploader()

        # Assuming files are passed as environment variable or directly
        files = os.getenv("FILES", "").split()  # Space-separated list of files
        if not files:
            logger.error("No files provided for upload.")
            return

        uploader.update_zenodo(files)

        logger.info("All operations completed successfully")

    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
