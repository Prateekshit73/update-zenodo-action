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
import time
import requests
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CONFIG = {
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 5
}


class ZenodoUploader:
    """Manage Zenodo depositions"""

    def __init__(self):
        self.zenodo_token = os.getenv("ZENODO_TOKEN")
        self.sandbox = os.getenv("ZENODO_SANDBOX", "false").lower() == "true"
        self.base_url = "https://sandbox.zenodo.org" if self.sandbox else "https://zenodo.org"
        self.citation = self._parse_citation()
        self.version = self.citation["version"].lstrip('v')  # Remove 'v' prefix for Zenodo

    def _zenodo_operation(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Perform Zenodo API operation with retries"""
        url = f"{self.base_url}/api/deposit/depositions{endpoint}"
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
                logger.error(f"Response content: {response.text}")
                if attempt < CONFIG["MAX_RETRIES"] - 1:
                    logger.warning(f"Retrying {method} {url} (attempt {attempt + 1})")
                    time.sleep(CONFIG["RETRY_DELAY"])
                else:
                    logger.error(f"Zenodo operation failed: {str(e)}")
                    raise

    def _parse_citation(self) -> dict:
        """Parse CITATION.cff file for metadata"""
        try:

            with open("CITATION.cff", "r") as f:
                citation = yaml.safe_load(f)

            logger.info(f"Successfully parsed CITATION.cff")

            # Validate required fields
            required_fields = ["title", "authors", "license", "version"]
            for field in required_fields:
                if field not in citation:
                    raise ValueError(f"Missing required field '{field}' in CITATION.cff")
            return {
                "title": citation["title"],
                "description": citation.get("abstract", ""),
                "creators": [{"name": f"{a['given-names']} {a['family-names']}".strip()}
                             for a in citation["authors"]],
                "license": citation["license"].lower(),
                "keywords": citation.get("keywords", []),
                "version": citation["version"],
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

            response = self._zenodo_operation("GET", "")
            depositions = response.json()

            # Find existing deposition by DOI or version
            concept_id = None
            deposition_id = None
            for dep in depositions:
                metadata = dep.get("metadata", {})
                if metadata.get("doi") == self.citation["doi"]:
                    concept_id = dep["conceptrecid"]
                    deposition_id = dep["id"]
                    break
                elif metadata.get("version") == self.version:
                    concept_id = dep["conceptrecid"]
                    deposition_id = dep["id"]
                    break
            # Prepare deposition data
            deposition_data = {
                "metadata": {
                    "title": self.citation["title"],
                    "version": self.version,
                    "upload_type": "software",
                    "description": self.citation["description"],
                    "creators": self.citation["creators"],
                    "license": self.citation["license"],
                    "keywords": self.citation["keywords"],
                    "doi": self.citation["doi"]
                }
            }
            # Handle version creation or new deposition
            if concept_id:
                logger.info(f"Creating new version for concept {concept_id}")
                try:
                    response = self._zenodo_operation("POST", f"/{concept_id}/actions/newversion")
                    deposition_id = response.json()["links"]["latest_draft"].split("/")[-1]
                except requests.HTTPError as e:
                    if e.response.status_code == 404:
                        logger.warning("Concept not found, creating new deposition")
                        response = self._zenodo_operation("POST", "", json=deposition_data)
                        deposition_id = response.json()["id"]
                        concept_id = response.json()["conceptrecid"]
                    else:
                        raise
            else:
                logger.info("Creating new Zenodo record")
                response = self._zenodo_operation("POST", "", json=deposition_data)
                deposition_id = response.json()["id"]
                concept_id = response.json()["conceptrecid"]
            # Upload files
            for artifact in files:
                try:
                    with open(artifact, "rb") as f:
                        self._zenodo_operation(
                            "POST",
                            f"/{deposition_id}/files",
                            files={"file": (os.path.basename(artifact), f)},
                            headers={}
                        )
                    logger.info(f"Uploaded {artifact}")

                except Exception as e:
                    logger.error(f"Failed to upload {artifact}: {str(e)}")
                    raise

            # Publish deposition
            self._zenodo_operation("POST", f"/{deposition_id}/actions/publish")
            logger.info("Zenodo update completed successfully")

        except Exception as e:
            logger.error(f"Zenodo update failed: {str(e)}")
            raise


def main() -> None:
    """Main execution flow"""
    try:
        uploader = ZenodoUploader()
        files = [f for f in os.getenv("FILES", "").split() if os.path.exists(f)]
        if not files:
            logger.error("No valid files provided for upload.")
            return
        logger.debug(f"Processing files: {files}")
        uploader.update_zenodo(files)

        logger.info("All operations completed successfully")

    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
