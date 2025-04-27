import os

from zenodo import ZenodoAPI


def main():
    token = os.getenv("ZENODO_TOKEN")
    concept_id = os.getenv("CONCEPT")
    filenames = os.getenv("FILENAMES")
    metadata_file = os.getenv("METADATA")
    sandbox = os.getenv("SANDBOX")
    publish = os.getenv("PUBLISH")

    api = ZenodoAPI(token, sandbox.lower() == "true", metadata_file)

    new_ver_id = api.create_version(concept_id)
    api.update_metadata(new_ver_id)
    api.delete_files(new_ver_id)
    api.upload_files(new_ver_id, filenames.split(" "))
    if publish.lower() == "true":
        api.publish_version(new_ver_id)


if __name__ == "__main__":
    main()
