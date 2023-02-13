from pathlib import Path
from boto3.session import Session
from typing import Tuple, NoReturn
from botocore.client import BaseClient


def create_s3_client(AWS_ACCESS_KEY, AWS_ACCESS_SECRET) -> BaseClient:
    """Creates an Amazon S3 client.

    Args:
        AWS_ACCESS_KEY (str): AWS access key for session creation
        AWS_ACCESS_SECRET (str): AWS access secret for session creation

    Returns:
        s3_client: botocore.client.s3
    """
    session = Session(
        aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_ACCESS_SECRET
    )
    s3_client = session.client("s3")
    return s3_client


def get_file_folders(s3_client, bucket_name, prefix="") -> Tuple[list, list]:
    """Gets list of all files and folders within a bucket filtered by a prefix string.

    Args:
        s3_client (botocore.client.s3): Amazon S3 client for interaction with specified bucket
        bucket_name (str): Name of Amazon S3 bucket with the files
        prefix (str, optional): A prefix to be used as a filter when selecting files. Defaults to "".

    Returns:
        file_names (list): A list of files selected using the prefix filter and bucket name
        folders (list): A list of folder like objects
    """
    file_names = []
    folders = []

    default_kwargs = {"Bucket": bucket_name, "Prefix": prefix}
    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token

        response = s3_client.list_objects_v2(**updated_kwargs)
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            if key[-1] == "/":
                folders.append(key)
            else:
                file_names.append(key)

        next_token = response.get("NextContinuationToken")

    return file_names, folders


def download_files(s3_client, bucket_name, local_path, file_names) -> None:
    """Creates folder structure based on the prefixes of Amazon S3 objects and downloads objects to it.

    Args:
        s3_client (botocore.client.s3): Amazon S3 client for interaction with specified bucket
        bucket_name (str): Name of Amazon S3 bucket with the files
        local_path (Path): Path where the folders will be created and objects downloaded
        file_names (list): A list of files in the bucket to be downloaded
        folders (list): A list of folders to be created
    """
    local_path = Path(local_path)

    for file_name in file_names:
        file_name_prefix_sanitized = file_name.replace("/", "_")
        file_path = Path.joinpath(local_path, file_name_prefix_sanitized)
        # Create folder for parent directory
        file_path.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket_name, file_name, str(file_path))
