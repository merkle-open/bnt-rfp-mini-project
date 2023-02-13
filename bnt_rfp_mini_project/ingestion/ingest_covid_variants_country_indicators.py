import configparser
from pathlib import Path
from typing import Tuple
from bnt_rfp_mini_project.ingestion.connectors.s3 import (
    create_s3_client,
    get_file_folders,
    download_files,
)
from bnt_rfp_mini_project.transformation.utils.create_logger import create_logger
from bnt_rfp_mini_project.conventions import data_raw_path, path_to_configs


logger = create_logger(name=Path(__file__).name)


def get_credentials() -> Tuple[str, str, str, str]:
    """Loads Amazon S3 credentials from the config/credentials.ini file and parses them to constant variables.

    Returns:
        BUCKET_NAME (str): source S3 bucket
        AWS_ACCESS_KEY (str): AWS access key for session creation
        AWS_ACCESS_SECRET (str): AWS access secret for session creation
        HOST_AWS (str): AWS hostname
    """
    credentials_ini = Path(path_to_configs, "credentials.ini")
    logger.debug(f"Starting parsing `{credentials_ini}`")
    config = configparser.ConfigParser()
    config.read(credentials_ini)
    BUCKET_NAME = config["aws_s3"]["bucket_name"]
    AWS_ACCESS_KEY = config["aws_s3"]["aws_access_key"]
    AWS_ACCESS_SECRET = config["aws_s3"]["aws_access_secret"]
    return BUCKET_NAME, AWS_ACCESS_KEY, AWS_ACCESS_SECRET


def persist_files_to_raw(
    raw_category_path: str,
) -> None:
    """Procedure writes loads AWS credentials from the credentials.ini config file

    It uses the credentials to authenticate to AWS and downloads the files
    from the Amazon S3 specified by credentials to the raw layer

    Args:
        raw_category_path (str): mandatory domain/subfolder for organizing the downloaded data files
    """
    # Loading files from the credentials.ini file.
    (BUCKET_NAME, AWS_ACCESS_KEY, AWS_ACCESS_SECRET) = get_credentials()

    # Creating a S3 client from the credentials.
    s3_client = create_s3_client(AWS_ACCESS_KEY, AWS_ACCESS_SECRET)

    # Getting objects metadata from S3
    file_names, _ = get_file_folders(s3_client=s3_client, bucket_name=BUCKET_NAME)
    logger.debug(f"Found files in S3 bucket `{BUCKET_NAME}: {file_names}")
    logger.info("Start downloading files from S3")
    download_files(
        s3_client=s3_client,
        bucket_name=BUCKET_NAME,
        local_path=Path(data_raw_path, raw_category_path),
        file_names=file_names,
    )
    logger.info("Download finished")


if __name__ == "__main__":
    persist_files_to_raw(raw_category_path="covid_variants_country_indicators")
