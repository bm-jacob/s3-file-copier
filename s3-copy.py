import argparse
import asyncio
import logging
import os
import re

# import unicodedata
from datetime import datetime, timezone

import aioboto3
import dateparser

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Reduce logging level for botocore to prevent excessive logging
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("aiobotocore").setLevel(logging.WARNING)
logging.getLogger("aioboto3").setLevel(logging.WARNING)

# Set up a separate file logger for copied files
file_logger = logging.getLogger("FileCopyLogger")
file_handler = logging.FileHandler("file_copy.log")
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)
file_logger.addHandler(file_handler)
file_logger.setLevel(logging.INFO)


def safe_filename(filename: str) -> str:
    """Sanitize filename to be safe for local file systems.

    :param filename: The original filename.
    :return: A sanitized version of the filename.
    """
    # Remove any characters that are not allowed in filenames
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)  # Replace invalid characters
    return filename.strip()


async def download_file(
    source_bucket: str, key: str, destination_folder: str, dry_run: bool, s3_client, semaphore: asyncio.Semaphore
) -> None:
    """Download a file from an S3 bucket to a local folder asynchronously.

    :param source_bucket: The source S3 bucket name.
    :param key: The object key to download.
    :param destination_folder: The folder to save the downloaded file.
    :param dry_run: If True, only list the file that would be downloaded.
    :param s3_client: The AWS S3 client.
    :param semaphore: Semaphore to limit concurrency.
    """
    logging.info(f"Preparing to download s3://{source_bucket}/{key}")

    if dry_run:
        print(f"Would download: s3://{source_bucket}/{key}")
        return

    # Create destination folder if it doesn't exist
    os.makedirs(destination_folder, exist_ok=True)

    local_filename = safe_filename(key)
    local_path = os.path.join(destination_folder, local_filename)

    logging.info(f"Downloading s3://{source_bucket}/{key} to {local_path}")

    async with semaphore:
        try:
            await s3_client.download_file(source_bucket, key, local_path)
            logging.info(f"Downloaded: s3://{source_bucket}/{key} to {local_path}")
            file_logger.info(f"Downloaded: s3://{source_bucket}/{key} to {local_path}")
        except Exception as e:
            logging.error(f"Error downloading s3://{source_bucket}/{key}: {str(e)}")


async def copy_file(
    source_bucket: str, destination_bucket: str, key: str, prefix: str, s3_dest_client, semaphore: asyncio.Semaphore
) -> None:
    """Copy a file from the source S3 bucket to the destination S3 bucket asynchronously.

    :param source_bucket: The source S3 bucket name.
    :param destination_bucket: The destination S3 bucket name.
    :param key: The object key to copy.
    :param prefix: The prefix to add to the destination key.
    :param s3_dest_client: The AWS S3 client for the destination.
    :param semaphore: Semaphore to limit concurrency.
    """
    source_key = key
    destination_key = prefix + key

    logging.info(f"Copying s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}")

    async with semaphore:
        try:
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            await s3_dest_client.copy(copy_source, destination_bucket, destination_key)
            logging.info(f"Copied: s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}")
            file_logger.info(
                f"Copied: s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}"
            )
        except Exception as e:
            logging.error(
                f"Error copying s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}: {str(e)}"
            )


async def list_files_with_metadata(
    bucket: str, profile: str, start_time: datetime, end_time: datetime, key_pattern: str, region: str
) -> list:
    """List all files in an S3 bucket with their metadata asynchronously.

    :param bucket: The S3 bucket name.
    :param profile: The AWS profile to use.
    :param start_time: The start time for filtering.
    :param end_time: The end time for filtering.
    :param key_pattern: The regex pattern for filtering keys.
    :param region: AWS region name.
    :return: A list of dictionaries containing object keys and their metadata.
    """
    if profile:
        session = aioboto3.Session(profile_name=profile, region_name=region)
    else:
        session = aioboto3.Session(region_name=region)

    async with session.client("s3") as s3_client:
        paginator = s3_client.get_paginator("list_objects_v2")
        files_metadata = []

        async for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                last_modified = obj["LastModified"]
                key = obj["Key"]

                # Check key against the regex pattern
                if re.search(key_pattern, key):
                    # Ensure last_modified is timezone-aware
                    if last_modified.tzinfo is None:
                        last_modified = last_modified.replace(tzinfo=timezone.utc)
                    else:
                        last_modified = last_modified.astimezone(timezone.utc)

                    # Check LastModified date against the filters
                    if start_time and last_modified < start_time:
                        continue
                    if end_time and last_modified > end_time:
                        continue

                    file_info = {
                        "Key": key,
                        "LastModified": last_modified,
                        "Size": obj["Size"],
                        "StorageClass": obj["StorageClass"],
                    }
                    files_metadata.append(file_info)

        return files_metadata


async def copy_files_between_buckets(
    source_bucket: str,
    destination_bucket: str,
    source_profile: str,
    destination_profile: str,
    key_pattern: str,
    start_time: datetime,
    end_time: datetime,
    prefix: str,
    destination_folder: str,
    dry_run: bool,
    region: str,
    max_concurrency: int,
) -> None:
    """Copy files from one S3 bucket to another and/or download to local folder based on filters.

    :param source_bucket: The source S3 bucket name.
    :param destination_bucket: The destination S3 bucket name (optional).
    :param source_profile: The AWS profile for the source bucket (optional).
    :param destination_profile: The AWS profile for the destination bucket (optional).
    :param key_pattern: The regex pattern for filtering keys.
    :param start_time: The start time for filtering.
    :param end_time: The end time for filtering.
    :param prefix: The prefix to add to the destination keys (if applicable).
    :param destination_folder: The local folder to download files (if applicable).
    :param dry_run: If True, only fetch and print the files that would be copied or downloaded.
    :param region: AWS region name.
    :param max_concurrency: Maximum number of concurrent tasks.
    """
    metadata = await list_files_with_metadata(source_bucket, source_profile, start_time, end_time, key_pattern, region)

    if not metadata:
        logging.info("No files matched the criteria. Exiting.")
        return

    if dry_run:
        # Print out the files that would be copied or downloaded
        for file in metadata:
            print(f"{file['LastModified']}\t{file['Key']}")
        return  # Exit after dry run

    semaphore = asyncio.Semaphore(max_concurrency)  # Control concurrency

    tasks = []

    # Create source session and client
    if source_profile:
        source_session = aioboto3.Session(profile_name=source_profile, region_name=region)
    else:
        source_session = aioboto3.Session(region_name=region)
    source_client = await source_session.client("s3").__aenter__()

    # Handle copying files to destination bucket if specified
    if destination_bucket:
        if destination_profile:
            destination_session = aioboto3.Session(profile_name=destination_profile, region_name=region)
        else:
            destination_session = aioboto3.Session(region_name=region)
        destination_client = await destination_session.client("s3").__aenter__()

        for file in metadata:
            tasks.append(
                copy_file(source_bucket, destination_bucket, file["Key"], prefix, destination_client, semaphore)
            )

    # Handle downloading files to local destination folder if specified
    if destination_folder:
        for file in metadata:
            tasks.append(
                download_file(source_bucket, file["Key"], destination_folder, dry_run, source_client, semaphore)
            )

    # Execute all tasks
    await asyncio.gather(*tasks)

    # Close clients
    await source_client.__aexit__(None, None, None)
    if destination_bucket:
        await destination_client.__aexit__(None, None, None)


async def main(
    source_bucket: str,
    destination_bucket: str,
    source_profile: str,
    destination_profile: str,
    key_pattern: str,
    start_time: str,
    end_time: str,
    prefix: str,
    destination_folder: str,
    dry_run: bool,
    region: str,
    max_concurrency: int,
) -> None:
    # Parse start_time
    start_time_dt = dateparser.parse(start_time)
    if start_time_dt is None:
        raise ValueError(f"Invalid start_time: {start_time}")
    # Ensure start_time_dt is timezone-aware
    if start_time_dt.tzinfo is None:
        start_time_dt = start_time_dt.replace(tzinfo=timezone.utc)
    else:
        start_time_dt = start_time_dt.astimezone(timezone.utc)

    # Parse end_time
    if end_time:
        end_time_dt = dateparser.parse(end_time)
        if end_time_dt is None:
            raise ValueError(f"Invalid end_time: {end_time}")
        if end_time_dt.tzinfo is None:
            end_time_dt = end_time_dt.replace(tzinfo=timezone.utc)
        else:
            end_time_dt = end_time_dt.astimezone(timezone.utc)
    else:
        end_time_dt = datetime.now(timezone.utc)

    await copy_files_between_buckets(
        source_bucket,
        destination_bucket,
        source_profile,
        destination_profile,
        key_pattern,
        start_time_dt,
        end_time_dt,
        prefix,
        destination_folder,
        dry_run,
        region,
        max_concurrency,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copy files from one S3 bucket to another and/or download to local folder with filtering and logging."
    )
    parser.add_argument("--source-bucket", required=True, help="The source S3 bucket name.")
    parser.add_argument("--destination-bucket", help="The destination S3 bucket name (optional).")
    parser.add_argument("--source-profile", help="The AWS profile name for the source bucket (optional).")
    parser.add_argument("--destination-profile", help="The AWS profile name for the destination bucket (optional).")
    parser.add_argument("--key-pattern", default=".*", help="Regex pattern to filter keys (default: '.*').")
    parser.add_argument(
        "--start-time", required=True, help="Start time for LastModified filtering (e.g., '2024-01-01')."
    )
    parser.add_argument("--end-time", help="End time for LastModified filtering (optional, defaults to now).")
    parser.add_argument("--prefix", default="", help="Prefix to add to the destination keys.")
    parser.add_argument(
        "--destination-folder", help="Folder to download files to (if not provided, will not download files locally)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If specified, only fetch and print the files that would be copied or downloaded.",
    )
    parser.add_argument("--region", default="us-east-1", help="AWS region name (default: 'us-east-1').")
    parser.add_argument(
        "--max-concurrency", type=int, default=10, help="Maximum number of concurrent tasks (default: 10)."
    )

    args = parser.parse_args()

    # Use asyncio.run() instead of get_event_loop()
    asyncio.run(
        main(
            source_bucket=args.source_bucket,
            destination_bucket=args.destination_bucket,
            source_profile=args.source_profile,
            destination_profile=args.destination_profile,
            key_pattern=args.key_pattern,
            start_time=args.start_time,
            end_time=args.end_time,
            prefix=args.prefix,
            destination_folder=args.destination_folder,
            dry_run=args.dry_run,
            region=args.region,
            max_concurrency=args.max_concurrency,
        )
    )
