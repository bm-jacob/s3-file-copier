# s3-file-copy.py

> A Python script to copy files between buckets in different environments (profiles)


## Functionality

- Copy from one bucket to another in **same or different AWS environments**
- Copy from one bucket to local folder
- Filter with regex matching for S3 key (filename)
- Filter by S3 updatedLast time (the file PutObject time when versioning is disabled)
- Append prefixes to copied files

## Logging

Logs to console and ./file_copy.log

## Usage

```text
usage: s3-copy.py 

Copy files from one S3 bucket to another or download to local folder with filtering and logging.

options:
  -h, --help            show this help message and exit
  --source-bucket SOURCE_BUCKET
                        The source S3 bucket name.
  --destination-bucket DESTINATION_BUCKET
                        The destination S3 bucket name (optional).
  --source-profile SOURCE_PROFILE
                        The AWS profile name for the source bucket.
  --destination-profile DESTINATION_PROFILE
                        The AWS profile name for the destination bucket (optional).
  --key-pattern KEY_PATTERN
                        Regex pattern to filter keys (default: '.*').
  --start-time START_TIME
                        Start time for LastModified filtering (e.g., '2024-01-01').
  --end-time END_TIME   End time for LastModified filtering (optional, defaults to now).
  --prefix PREFIX       Prefix to add to the destination keys.
  --destination-folder DESTINATION_FOLDER
                        Folder to download files to (if not provided, will not download files locally).
  --dry-run             If specified, only fetch and print the files that would be copied or downloaded.
  --region REGION       AWS region name (default: 'us-east-1').
  --max-concurrency MAX_CONCURRENCY
                        Maximum number of concurrent tasks (default: 10).
```

## Examples

### Dry run (query what will be copied over)

> Use `--dry-run` to check what files will be copied after applying the datetime and key filters
> This will not copy/download any files.
> Remove `--dry-run` from examples to execute the copying.

#### Query all files from 2024-10-01 in bucket-1 in default (current) profile

```bash
python s3-copy.py \
--source-bucket "bucket-1" \
--start-time "2024-10-01" \
--dry-run
```

Note: the output of the above --dryrun is `{{S3 UpdatedLast timestamp}}\t{{S3 Key(filename with relative path)}}`

```csv
2024-10-28 08:39:10+00:00       900
2024-10-28 08:39:10+00:00       901
2024-10-28 08:39:10+00:00       902
2024-10-28 08:39:10+00:00       903
2024-10-28 08:39:09+00:00       904
2024-10-28 08:39:09+00:00       905
2024-10-28 08:39:09+00:00       906
```

### Copy from one bucket to another in default/current AWS profile, select custom region

```bash
 python s3-copy.py \ 
--source-bucket "bucket-1" \
--destination-bucket "bucket-2" \
--start-time "2024-10-01" \
--region "eu-central-1"
--dry-run
```

### Copy from one bucket to another in *different* AWS profiles in default region (eu-central-1)

```bash
 python s3-copy.py \ 
--source-bucket "bucket-1" \
--source-profile "profile-1" \
--destination-bucket "bucket-2" \
--destination-profile "profile-2" \
--start-time "2024-10-01"
--dry-run
```

## Download files from a bucket to local folder

Note: used with `--destination-bucket`  download and copy to bucket are executed in same run

```bash
 python s3-copy.py \ 
--source-bucket "bucket-1" \
--source-profile "profile-1" \
--start-time "2024-10-01" \
--destination-folder "./tmp"
--dry-run
```

## Download , filter precise start-time & end-time timestamps and regex for key matching

```bash
python s3-copy.py \
--source-bucket "bucket-1" \
--source-profile "profile-1" \
--start-time "2024-10-28 08:39:28+00:00" \
--end-time "2024-10-28 08:39:30+00:00" \
--region "eu-central-1" \
--key-pattern "^9\d+" \
--dry-run
```

Note:

- `start-time`, `end-time` are inclusive ranges, if no TZ offset provided UTC will be assumed.
- `end_date` is assumed Now, unless otherwise defined in request

### Examples of accepted datetime formats

- `2024-01-01`
- `2024-01-01 22:00:00` (assumes UTC offset +00:00)
- `2024-01-01 22:00:00+00:00` (or any other offset)
- `yesterday`, `two days ago`, `1 min ago`, `2 weeks ago`, `3 months, 1 week and 1 day ago`

## Full example

- Copy from bucket-1 in profile-1 to bucket-2 in profile-2
- Filter by start-time & end-time timestamps
- Filter regex for key/file matching
- Add prefix to each file copied to bucket-2
- Download files to local folder as well
- Run 25 concurent i/o threads

```bash
 python s3-copy.py \ 
--source-bucket "bucket-1" \
--destination-bucket "bucket-2" \
--source-profile "profile-1" \
--destination-profile "profile-2" \
--destination-folder "./tmp" \
--start-time "five days ago" \
--end-time 'yesterday'
--prefix "prefix/" \
--key-pattern "^StatusUpdate__\d{3}__" \
--region "eu-central-1" \ 
--max-concurrency 25 / 
--dry-run
```
