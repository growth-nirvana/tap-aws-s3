import io
import csv
import pandas as pd
import boto3
import zipfile
import typing as t
from datetime import datetime, timezone
from botocore.config import Config
import logging

import pendulum
from singer_sdk import typing as th
from singer_sdk.streams import Stream

logger = logging.getLogger(__name__)

class S3CSVStream(Stream):
    name = "s3_csv_stream"
    replication_key = "last_modified"

    def __init__(self, tap, **kwargs):
        super().__init__(tap, **kwargs)

    @property
    def bucket_name(self) -> str:
        return self.config["bucket_name"]

    @property
    def aws_access_key_id(self) -> str:
        return self.config["aws_access_key_id"]

    @property
    def aws_secret_access_key(self) -> str:
        return self.config["aws_secret_access_key"]

    @property
    def region_name(self) -> str:
        return self.config["region_name"]

    @property
    def prefix(self) -> str:
        return self.config.get("prefix", "")

    @property
    def start_date(self):
        return self.config.get("start_date")

    @property
    def schema(self) -> dict:
        sample_file = self._get_sample_csv_key()
        if not sample_file:
            self.logger.warning("No sample file found. Using fallback schema.")
            return th.PropertiesList(
                th.Property("last_modified", th.StringType),
            ).to_dict()

        df = self._download_csv_to_df(sample_file)
        properties = [
            th.Property(col, th.StringType) for col in df.columns
        ]
        properties.append(th.Property("last_modified", th.StringType))
        return th.PropertiesList(*properties).to_dict()

    def _download_csv_to_df(self, s3_key: str) -> pd.DataFrame:
        s3_client = self._get_s3_client()
        response = s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)

        if s3_key.endswith('.zip'):
            with io.BytesIO(response['Body'].read()) as zip_buffer:
                with zipfile.ZipFile(zip_buffer, 'r') as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        raise Exception(f"No CSV files found in ZIP: {s3_key}")
                    df_list = [pd.read_csv(z.open(csv_file)) for csv_file in csv_files]
                    df = pd.concat(df_list, ignore_index=True)
        else:
            df = pd.read_csv(io.BytesIO(response['Body'].read()))

        return df

    def _get_s3_client(self):
        s3_config = Config(read_timeout=300, retries={"max_attempts": 10})
        return boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
            config=s3_config
        )

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        bucket = self.config["bucket_name"]
        prefix = self.config.get("prefix", "")
        start_date = pendulum.parse(self.config.get("start_date")) if self.config.get("start_date") else None

        paginator = self._get_s3_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                last_modified = obj["LastModified"]

                # Filter files by start_date
                if start_date and last_modified <= start_date:
                    continue

                self.logger.info(f"📥 Downloading {key}")
                s3_obj = self._get_s3_client().get_object(Bucket=bucket, Key=key)

                if key.endswith(".zip"):
                    yield from self._process_zip(s3_obj["Body"], last_modified)
                elif key.endswith(".csv"):
                    yield from self._process_csv(s3_obj["Body"], last_modified)

    def _process_zip(self, body, last_modified: datetime) -> t.Iterable[dict]:
        with io.BytesIO(body.read()) as bio:
            with zipfile.ZipFile(bio) as z:
                for file_name in z.namelist():
                    if not file_name.endswith(".csv"):
                        continue
                    with z.open(file_name) as csvfile:
                        yield from self._iterate_csv(csvfile, last_modified)

    def _process_csv(self, body, last_modified: datetime) -> t.Iterable[dict]:
        yield from self._iterate_csv(body, last_modified)

    def _iterate_csv(self, file_obj, last_modified: datetime) -> t.Iterable[dict]:
        text_stream = io.TextIOWrapper(file_obj, encoding="utf-8")
        reader = csv.DictReader(text_stream)
        for row in reader:
            # Ensure all values are strings or None
            clean_row = {k: str(v) if v is not None else None for k, v in row.items()}
            clean_row["last_modified"] = last_modified.isoformat()
            yield clean_row

    def _get_sample_csv_key(self) -> str | None:
        """Pick a sample CSV file to generate the schema."""
        bucket = self.config["bucket_name"]
        prefix = self.config.get("prefix", "")
        paginator = self._get_s3_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    return obj["Key"]
        return None
