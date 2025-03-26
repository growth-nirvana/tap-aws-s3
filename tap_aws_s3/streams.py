import io
import csv
import boto3
import zipfile
import typing as t
from datetime import datetime
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
        self._s3_object_list = None  # Initialize early to avoid attribute errors
        super().__init__(tap, **kwargs)

    @property
    def bucket_name(self) -> str:
        return self.config["bucket_name"]

    @property
    def prefix(self) -> str:
        return self.config.get("prefix", "")

    @property
    def start_date(self):
        return pendulum.parse(self.config.get("start_date")) if self.config.get("start_date") else None

    @property
    def aws_credentials(self):
        return {
            "aws_access_key_id": self.config["aws_access_key_id"],
            "aws_secret_access_key": self.config["aws_secret_access_key"],
            "region_name": self.config["region_name"],
        }

    @property
    def s3_client(self):
        if not hasattr(self, '_s3_client'):
            s3_config = Config(read_timeout=300, retries={"max_attempts": 10})
            self._s3_client = boto3.client("s3", config=s3_config, **self.aws_credentials)
        return self._s3_client

    def _list_s3_objects(self) -> list[dict]:
        if self._s3_object_list is not None:
            return self._s3_object_list

        paginator = self.s3_client.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
            objects.extend(page.get("Contents", []))

        self._s3_object_list = objects
        return objects

    @property
    def schema(self) -> dict:
        sample_key = self._get_sample_csv_key()
        if not sample_key:
            self.logger.warning("No sample file found. Using fallback schema.")
            return th.PropertiesList(th.Property("last_modified", th.StringType)).to_dict()

        sample_columns = self._extract_csv_columns(sample_key)
        properties = [th.Property(col, th.StringType) for col in sample_columns]
        properties.append(th.Property("last_modified", th.StringType))
        return th.PropertiesList(*properties).to_dict()

    def _extract_csv_columns(self, s3_key: str) -> t.List[str]:
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
        if s3_key.endswith(".zip"):
            with io.BytesIO(obj["Body"].read()) as zip_buffer:
                with zipfile.ZipFile(zip_buffer, 'r') as z:
                    for file in z.namelist():
                        if file.endswith(".csv"):
                            with z.open(file) as csvfile:
                                reader = csv.reader(io.TextIOWrapper(csvfile, encoding="utf-8"))
                                return next(reader)
        else:
            reader = csv.reader(io.TextIOWrapper(obj["Body"], encoding="utf-8"))
            return next(reader)

    def _get_sample_csv_key(self) -> str | None:
        for obj in self._list_s3_objects():
            if obj["Key"].endswith(".csv"):
                return obj["Key"]
        return None

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        start_date = self.start_date
        for obj in self._list_s3_objects():
            key = obj["Key"]
            last_modified = obj["LastModified"]
            if start_date and last_modified <= start_date:
                continue

            self.logger.info(f"📥 Processing {key}")
            s3_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)

            if key.endswith(".zip"):
                yield from self._process_zip(s3_obj["Body"], last_modified)
            elif key.endswith(".csv"):
                yield from self._process_csv(s3_obj["Body"], last_modified)

    def _process_zip(self, body, last_modified: datetime) -> t.Iterable[dict]:
        with io.BytesIO(body.read()) as bio:
            with zipfile.ZipFile(bio) as z:
                for file_name in z.namelist():
                    if file_name.endswith(".csv"):
                        with z.open(file_name) as csvfile:
                            yield from self._stream_csv(csvfile, last_modified)

    def _process_csv(self, body, last_modified: datetime) -> t.Iterable[dict]:
        yield from self._stream_csv(body, last_modified)

    def _stream_csv(self, file_obj, last_modified: datetime) -> t.Iterable[dict]:
        reader = csv.DictReader(io.TextIOWrapper(file_obj, encoding="utf-8"))
        for row in reader:
            clean_row = {k: str(v) if v is not None else None for k, v in row.items()}
            clean_row["last_modified"] = last_modified.isoformat()
            yield clean_row
