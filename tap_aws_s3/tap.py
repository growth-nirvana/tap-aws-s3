from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from tap_aws_s3.streams import S3CSVStream

class TapAwsS3(Tap):
    """Singer tap for AWS S3."""

    name = "tap-aws-s3"

    config_jsonschema = th.PropertiesList(
        th.Property("bucket_name", th.StringType, required=True),
        th.Property("aws_access_key_id", th.StringType, required=True),
        th.Property("aws_secret_access_key", th.StringType, required=True),
        th.Property("prefix", th.StringType, required=False),
        th.Property("start_date", th.StringType, required=False),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        return [S3CSVStream(tap=self)]
