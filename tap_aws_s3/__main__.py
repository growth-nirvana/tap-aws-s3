"""AwsS3 entry point."""

from __future__ import annotations

from tap_aws_s3.tap import TapAwsS3

TapAwsS3.cli()
