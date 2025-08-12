"""
Send downloaded/uplaoded files to S3 (or compatible)
"""

from __future__ import annotations

import os.path
from typing import Any

from configparser import NoOptionError

from botocore.exceptions import ClientError
from botocore.session import get_session

from twisted.internet import defer, threads
from twisted.python import log

import cowrie.core.output
from cowrie.core.config import CowrieConfig


class Output(cowrie.core.output.Output):
    """
    s3 output
    """

    def start(self) -> None:
        self.bucket = CowrieConfig.get("output_s3", "bucket")
        self.seen: set[str] = set()
        self.session = get_session()

        try:
            if CowrieConfig.get("output_s3", "access_key_id") and CowrieConfig.get(
                "output_s3", "secret_access_key"
            ):
                self.session.set_credentials(
                    CowrieConfig.get("output_s3", "access_key_id"),
                    CowrieConfig.get("output_s3", "secret_access_key"),
                )
        except NoOptionError:
            log.msg(
                "No AWS credentials found in config - using botocore global settings."
            )

        self.client = self.session.create_client(
            "s3",
            region_name=CowrieConfig.get("output_s3", "region"),
            endpoint_url=CowrieConfig.get("output_s3", "endpoint", fallback=None),
            verify=CowrieConfig.getboolean("output_s3", "verify", fallback=True),
        )

    def stop(self) -> None:
        pass

    def write(self, event: dict[str, Any]) -> None:
        if event["eventid"] == "cowrie.session.file_download":
            self.upload(event["shasum"], event["outfile"], "downloads")

        elif event["eventid"] == "cowrie.session.file_upload":
            self.upload(event["shasum"], event["outfile"], "downloads")

        elif event['eventid'] == 'cowrie.log.closed':
            self.upload(event["shasum"], event["ttylog"], "tty")

    @defer.inlineCallbacks
    def _object_exists_remote(self, key):
        try:
            yield threads.deferToThread(
                self.client.head_object,
                Bucket=self.bucket,
                Key=key,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                defer.returnValue(False)
            raise

        defer.returnValue(True)

    @defer.inlineCallbacks
    def upload(self, shasum, filename, path):
        key = os.path.join(path, shasum)
        if key in self.seen:
            log.msg(f"Already uploaded file {key} to S3")
            return

        exists = yield self._object_exists_remote(key)
        if exists:
            log.msg(f"Somebody else already uploaded file {key} to S3")
            self.seen.add(key)
            return

        log.msg(f"Uploading file {key} ({filename}) to S3")
        with open(filename, "rb") as fp:
            yield threads.deferToThread(
                self.client.put_object,
                Bucket=self.bucket,
                Key=key,
                Body=fp.read(),
                ContentType="application/octet-stream",
            )

        self.seen.add(shasum)
