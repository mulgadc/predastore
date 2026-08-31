"""Pytest plugin for running ceph/s3-tests against predastore.

Two jobs, both about making the run report honestly.

**Cleanup fallback.** s3-tests empties a bucket with ListObjectVersions and
DeleteObjects. Predastore answers 405 to DeleteObjects and serves a plain object
listing for the versions request, which boto3 parses as zero versions, so nothing
is deleted and every DeleteBucket fails with BucketNotEmpty. That failure lands
in the *setup* of the next test, so one missing operation turns the whole suite
into 880 errors that say nothing about the 880 operations they were meant to
measure. Only the suite's own teardown helper is replaced -- no test body,
assertion or fixture value changes, and cleanup is not a measured behaviour.
Drop this half when DeleteObjects lands.

**Manifest.** Writes one `STATUS|node id` line per case to the path in
PREDA_S3TESTS_MANIFEST. Taken from pytest's own report rather than from JUnit
XML, which carries a dotted module name and no file, so a node id has to be
guessed back out of it.
"""

import os

import botocore.exceptions


def _nuke_bucket(client, bucket):
    """Empty a bucket one key at a time, then delete it."""
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            client.delete_object(Bucket=bucket, Key=obj['Key'])

    # An upload that was never completed holds parts but no key, so the listing
    # above does not see it and the bucket will not delete.
    try:
        uploads = client.list_multipart_uploads(Bucket=bucket).get('Uploads', [])
    except botocore.exceptions.ClientError:
        uploads = []
    for upload in uploads:
        try:
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload['Key'], UploadId=upload['UploadId'])
        except botocore.exceptions.ClientError:
            pass

    client.delete_bucket(Bucket=bucket)


# A case is reported once per phase, so the three have to be reduced to one
# status. The strongest wins: a test whose fixtures did not run has measured
# nothing, and a skipped test whose teardown passed is still skipped.
_RANK = {'PASS': 0, 'SKIP': 1, 'FAIL': 2, 'ERROR': 3}


class ManifestRecorder:
    def __init__(self, path):
        self.path = path
        self.results = {}

    def pytest_runtest_logreport(self, report):
        if report.outcome == 'failed':
            status = 'FAIL' if report.when == 'call' else 'ERROR'
        elif report.outcome == 'skipped':
            status = 'SKIP'
        else:
            status = 'PASS'

        current = self.results.get(report.nodeid)
        if current is None or _RANK[status] > _RANK[current]:
            self.results[report.nodeid] = status

    def pytest_deselected(self, items):
        # A deselected case -- filtered by -m or --deselect, marker and node id
        # id alike -- never runs, so logreport never fires for it. Record it as
        # SKIP here or it silently drops out of the manifest instead.
        for item in items:
            current = self.results.get(item.nodeid)
            if current is None or _RANK['SKIP'] > _RANK[current]:
                self.results[item.nodeid] = 'SKIP'

    def pytest_sessionfinish(self, session):
        with open(self.path, 'w', encoding='utf-8') as handle:
            for node in sorted(self.results):
                handle.write('%s|%s\n' % (self.results[node], node))


def pytest_configure(config):
    # Imported here rather than at module scope: pytest loads this plugin
    # before the s3-tests checkout is on sys.path.
    import s3tests.functional as functional

    functional.nuke_bucket = _nuke_bucket

    manifest = os.environ.get('PREDA_S3TESTS_MANIFEST')
    if manifest:
        config.pluginmanager.register(ManifestRecorder(manifest), 'predastore-manifest')
