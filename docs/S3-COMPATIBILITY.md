# S3 compatibility

Measured, not asserted. The numbers here come from `ceph/s3-tests` — the suite Ceph RGW, MinIO and Garage are all validated against — run against a single-node predastore and recorded in `scripts/s3-tests-baseline.txt`.

Reproduce it with:

```
./scripts/start.sh -w s3tests
make s3-tests
```

## Where it stands

886 cases from `test_s3.py` and `test_headers.py`:

| | count |
| --- | --- |
| pass | 178 |
| fail | 614 |
| skip | 92 |
| error | 2 |

The skips are the suite's own — cases it excludes on any implementation. None are predastore exclusions; `scripts/s3-tests-skips.txt` is empty, and the bar for adding a line to it is deliberately high.

A pass rate of 20% sounds worse than the practical position is. Most of the 614 are features predastore has not started rather than operations it gets wrong: object lock, versioning, ACLs, bucket policy, lifecycle, bucket logging, POST uploads, encrypted copy and server-side encryption together account for 295 of them. The gaps that hurt an ordinary client are a much shorter list, and they are in the first table below.

## The gaps that break real clients

| Operation | State | What happens |
| --- | --- | --- |
| `DeleteObjects` | **Missing** | 405 Method Not Allowed. Every client that empties a bucket in batches — the AWS CLI's `s3 rm --recursive`, rclone `purge`, the s3-tests teardown — falls back or fails. |
| ETag | **Wrong value** | Not the body MD5. `PUT` of `hello` returns `678f45d4…`, not `5d41402a…`. It is also unquoted, where S3 quotes it, and `ListObjectsV2` returns it as the empty string while `HEAD` and `GET` return a value. rclone rejects every upload as corrupt on this. |
| `ListObjects` (v1) | **Partial** | `Marker` is ignored. A v1 listing with `Marker=baz&MaxKeys=2` returns the first two keys again rather than the ones after `baz`, so a v1 client paging a prefix loops. v2's `continuation-token` and `start-after` do work. |
| `ListObjectVersions` | **Answers the wrong document** | `GET /{bucket}?versions` is not routed, so it falls through and serves a plain `ListBucketResult`. boto3 parses that as zero versions and reports success. A client asking what versions exist is told "none". |
| User metadata | **Dropped** | `x-amz-meta-*` sent on `PutObject` does not come back on `HeadObject`. |
| `POST` object | **Missing** | All 36 browser-upload cases fail. |

### The pattern behind several of these

An unrecognised sub-resource query string is not rejected — the request falls through to the plain bucket or object handler and answers 200 with the wrong document. `?versions` above is the clearest case. The same shape shows up on read paths for configuration that does not exist:

| Request | Predastore | S3 |
| --- | --- | --- |
| `GetBucketCors` on a bucket with no CORS | 200, empty | `NoSuchCORSConfiguration` |
| `GetBucketLifecycleConfiguration`, none set | 200, empty | `NoSuchLifecycleConfiguration` |
| `GetBucketEncryption`, none set | 200, empty | `ServerSideEncryptionConfigurationNotFoundError` |
| `GetBucketTagging`, none set | 200, empty | `NoSuchTagSet` |
| `GetPublicAccessBlock`, none set | 200, empty | `NoSuchPublicAccessBlockConfiguration` |
| `GetObjectLockConfiguration`, none set | 200, empty | `ObjectLockConfigurationNotFoundError` |
| `PutBucketVersioning` | `BucketAlreadyOwnedByYou` | 200 |
| `PutObjectAcl` | 200, no effect | 200, ACL applied |
| `GetObjectAcl` | 500 | 200 |

A client cannot tell "predastore does not do this" from "this bucket has none of it configured". That is worse for the caller than a clean `NotImplemented`, which `GetBucketAcl` and `PutBucketPolicy` do return.

## By area

Counts from the committed baseline. `pass`/`fail` only — the suite's own skips are left out of the rows.

| Area | Pass | Fail | Note |
| --- | --- | --- | --- |
| ListObjectsV2 | 34 | 6 | The best-supported listing path. |
| ListObjects (v1) | 28 | 16 | `Marker` ignored. |
| CreateBucket / naming rules | 23 | 17 | |
| Object create / write | 11 | 25 | Metadata and conditional headers. |
| Multipart upload | 6 | 6 | |
| Ranged GET | 4 | 1 | |
| Bucket logging | 0 | 31 | Not implemented. |
| Copy with encryption | 0 | 64 | Not implemented. |
| POST object | 0 | 36 | Not implemented. |
| Object lock | 3 | 36 | Not implemented. |
| Bucket policy | 0 | 23 | `NotImplemented`. Two more error in teardown. |
| Bucket / object ACL | 2 | 28 | |
| Lifecycle | 0 | 24 | |
| SSE (S3, KMS, C) | 7 | 36 | |
| Versioning | 0 | 15 | |
| CORS | 0 | 10 | |
| DeleteObjects | 0 | 9 | 405. |
| CopyObject / copy part | 4 | 40 | |

## What is not measured

`test_iam.py`, `test_sts.py`, `test_sns.py`, `test_s3select.py` and `test_s3control.py` are not run. They cover RGW extensions and other AWS services, none of which predastore claims, so including them would fill the manifest with cases that are not a compatibility question.

## Reading the manifest

`scripts/s3-tests-baseline.txt` is one `STATUS|node id` line per case. It is committed so that a change shows up as a diff rather than as an absolute number. `make s3-tests` fails only on a regression — a line that moved off `PASS`, or a case that vanished. `make s3-tests-strict` fails on any failing case and is red by design; it exists to be read.

When a fix lands, re-record in the same change:

```
make s3-tests-baseline
```

## Caveat: the run needs a cleanup fallback

s3-tests empties a bucket with `ListObjectVersions` and `DeleteObjects`. Predastore supports neither correctly, so nothing is deleted, every `DeleteBucket` answers `BucketNotEmpty`, and that failure lands in the *setup* of the next case. Unpatched, the run is 884 errors that say nothing about the 884 operations they were meant to measure.

`scripts/s3tests/predastore_cleanup.py` replaces the suite's teardown helper with per-key deletes. No test body, assertion or fixture value changes, and cleanup is not a measured behaviour. Delete that half of the plugin when `DeleteObjects` lands.
