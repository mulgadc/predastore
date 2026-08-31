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
| fail | 216 |
| skip | 492 |
| error | 0 |

`skip` is two different things, and it matters which:

- **The suite's own skips** (6 of the 492) — cases ceph/s3-tests excludes on any implementation, decided by the suite itself before predastore is ever reached.
- **Predastore's deliberate skips** (486 of the 492, listed in `scripts/s3-tests-skips.txt`) — a feature predastore has decided not to offer for now: object lock, POST uploads, server-side encryption and encrypted copy, bucket logging, ACLs, bucket policy, lifecycle, versioning, cross-account bucket access, CORS and public access block. These are deselected before the run rather than executed and failed, which is the difference between this run taking about seven minutes and thirteen. A case only earns a line there when nobody is actively fixing it — see the header of that file for the exact bar, and `pytest_deselected` in `scripts/s3tests/predastore_cleanup.py` for how a deselected case still lands in the manifest as SKIP instead of silently vanishing.

A passing case is never one of the 486, whatever family's marker or node id would otherwise catch it. `pytest_collection_modifyitems` in `predastore_cleanup.py` computes the skip set from the file, then removes anything the committed baseline records as PASS before it deselects the rest, and prints the exception to stderr. The skip list exists to stop re-running known gaps, not to stop measuring what works — a marker written for one family can catch a case that belongs to a different, working one, as `encryption` did here with plain TLS-transfer tests. Fourteen cases are held back by this guard right now: `test_encrypted_transfer_13b/1MB/1b/1kb` and seven `test_sse_kms_*` cases, caught by the `encryption`/`sse_s3` markers meant for actual server-side encryption, and `test_object_lock_changing_mode_from_governance_with_bypass`, `test_object_lock_get_legal_hold` and `test_object_lock_put_legal_hold`, listed by node id alongside object lock cases that do fail. All fourteen pass and stay measured.

A skip is not a pass. It means the same thing it always did for the suite's own skips: predastore has not been measured against that case in this run, on purpose. `docs/development/bugs/` and `docs/development/improvements/` carry the beads for anything in progress; a deliberate skip here means no bead is open yet.

The 216 remaining fails are not features predastore has not started — those are now skipped — they are operations predastore attempts and gets wrong, or is actively being fixed: DeleteObjects, CopyObject and multipart copy, ETag, `ListObjects` v1 `Marker`, user metadata, sub-resource routing, and the request-validation cases in `test_headers.py`. The gaps that hurt an ordinary client are a much shorter list, and they are in the first table below.

## The gaps that break real clients

| Operation | State | What happens |
| --- | --- | --- |
| `DeleteObjects` | **Missing** | 405 Method Not Allowed. Every client that empties a bucket in batches — the AWS CLI's `s3 rm --recursive`, rclone `purge`, the s3-tests teardown — falls back or fails. |
| ETag | **Wrong value** | Not the body MD5. `PUT` of `hello` returns `678f45d4…`, not `5d41402a…`. It is also unquoted, where S3 quotes it, and `ListObjectsV2` returns it as the empty string while `HEAD` and `GET` return a value. rclone rejects every upload as corrupt on this. |
| `ListObjects` (v1) | **Partial** | `Marker` is ignored. A v1 listing with `Marker=baz&MaxKeys=2` returns the first two keys again rather than the ones after `baz`, so a v1 client paging a prefix loops. v2's `continuation-token` and `start-after` do work. |
| `ListObjectVersions` | **Answers the wrong document** | `GET /{bucket}?versions` is not routed, so it falls through and serves a plain `ListBucketResult`. boto3 parses that as zero versions and reports success. A client asking what versions exist is told "none". |
| User metadata | **Dropped** | `x-amz-meta-*` sent on `PutObject` does not come back on `HeadObject`. |
| `POST` object | **Missing** | Browser-form uploads. All 36 cases are a deliberate skip below rather than a FAIL — no bead is open for this one yet. |

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

Counts from the committed baseline, for the areas still running. `pass`/`fail`
only — the suite's own skips are left out of the rows. An area predastore has
deliberately not implemented is not here; see the next table.

| Area | Pass | Fail | Note |
| --- | --- | --- | --- |
| ListObjectsV2 | 34 | 6 | The best-supported listing path. |
| ListObjects (v1) | 28 | 16 | `Marker` ignored. |
| CreateBucket / naming rules | 23 | 17 | |
| Object create / write | 11 | 25 | Metadata and conditional headers. |
| Multipart upload | 6 | 6 | |
| Ranged GET | 4 | 1 | |
| DeleteObjects | 0 | 9 | 405. |
| CopyObject / copy part | 4 | 23 | Excludes encrypted copy, which is a deliberate skip below. |

## Deliberate skips

`scripts/s3-tests-skips.txt` names cases across the areas below, either by
pytest marker or by node id where ceph/s3-tests has no marker for the
feature. Each is a feature predastore has decided not to offer for now, not a
case the suite itself would skip. The counts below are after the PASS guard
described above removes anything actually passing — Object lock and SSE are
each three and eleven lower than what the file's markers and node ids alone
would catch, for the fourteen cases named above. The areas are not disjoint
either — a bucket-policy case that also exercises SSE, or a lifecycle case
that also exercises versioning's delete marker, is counted in both rows it
belongs to — so the rows sum to 493 while the file skips 486 distinct cases.

| Area | Cases | Selector |
| --- | --- | --- |
| SSE (S3, KMS, C) and encrypted copy | 136 | `marker:encryption`, `marker:sse_s3`, `marker:bucket_encryption` |
| Bucket logging | 113 | `marker:bucket_logging` |
| Bucket and object ACLs | 35 | node ids |
| Bucket policy | 36 | `marker:bucket_policy` + 5 `GetBucketPolicyStatus` node ids |
| Object lock | 34 | node ids |
| Lifecycle | 48 | `marker:lifecycle` (a superset of `lifecycle_expiration`/`lifecycle_transition`) |
| Versioning | 20 | node ids + `marker:delete_marker` |
| POST object uploads | 36 | node ids |
| CORS | 14 | node ids |
| Cross-account bucket access | 12 | node ids |
| Public access block | 9 | node ids |

Two cases stay a FAIL on purpose despite matching one of these areas by name:
`test_object_lock_get_obj_lock_invalid_bucket` and
`test_get_undefined_public_block` only check that the gate answers a proper
"not configured" error, which is the sub-resource-routing gap
(`mulga-nv5p5`), not the underlying feature. They should flip to PASS when
that bead lands rather than staying hidden behind a skip. See the comments
above each block in `scripts/s3-tests-skips.txt` for the versioning and
CopyObject/DeleteObjects cases held back from their name-matching family for
the same kind of reason.

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
