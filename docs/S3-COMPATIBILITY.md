# S3 compatibility

Measured, not asserted. The numbers here come from `ceph/s3-tests` — the suite Ceph RGW, MinIO and Garage are all validated against — run against a single-node predastore and recorded in `scripts/s3-tests-baseline.txt`. The suite is pinned by `S3TESTS_REF` in `scripts/s3-tests.sh`.

Reproduce it with:

```
./scripts/start.sh -w s3tests
make s3-tests
```

## Where it stands

886 cases from `test_s3.py` and `test_headers.py`:

| | count |
| --- | --- |
| pass | 215 |
| fail | 176 |
| skip | 495 |
| error | 0 |

`skip` is two different things, and it matters which:

- **The suite's own skips** (6 of the 495) — the `cloud_restore` cases, which ceph/s3-tests excludes on any implementation before predastore is ever reached.
- **Predastore's deliberate skips** (489 of the 495, selected by `scripts/s3-tests-skips.txt`) — object lock, POST uploads, server-side encryption and encrypted copy, bucket logging, ACLs, bucket policy, lifecycle, part of versioning, cross-account bucket access, CORS and public access block. These are deselected before the run rather than executed and failed. A case only earns a line there when nobody is actively fixing it — see the header of that file for the exact bar, and `pytest_deselected` in `scripts/s3tests/predastore_cleanup.py` for how a deselected case still lands in the manifest as SKIP instead of silently vanishing.

A passing case is never one of the 489, whatever family's marker or node id would otherwise catch it. `pytest_collection_modifyitems` in `predastore_cleanup.py` computes the skip set from the file, then removes anything the committed baseline records as PASS before it deselects the rest, and prints the exception to stderr. The skip list exists to stop re-running known gaps, not to stop measuring what works. Eleven cases are held back by this guard: `test_encrypted_transfer_13b/1MB/1b/1kb` and seven `test_sse_kms_*` cases, all caught by the `encryption` marker, which ceph/s3-tests also puts on plain TLS-transfer cases. All eleven pass and stay measured.

A skip is not a pass. It means predastore has not been measured against that case in this run, on purpose.

The 176 fails are operations predastore attempts and gets wrong, or refuses: conditional request headers, checksums and `GetObjectAttributes`, object tagging, stored response headers other than `Content-Type`, multipart edge cases, bucket ownership controls, and 36 request-validation cases in `test_headers.py`, 21 of which use Signature V2. The gaps that hurt an ordinary client are a much shorter list, and they are in the first table below.

## The gaps that break real clients

| Operation | State | What happens |
| --- | --- | --- |
| Conditional requests | **Ignored** | `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` are not read on `PutObject`, `GetObject`, `DeleteObject` or `DeleteObjects`. A `PutObject` with `If-None-Match: *` over an existing key overwrites it and answers 200, so a client using it as a create-only lock loses the race silently. `CopyObject` and `UploadPartCopy` refuse the `x-amz-copy-source-if-*` headers with `NotImplemented` rather than ignoring them. `mulga-7oevb` covers `PutObject`. |
| Stored headers other than `Content-Type` | **Dropped** | `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language` and `Expires` are not stored, so a CDN in front of predastore gets no caching directive from the object. `Content-Type` and `x-amz-meta-*` are stored and served back, and survive multipart upload and `CopyObject`. |
| Object tagging | **Refused** | `PutObjectTagging`, `GetObjectTagging` and `DeleteObjectTagging` answer `NotImplemented`, and `x-amz-tagging` on `PutObject` is not applied. Bucket tagging is served. |
| Object ACLs | **Refused** | `GetObjectAcl` and `PutObjectAcl` answer `NotImplemented`. |
| `POST` object | **Missing** | Browser-form uploads. All 36 cases are a deliberate skip below rather than a FAIL. |

### How an unserved sub-resource answers

`s3api/routes.go` declares every operation the gate serves, and a route that names no sub-resource does not select a request carrying one. A sub-resource no route serves is refused by name rather than falling through to the plain bucket or object handler, so a client is never answered with the wrong document or has its object overwritten by a tagging or retention request.

What the refusal says depends on the request:

| Request | Predastore |
| --- | --- |
| Read of an unconfigured bucket control that S3 has a "not configured" error for: `cors`, `encryption`, `lifecycle`, `object-lock`, `ownershipControls`, `policy`, `publicAccessBlock`, `replication`, `website` | 404 with S3's code for it, e.g. `NoSuchCORSConfiguration`, `ObjectLockConfigurationNotFoundError` (`internal/gate/routes.go`) |
| Read of a bucket control S3 answers with a populated 200 when unset: `acl`, `notification`, `logging`, `accelerate`, `requestPayment` | `NotImplemented` |
| Any write to an unserved control | `NotImplemented` |
| Any unserved object sub-resource: `acl`, `tagging`, `retention`, `legal-hold`, `attributes`, `torrent`, … | `NotImplemented` |

The not-configured code is the true answer, since nothing is configured and nothing can configure it, and it is the one a client can carry on from: the Terraform AWS provider reads these during refresh and treats the code as absent. A populated 200 is never synthesised, because that would claim a control predastore does not have.

Served sub-resources: bucket `tagging`, `versioning`, `versions`, `location`, `uploads` and `delete`, and the multipart `uploads`/`uploadId`/`partNumber` object operations.

## By area

Counts from the committed baseline, `pass`/`fail` only. Each non-skipped case is counted in the first row whose selector matches its name, top to bottom, so the rows sum to the 215/176 above. An area predastore has deliberately not implemented is not here; see the next table.

| Area | Pass | Fail | Selector (test name) | Note |
| --- | --- | --- | --- | --- |
| TLS transfer | 11 | 0 | `test_encrypted_transfer_*`, `test_sse_kms_*` | Held by the skip guard; plain transfers over TLS, not server-side encryption. |
| Conditional requests | 9 | 38 | contains `if_match`, `if_none_match`, `ifmatch`, `ifnonematch`, `ifnonmatch`, `ifmodifiedsince`, `ifunmodifiedsince` or `conditional_write` | Every pass is a case where ignoring the header gives the right answer anyway. |
| Checksums and object attributes | 0 | 16 | contains `checksum`, `cksum` or `object_attributes` | |
| Object and bucket tagging | 1 | 11 | ends `_tags`, or contains `tagging` | The pass is bucket tagging. |
| CopyObject / UploadPartCopy | 20 | 5 | `test_object_copy_*`, `test_multipart_copy_*`, `test_upload_part_copy_*` | Invalid ranges, versioned sources and cross-owner copies fail. |
| DeleteObjects | 7 | 1 | contains `multi_object` | The fail is the concurrent versioned delete. |
| Multipart upload | 11 | 10 | contains `multipart` | `GetObject` by `partNumber`, empty and single-small uploads, completing an upload twice. |
| ListObjectsV2 | 37 | 5 | `test_bucket_listv2_*`, `test_bucketv2_*`, `test_basic_key_count` | |
| ListObjects (v1) | 38 | 6 | `test_bucket_list_*` | Both versions fail unordered keys and anonymous listing. |
| Versioning | 4 | 0 | `test_versioned_*`, `test_versioning_*` | Only the cases still selected; the rest are in the next table. |
| Bucket create, delete, head | 33 | 31 | `test_bucket_*`, `test_create_bucket_*`, `test_buckets_*`, `test_list_buckets*`, `test_put_bucket_ownership_*`, `test_expected_bucket_owner` | Request validation (12, most of them Signature V2), ownership controls (7), and `ListBuckets` pagination and anonymous access. |
| Object write and read | 30 | 31 | `test_object_{create,write,set_get,metadata,head,read,delete,put,anon,content}*`, `test_100_continue*`, `test_atomic_*` | Request validation, stored headers other than `Content-Type`, and non-ASCII metadata, which S3 itself returns RFC 2047 encoded. |
| Raw and presigned HTTP | 8 | 11 | `test_object_raw_*`, `test_object_presigned_*`, `test_object_requestid*` | Anonymous reads, `X-Amz-Expires` bounds and response-header overrides. |
| Ranged GET | 5 | 1 | `test_ranged_*` | |
| Everything else | 1 | 10 | | Object ACLs, object lock, bucket policy, usage, torrent and public access block. |

`test_get_undefined_public_block` is in the last row and still fails, although a `GetPublicAccessBlock` on a bucket with none set answers `NoSuchPublicAccessBlockConfiguration`. Why has not been established.

## Deliberate skips

`scripts/s3-tests-skips.txt` names cases across the areas below, either by pytest marker or by node id where ceph/s3-tests has no marker for the feature. The counts are after the PASS guard described above removes anything actually passing, which is why SSE is eleven lower than its markers alone would catch. The areas are not disjoint — a bucket-policy case that also exercises SSE, or a lifecycle case that also exercises versioning's delete marker, is counted in both rows it belongs to — so the rows sum to 496 while the file skips 489 distinct cases.

| Area | Cases | Selector |
| --- | --- | --- |
| SSE (S3, KMS, C) and encrypted copy | 136 | `marker:encryption`, `marker:sse_s3`, `marker:bucket_encryption` |
| Bucket logging | 113 | `marker:bucket_logging` |
| Lifecycle | 48 | `marker:lifecycle` (a superset of `lifecycle_expiration`/`lifecycle_transition`) |
| Object lock | 37 | node ids |
| Bucket policy | 36 | `marker:bucket_policy` + 5 `GetBucketPolicyStatus` node ids |
| POST object uploads | 36 | node ids |
| Bucket and object ACLs | 35 | node ids |
| Versioning | 20 | `marker:delete_marker` + 16 node ids |
| CORS | 14 | node ids |
| Cross-account bucket access | 12 | node ids |
| Public access block | 9 | node ids |

Versioning is the exception to the bar above: predastore serves it, and the file still deselects its suspended-bucket, null-version, delete-marker, versioned-ACL and version-id-on-upload cases as not implemented. Those 20 are unmeasured, not known to fail.

Two cases stay selected on purpose despite matching one of these areas by name: `test_object_lock_get_obj_lock_invalid_bucket` and `test_get_undefined_public_block` only check that the gate answers the "not configured" error, which is sub-resource routing rather than the underlying feature. The first passes; the second is the open question above. See the comments above each block in `scripts/s3-tests-skips.txt` for the other cases held back from their name-matching family.

## What is not measured

`test_iam.py`, `test_sts.py`, `test_sns.py`, `test_s3select.py` and `test_s3control.py` are not run. They cover RGW extensions and other AWS services, none of which predastore claims, so including them would fill the manifest with cases that are not a compatibility question.

## Reading the manifest

`scripts/s3-tests-baseline.txt` is one `STATUS|node id` line per case. It is committed so that a change shows up as a diff rather than as an absolute number. `make s3-tests` fails only on a regression — a line that moved off `PASS`, or a case that vanished. `make s3-tests-strict` fails on any failing case and is red by design; it exists to be read.

When a fix lands, re-record in the same change:

```
make s3-tests-baseline
```

## The cleanup replacement

`scripts/s3tests/predastore_cleanup.py` replaces the suite's bucket-teardown helper. It empties a bucket the same way the suite does, with `ListObjectVersions` and `DeleteObjects`, and then also aborts any multipart upload left incomplete. An incomplete upload holds parts under no key, so no listing reports it, the suite's own helper leaves the bucket undeletable, and the `BucketNotEmpty` lands in the setup of the next case as an error that says nothing about the operation it was meant to measure. No test body, assertion or fixture value changes, and cleanup is not a measured behaviour.
