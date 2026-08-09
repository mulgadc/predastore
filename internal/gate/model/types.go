// Package model holds the S3 semantics the gate speaks: the stored metadata
// records, the error taxonomy, name validation, the multipart helpers, object
// hashing and the global-state table names. It is pure data and pure functions
// — nothing here talks to storage, state or the network.
package model

import "time"

// BucketMetadata contains complete bucket metadata stored in global state.
type BucketMetadata struct {
	Name         string    `json:"name"`
	Region       string    `json:"region"`
	OwnerID      string    `json:"owner_id"`      // Access key ID of owner (legacy)
	AccountID    string    `json:"account_id"`    // 12-digit account ID of owner
	OwnerDisplay string    `json:"owner_display"` // Display name
	CreationDate time.Time `json:"creation_date"`
	Public       bool      `json:"public"`      // Allow anonymous access
	ObjectLock   bool      `json:"object_lock"` // Object lock enabled
	Versioning   string    `json:"versioning"`  // Enabled, Suspended, or empty
}

// CompletedPart is one part a client claims to have uploaded, as named in a
// CompleteMultipartUpload request.
type CompletedPart struct {
	PartNumber int
	ETag       string
}
