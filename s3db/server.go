package s3db

// Server package for the distributed database

// REST (Fiber) service, using AWS authentication

// Endpoints:
// GET /get/{table}/{key}
// POST /put/{table}/{key}
// DELETE /delete/{table}/{key}

// Use Hashicorp raft for consensus, leader election and consensus across the cluster.

// Use BadgerDB for storage

// Use Fiber for HTTP server
