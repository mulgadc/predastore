package auth

import (
	"errors"
	"strings"
)

// ParseRoleARN extracts the account ID and role name from an IAM role ARN of
// the form arn:aws:iam::<accountID>:role/<path>/<name> (path optional). The
// name is the segment after the final "/". A malformed ARN — wrong prefix,
// non-role resource, empty name, or empty account — returns an error; callers
// that must fail closed treat any error as an implicit deny.
func ParseRoleARN(arn string) (accountID, name string, err error) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "iam" || parts[3] != "" {
		return "", "", errors.New("not an IAM ARN")
	}
	const prefix = "role/"
	resource := parts[5]
	if !strings.HasPrefix(resource, prefix) {
		return "", "", errors.New("ARN resource is not a role")
	}
	pathAndName := resource[len(prefix):]
	if slash := strings.LastIndex(pathAndName, "/"); slash >= 0 {
		name = pathAndName[slash+1:]
	} else {
		name = pathAndName
	}
	if name == "" {
		return "", "", errors.New("role name is empty")
	}
	// A real IAM role ARN always carries a non-empty account, so an empty
	// account segment is malformed; reject it so callers fail closed.
	if parts[4] == "" {
		return "", "", errors.New("account ID is empty")
	}
	return parts[4], name, nil
}

// ExtractPolicyName returns the trailing name segment of an IAM policy ARN,
// e.g. arn:aws:iam::000000000001:policy/AdministratorAccess -> AdministratorAccess.
// Returns "" for an ARN with no ":policy" segment.
func ExtractPolicyName(arn string) string {
	parts := strings.SplitN(arn, ":policy", 2)
	if len(parts) != 2 || parts[1] == "" {
		return ""
	}
	segments := strings.Split(parts[1], "/")
	return segments[len(segments)-1]
}
