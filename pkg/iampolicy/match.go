package iampolicy

import "strings"

// MatchWildcard reports whether value matches pattern, where "*" matches zero or
// more characters at any position (infix), as AWS IAM Action and Resource
// patterns require (e.g. arn:aws:iam::*:role/app-*). Matching is case-sensitive;
// callers that need case-insensitivity lower-case both inputs first.
func MatchWildcard(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == value
	}

	parts := strings.Split(pattern, "*")
	last := len(parts) - 1

	if !strings.HasPrefix(value, parts[0]) {
		return false
	}
	if !strings.HasSuffix(value, parts[last]) {
		return false
	}

	// Trim the anchored ends before scanning the middle parts.
	remaining := value[len(parts[0]):]
	if len(remaining) < len(parts[last]) {
		return false
	}
	remaining = remaining[:len(remaining)-len(parts[last])]

	// Walk through the middle parts in order.
	for i := 1; i < last; i++ {
		idx := strings.Index(remaining, parts[i])
		if idx < 0 {
			return false
		}
		remaining = remaining[idx+len(parts[i]):]
	}
	return true
}

// matchesAny reports whether any pattern matches value. When caseInsensitive is
// true both sides are lower-cased before wildcard matching (used for IAM
// actions); when false, matching is exact-case (used for resource ARNs).
func matchesAny(patterns []string, value string, caseInsensitive bool) bool {
	if caseInsensitive {
		value = strings.ToLower(value)
	}
	for _, p := range patterns {
		if caseInsensitive {
			p = strings.ToLower(p)
		}
		if MatchWildcard(p, value) {
			return true
		}
	}
	return false
}
