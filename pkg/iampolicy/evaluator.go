package iampolicy

import "log/slog"

// Decision represents the outcome of a policy evaluation.
type Decision int

const (
	// Deny is the default — no matching Allow, or an explicit Deny.
	Deny Decision = iota
	// Allow means an explicit Allow was found with no overriding Deny.
	Allow
)

// Evaluate reports whether action on resource is permitted by the supplied
// policy documents, following AWS's evaluation order:
//
//  1. Explicit Deny in any statement → Deny (wins immediately).
//  2. Explicit Allow in any statement → Allow.
//  3. No matching statement → Deny (implicit default).
//
// Actions match case-insensitively (AWS lower-cases service:verb); resource
// ARNs match case-sensitively (AWS spec). An unrecognized Effect fails closed to
// Deny with a warning. Root bypass, if any, is handled by the caller.
func Evaluate(action, resource string, policies []PolicyDocument) Decision {
	hasAllow := false
	for i := range policies {
		for j := range policies[i].Statement {
			stmt := &policies[i].Statement[j]

			if !matchesAny(stmt.Action, action, true) {
				continue
			}
			if !matchesAny(stmt.Resource, resource, false) {
				continue
			}
			switch stmt.Effect {
			case EffectDeny:
				return Deny
			case EffectAllow:
				hasAllow = true
			default:
				slog.Warn("iampolicy.Evaluate: unrecognized Effect, treating as Deny",
					"effect", stmt.Effect, "action", action)
				return Deny
			}
		}
	}

	if hasAllow {
		return Allow
	}
	return Deny
}
