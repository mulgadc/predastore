// Package iampolicy holds the canonical IAM policy-document DTOs and the
// explicit-deny-wins access evaluator shared by predastore and spinifex. It is a
// leaf package (stdlib-only) so both modules can depend on it without a cycle.
package iampolicy

import "encoding/json"

const (
	// EffectAllow is the statement Effect that grants access.
	EffectAllow = "Allow"
	// EffectDeny is the statement Effect that denies access (wins over Allow).
	EffectDeny = "Deny"
)

// PolicyDocument is the parsed IAM policy JSON structure.
type PolicyDocument struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// Statement is a single statement within a policy document.
type Statement struct {
	Sid      string      `json:"Sid,omitempty"`
	Effect   string      `json:"Effect"`
	Action   StringOrArr `json:"Action"`
	Resource StringOrArr `json:"Resource"`
}

// StringOrArr handles JSON fields that can be either a string or an array of
// strings — the AWS shape for Action and Resource.
type StringOrArr []string

// UnmarshalJSON accepts either a JSON string or an array of strings. A JSON null
// yields a nil slice (an inert statement field) rather than [""].
func (s *StringOrArr) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*s = nil
		return nil
	}
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = []string{single}
		return nil
	}
	var arr []string
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	*s = arr
	return nil
}

// MarshalJSON marshals as a bare string when the slice has exactly one element,
// otherwise as an array — the AWS-compatible shape spinifex writes.
func (s StringOrArr) MarshalJSON() ([]byte, error) {
	if len(s) == 1 {
		return json.Marshal(s[0])
	}
	return json.Marshal([]string(s))
}
