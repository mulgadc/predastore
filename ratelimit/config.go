// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package ratelimit

// Config holds rate limit settings.
type Config struct {
	Enabled bool                    `toml:"enabled"`
	Rate    int                     `toml:"rate"`   // default requests/sec
	Burst   int                     `toml:"burst"`  // default burst capacity
	Action  map[string]BucketConfig `toml:"action"` // per-action overrides
}

// BucketConfig holds per-action rate limit overrides.
type BucketConfig struct {
	Rate  int `toml:"rate"`
	Burst int `toml:"burst"`
}

// ResolveLimit returns the rate and burst for a given action.
// Per-action override if present, otherwise default.
func (c *Config) ResolveLimit(action string) (rate int, burst int) {
	if override, ok := c.Action[action]; ok {
		return override.Rate, override.Burst
	}
	return c.Rate, c.Burst
}
