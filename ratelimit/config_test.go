package ratelimit

import "testing"

func TestConfigResolve_Default(t *testing.T) {
	cfg := Config{Rate: 20, Burst: 100}
	r, b := cfg.ResolveLimit("DescribeInstances")
	if r != 20 || b != 100 {
		t.Fatalf("expected 20/100, got %d/%d", r, b)
	}
}

func TestConfigResolve_PerActionOverride(t *testing.T) {
	cfg := Config{
		Rate:  20,
		Burst: 100,
		Action: map[string]BucketConfig{
			"RunInstances": {Rate: 2, Burst: 40},
		},
	}

	r, b := cfg.ResolveLimit("RunInstances")
	if r != 2 || b != 40 {
		t.Fatalf("expected 2/40, got %d/%d", r, b)
	}

	// Unlisted action should use default.
	r, b = cfg.ResolveLimit("DescribeInstances")
	if r != 20 || b != 100 {
		t.Fatalf("expected 20/100, got %d/%d", r, b)
	}
}

func TestConfigResolve_EmptyActions(t *testing.T) {
	cfg := Config{Rate: 10, Burst: 50}
	r, b := cfg.ResolveLimit("anything")
	if r != 10 || b != 50 {
		t.Fatalf("expected 10/50, got %d/%d", r, b)
	}
}
