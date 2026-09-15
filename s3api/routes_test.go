package s3api_test

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/mulgadc/predastore/s3api"
)

func TestRoutesAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, route := range s3api.Routes() {
		if route.ID == "" || len(route.Names) == 0 {
			t.Errorf("route %+v has no ID or no operation name", route)
		}
		if seen[route.ID] {
			t.Errorf("route ID %q is declared twice", route.ID)
		}
		seen[route.ID] = true

		switch route.Scope {
		case s3api.ScopeService, s3api.ScopeBucket, s3api.ScopeObject:
		default:
			t.Errorf("route %q has unknown scope %q", route.ID, route.Scope)
		}
	}
}

// The gate takes the first route a request selects, so a route that constrains
// nothing answers everything after it.
func TestUnconstrainedRouteIsLastInItsGroup(t *testing.T) {
	fallback := map[string]string{}
	for _, route := range s3api.Routes() {
		key := route.Method + " " + route.Pattern
		if id, ok := fallback[key]; ok {
			t.Errorf("route %q is unreachable: %q already answers every %s", route.ID, id, key)
		}
		if route.Query == "" && route.Header == "" {
			fallback[key] = route.ID
		}
	}
}

func TestOperationsAreSortedAndDistinct(t *testing.T) {
	operations := s3api.Operations()
	if !slices.IsSorted(operations) {
		t.Errorf("Operations() is not sorted: %v", operations)
	}
	if len(slices.Compact(slices.Clone(operations))) != len(operations) {
		t.Errorf("Operations() repeats an operation: %v", operations)
	}

	declared := 0
	for _, route := range s3api.Routes() {
		declared += len(route.Names)
	}
	if declared < len(operations) {
		t.Errorf("Operations() reports %d operations from %d declared names", len(operations), declared)
	}
}

func TestSelectsMatchesOnQueryAndHeader(t *testing.T) {
	route := s3api.Route{Query: "partNumber", Header: "X-Amz-Copy-Source"}

	for name, tc := range map[string]struct {
		target string
		header string
		want   bool
	}{
		"both present":   {target: "/b/k?partNumber=1", header: "/b/src", want: true},
		"query only":     {target: "/b/k?partNumber=1", want: false},
		"header only":    {target: "/b/k", header: "/b/src", want: false},
		"neither":        {target: "/b/k", want: false},
		"empty value ok": {target: "/b/k?partNumber", header: "/b/src", want: true},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, tc.target, nil)
			if tc.header != "" {
				req.Header.Set("X-Amz-Copy-Source", tc.header)
			}
			if got := route.Selects(req); got != tc.want {
				t.Errorf("Selects() = %v, want %v", got, tc.want)
			}
		})
	}
}
