package gate

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/s3api"
)

func stubHandlers() map[string]http.Handler {
	built := map[string]http.Handler{}
	for _, route := range s3api.Routes() {
		built[route.ID] = http.NotFoundHandler()
	}
	return built
}

// The table names what predastore serves, so a handler set that has drifted
// from it is a build-time failure rather than a route nobody notices.
func TestCheckHandlersRejectsDrift(t *testing.T) {
	if err := checkHandlers(stubHandlers()); err != nil {
		t.Fatalf("the declared table and the handler set disagree: %v", err)
	}

	missing := stubHandlers()
	delete(missing, "PutObject")
	if err := checkHandlers(missing); err == nil {
		t.Error("checkHandlers accepted a declared route with no handler")
	}

	orphan := stubHandlers()
	orphan["PutBucketVersioning"] = http.NotFoundHandler()
	if err := checkHandlers(orphan); err == nil {
		t.Error("checkHandlers accepted a handler answering no declared route")
	}
}

func TestGroupRoutesGroupsByMethodAndPattern(t *testing.T) {
	for _, group := range groupRoutes(s3api.ScopeObject) {
		for _, route := range group {
			if route.Method != group[0].Method || route.Pattern != group[0].Pattern {
				t.Errorf("route %q is grouped under %s %s", route.ID, group[0].Method, group[0].Pattern)
			}
		}
	}

	for scope, want := range map[s3api.Scope]string{
		s3api.ScopeService: "/",
		s3api.ScopeBucket:  "/{bucket}",
		s3api.ScopeObject:  "/{bucket}/*",
	} {
		groups := groupRoutes(scope)
		if len(groups) == 0 {
			t.Fatalf("scope %q has no routes", scope)
		}
		for _, group := range groups {
			if group[0].Pattern != want {
				t.Errorf("scope %q carries pattern %q, want %q", scope, group[0].Pattern, want)
			}
		}
	}
}

// PUT on an object is the four-way split the table exists to make explicit.
func TestSelectRouteTakesTheFirstMatch(t *testing.T) {
	var served string
	built := map[string]http.Handler{}
	for _, route := range s3api.Routes() {
		built[route.ID] = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = route.ID })
	}

	var object []s3api.Route
	for _, group := range groupRoutes(s3api.ScopeObject) {
		if group[0].Method == http.MethodPut {
			object = group
		}
	}
	handler := selectRoute(object, built)

	for name, tc := range map[string]struct {
		target string
		copy   string
		want   string
	}{
		"plain put":     {target: "/b/k", want: "PutObject"},
		"copy":          {target: "/b/k", copy: "/b/src", want: "CopyObject"},
		"upload part":   {target: "/b/k?partNumber=1&uploadId=u", want: "UploadPart"},
		"part copy":     {target: "/b/k?partNumber=1&uploadId=u", copy: "/b/src", want: "UploadPartCopy"},
		"unknown query": {target: "/b/k?acl", want: "PutObject"},
	} {
		t.Run(name, func(t *testing.T) {
			served = ""
			req := httptest.NewRequest(http.MethodPut, tc.target, nil)
			if tc.copy != "" {
				req.Header.Set("X-Amz-Copy-Source", tc.copy)
			}
			handler.ServeHTTP(httptest.NewRecorder(), req)
			if served != tc.want {
				t.Errorf("served %q, want %q", served, tc.want)
			}
		})
	}
}

// A POST at a bucket is the batch delete and nothing else, so the group has no
// fallback and an unselected request is answered rather than dropped.
func TestSelectRouteAnswersAnUnselectedRequest(t *testing.T) {
	var group []s3api.Route
	for _, candidate := range groupRoutes(s3api.ScopeBucket) {
		if candidate[0].Method == http.MethodPost {
			group = candidate
		}
	}

	recorder := httptest.NewRecorder()
	selectRoute(group, stubHandlers()).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/b", nil))

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
	if !strings.Contains(recorder.Body.String(), "MethodNotAllowed") {
		t.Errorf("body does not carry the S3 error code:\n%s", recorder.Body.String())
	}
}
