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
	orphan["PutBucketReplication"] = http.NotFoundHandler()
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
		"plain put":   {target: "/b/k", want: "PutObject"},
		"copy":        {target: "/b/k", copy: "/b/src", want: "CopyObject"},
		"upload part": {target: "/b/k?partNumber=1&uploadId=u", want: "UploadPart"},
		"part copy":   {target: "/b/k?partNumber=1&uploadId=u", copy: "/b/src", want: "UploadPartCopy"},
		// A sub-resource nobody serves selects nothing. Falling through to
		// PutObject here stored the sub-resource document as the object body.
		"unserved sub-resource": {target: "/b/k?acl", want: ""},
		// versionId names a version of the object, not an operation of its own,
		// so it still selects the route it modifies.
		"version modifier": {target: "/b/k?versionId=v", want: "PutObject"},
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

// Every bucket sub-resource predastore serves used to select the unconstrained
// route for its method, so a tag write was answered as CreateBucket and a
// location read as a listing.
func TestSelectRouteDispatchesBucketSubResources(t *testing.T) {
	var served string
	built := map[string]http.Handler{}
	for _, route := range s3api.Routes() {
		built[route.ID] = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = route.ID })
	}

	groups := map[string][]s3api.Route{}
	for _, group := range groupRoutes(s3api.ScopeBucket) {
		groups[group[0].Method] = group
	}

	for name, tc := range map[string]struct {
		method string
		target string
		want   string
	}{
		"tag write":    {method: http.MethodPut, target: "/b?tagging", want: "PutBucketTagging"},
		"plain create": {method: http.MethodPut, target: "/b", want: "CreateBucket"},
		// A bucket sub-resource nobody serves is refused by the router rather
		// than answered as a bucket create.
		"refused write": {method: http.MethodPut, target: "/b?encryption", want: ""},
		"tag read":      {method: http.MethodGet, target: "/b?tagging", want: "GetBucketTagging"},
		"location read": {method: http.MethodGet, target: "/b?location", want: "GetBucketLocation"},
		"plain listing": {method: http.MethodGet, target: "/b", want: "ListObjects"},
		"tag delete":    {method: http.MethodDelete, target: "/b?tagging", want: "DeleteBucketTagging"},
		"bucket delete": {method: http.MethodDelete, target: "/b", want: "DeleteBucket"},
	} {
		t.Run(name, func(t *testing.T) {
			served = ""
			selectRoute(groups[tc.method], built).ServeHTTP(
				httptest.NewRecorder(), httptest.NewRequest(tc.method, tc.target, nil))
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

// objectSubResources are the sub-resources an object request can carry that no
// route serves. Written out rather than read from s3api's own list, so removing
// one from that list fails here instead of quietly restoring the fall-through.
var objectSubResources = []string{
	"acl", "tagging", "retention", "legal-hold", "torrent", "restore",
	"select", "attributes", "policy", "policyStatus",
}

// Every one of these used to select the unconstrained route for its method. On
// PUT that is PutObject, so asking about an object's tags stored the request
// body as the object; on DELETE it is DeleteObject, so asking to clear them
// deleted it.
func TestUnservedObjectSubResourcesReachNoHandler(t *testing.T) {
	groups := map[string][]s3api.Route{}
	for _, group := range groupRoutes(s3api.ScopeObject) {
		groups[group[0].Method] = group
	}

	for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete, http.MethodHead} {
		for _, sub := range objectSubResources {
			t.Run(method+" "+sub, func(t *testing.T) {
				var served string
				built := map[string]http.Handler{}
				for _, route := range s3api.Routes() {
					built[route.ID] = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = route.ID })
				}

				recorder := httptest.NewRecorder()
				selectRoute(groups[method], built).ServeHTTP(recorder,
					httptest.NewRequest(method, "/b/k?"+sub, nil))

				if served != "" {
					t.Fatalf("%s ?%s was served by %q", method, sub, served)
				}
				if recorder.Code != http.StatusNotImplemented {
					t.Errorf("status = %d, want %d", recorder.Code, http.StatusNotImplemented)
				}
				// HEAD carries no body, so the code is all there is to assert on.
				if method != http.MethodHead && !strings.Contains(recorder.Body.String(), sub) {
					t.Errorf("the refusal does not name the sub-resource:\n%s", recorder.Body.String())
				}
			})
		}
	}
}

// The parameters the SDKs add are modifiers on an operation rather than
// operations, so each must still select the route it modifies. A guard written
// as "any query key no route claims" would refuse every one of these.
func TestModifiersStillSelectTheirRoute(t *testing.T) {
	groups := map[string][]s3api.Route{}
	for _, group := range groupRoutes(s3api.ScopeObject) {
		groups[group[0].Method] = group
	}

	for name, tc := range map[string]struct {
		method string
		target string
		want   string
	}{
		"put with sdk marker": {method: http.MethodPut, target: "/b/k?x-id=PutObject", want: "PutObject"},
		"get a version":       {method: http.MethodGet, target: "/b/k?versionId=v", want: "GetObject"},
		"get with overrides":  {method: http.MethodGet, target: "/b/k?response-content-type=text%2Fplain", want: "GetObject"},
		"delete a version":    {method: http.MethodDelete, target: "/b/k?versionId=v", want: "DeleteObject"},
		"head a version":      {method: http.MethodHead, target: "/b/k?versionId=v", want: "HeadObject"},
		"list the parts":      {method: http.MethodGet, target: "/b/k?uploadId=u", want: "ListParts"},
		"abort the upload":    {method: http.MethodDelete, target: "/b/k?uploadId=u", want: "AbortMultipartUpload"},
		"start an upload":     {method: http.MethodPost, target: "/b/k?uploads", want: "CreateMultipartUpload"},
		"complete an upload":  {method: http.MethodPost, target: "/b/k?uploadId=u", want: "CompleteMultipartUpload"},
		"upload a part":       {method: http.MethodPut, target: "/b/k?partNumber=1&uploadId=u", want: "UploadPart"},
		"plain object write":  {method: http.MethodPut, target: "/b/k", want: "PutObject"},
		"plain object read":   {method: http.MethodGet, target: "/b/k", want: "GetObject"},
		"plain object delete": {method: http.MethodDelete, target: "/b/k", want: "DeleteObject"},
	} {
		t.Run(name, func(t *testing.T) {
			var served string
			built := map[string]http.Handler{}
			for _, route := range s3api.Routes() {
				built[route.ID] = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = route.ID })
			}

			selectRoute(groups[tc.method], built).ServeHTTP(
				httptest.NewRecorder(), httptest.NewRequest(tc.method, tc.target, nil))
			if served != tc.want {
				t.Errorf("served %q, want %q", served, tc.want)
			}
		})
	}
}
