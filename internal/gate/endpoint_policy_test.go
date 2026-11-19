package gate

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore/endpointproxy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The callers these tests sign as, each resolved by the gate's own authenticator
// from IAM records as Spinifex writes them.
const (
	policyAccount = acctOwner
	aliceKey      = "AKIAALICEENDPOINT001"
	bobKey        = "AKIABOBENDPOINT00001"
	readerSession = "ASIAREADERSESSION001"
	writerSession = "ASIAWRITERSESSION001"
	serviceKey    = "AKIASERVICEACCOUNT01"
	callerSecret  = "endpoint-policy-test-secret"
)

var (
	aliceARN  = "arn:aws:iam::" + policyAccount + ":user/alice"
	readerARN = "arn:aws:iam::" + policyAccount + ":role/reader"
)

// seedIAM writes the records the authenticator reads: two users and two role
// sessions, each holding an identity policy that allows all of S3.
func seedIAM(t *testing.T, js jetstream.JetStream, key *masterkey.Key) {
	t.Helper()
	ctx := t.Context()
	bucket := func(name string) jetstream.KeyValue {
		kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name})
		require.NoError(t, err)
		return kv
	}
	put := func(kv jetstream.KeyValue, k string, v any) {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		_, err = kv.Put(ctx, k, b)
		require.NoError(t, err)
	}
	sealed, err := key.EncryptBase64(callerSecret)
	require.NoError(t, err)

	policyARN := "arn:aws:iam::" + policyAccount + ":policy/S3Full"
	put(bucket("spinifex-iam-policies"), policyAccount+".S3Full", map[string]string{
		"policy_name": "S3Full", "arn": policyARN,
		"policy_document": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
	})
	put(bucket("spinifex-accounts"), policyAccount, map[string]string{"status": "ACTIVE"})

	keys, users := bucket("spinifex-iam-access-keys"), bucket("spinifex-iam-users")
	for name, ak := range map[string]string{"alice": aliceKey, "bob": bobKey} {
		put(keys, ak, map[string]string{
			"access_key_id": ak, "secret_access_key": sealed,
			"user_name": name, "account_id": policyAccount, "status": "Active",
		})
		put(users, policyAccount+"."+name, map[string]any{
			"user_name": name, "user_id": "AIDA" + name, "account_id": policyAccount,
			"arn": "arn:aws:iam::" + policyAccount + ":user/" + name, "attached_policies": []string{policyARN},
		})
	}

	roles, sessions := bucket("spinifex-iam-roles"), bucket("spinifex-iam-session-credentials")
	for role, ak := range map[string]string{"reader": readerSession, "writer": writerSession} {
		put(roles, policyAccount+"."+role, map[string]any{
			"role_name": role, "account_id": policyAccount, "attached_policies": []string{policyARN},
		})
		put(sessions, ak, map[string]any{
			"access_key_id": ak, "secret_encrypted": sealed, "account_id": policyAccount,
			"principal_type": "assumed-role", "session_name": "i-0123456789abcdef0",
			"underlying_role_arn": "arn:aws:iam::" + policyAccount + ":role/" + role,
			"assumed_role_id":     "AROA" + role + ":i-0123456789abcdef0",
			"expires_at":          time.Now().Add(time.Hour),
		})
	}
}

// policyGate is the whole gate behind the endpoint listener, authenticating
// against an embedded NATS that holds seedIAM's records, with one service
// account in config as a node's Predastore has.
func policyGate(t *testing.T) (addr string, pki endpointPKI, gate *Server) {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
	})
	require.NoError(t, err)
	go ns.Start()
	t.Cleanup(ns.Shutdown)
	require.True(t, ns.ReadyForConnections(10*time.Second), "embedded NATS did not start")

	raw := make([]byte, masterkey.MasterKeySize)
	_, err = rand.Read(raw)
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "master.key")
	require.NoError(t, os.WriteFile(keyPath, raw, 0o600))
	key, err := masterkey.LoadShared(keyPath)
	require.NoError(t, err)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	seedIAM(t, js, key)

	iam, err := auth.NewNATSIAMProvider(&auth.IAMConfig{NATSUrl: ns.ClientURL(), MasterKeyPath: keyPath})
	require.NoError(t, err)
	t.Cleanup(iam.Close)
	creds := auth.NewChainProvider(iam, auth.NewConfigProvider([]auth.Entry{{
		AccessKeyID: serviceKey, SecretAccessKey: callerSecret, AccountID: policyAccount,
	}}))

	gate = newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: policyAccount, OwnerID: keyCreator}),
		CredProv: creds,
	})
	pki = newEndpointPKI(t)
	addr, _, _ = endpointServerFor(t, pki, gate)
	return addr, pki, gate
}

// policyHeaderFor is the endpoint's header at contract version 2 carrying policy.
func policyHeaderFor(t *testing.T, policy string) []byte {
	t.Helper()
	h := testHeader()
	h.Version = endpointproxy.ContractVersion2
	h.PolicyGeneration = 1
	h.Policy = policy
	b, err := h.Marshal()
	require.NoError(t, err)
	return b
}

// throughEndpoint sends one signed request over a fresh endpoint session, as
// a guest behind the proxy would, and returns the response.
func throughEndpoint(t *testing.T, addr string, pki endpointPKI, header []byte,
	accessKey, method, target string, body []byte) (int, []byte, error) {
	t.Helper()
	stream, err := dialEndpoint(t, addr, pki, &pki.node, header)
	if err != nil {
		return 0, nil, err
	}
	defer stream.Close()
	guest := guestTLS(stream, pki)
	req, err := http.NewRequest(method, "https://127.0.0.1"+target, bytes.NewReader(body))
	require.NoError(t, err)
	signTestReq(t, req, body, accessKey, callerSecret, "ap-southeast-2", "s3")
	if err := req.Write(guest); err != nil {
		return 0, nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(guest), req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, got, nil
}

// batchOutcome deletes public/a and secret/b in one batch and returns the keys
// deleted and the keys refused.
func batchOutcome(t *testing.T, addr string, pki endpointPKI, header []byte, accessKey string) (deleted, refused []string) {
	t.Helper()
	body := []byte("<Delete><Object><Key>public/a</Key></Object><Object><Key>secret/b</Key></Object></Delete>")
	status, got, err := throughEndpoint(t, addr, pki, header, accessKey, http.MethodPost, "/owner-bucket?delete=", body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, string(got))
	var result handlers.DeleteResult
	require.NoError(t, xml.Unmarshal(got, &result))
	for _, d := range result.Deleted {
		deleted = append(deleted, d.Key)
	}
	for _, e := range result.Errors {
		assert.Equal(t, string(model.ErrAccessDenied), e.Code, "key %s", e.Key)
		refused = append(refused, e.Key)
	}
	return deleted, refused
}

// endpointPolicyFor lets principal list the bucket and delete under public/,
// naming the caller as the statement's principal or by aws:PrincipalArn.
func endpointPolicyFor(principal string, arnCondition string) string {
	condition := ""
	if arnCondition != "" {
		condition = `,"Condition":{"ArnEquals":{"aws:PrincipalArn":"` + arnCondition + `"}}`
	}
	return `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":` + principal + `,"Action":"s3:ListBucket","Resource":"arn:aws:s3:::owner-bucket"` + condition + `},` +
		`{"Effect":"Allow","Principal":` + principal + `,"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::owner-bucket/public/*"` + condition + `}]}`
}

// The endpoint policy decides for callers the authenticator resolved, at the
// request check and per key of a batch, as on AWS.
func TestEndpointPolicy_DecidesForAuthenticatedCallers(t *testing.T) {
	addr, pki, _ := policyGate(t)
	for name, tc := range map[string]struct {
		policy           string
		allowed, refused string
	}{
		"aws:PrincipalArn naming a user": {
			endpointPolicyFor(`"*"`, aliceARN), aliceKey, bobKey,
		},
		"aws:PrincipalArn naming a role matches its sessions only": {
			endpointPolicyFor(`"*"`, readerARN), readerSession, writerSession,
		},
		"aws:PrincipalArn names a role, not its session": {
			endpointPolicyFor(`"*"`, "arn:aws:sts::"+policyAccount+":assumed-role/reader/i-0123456789abcdef0"), "", readerSession,
		},
	} {
		t.Run(name, func(t *testing.T) {
			header := policyHeaderFor(t, tc.policy)
			if tc.allowed != "" {
				status, body, err := throughEndpoint(t, addr, pki, header, tc.allowed, http.MethodGet, "/owner-bucket", nil)
				require.NoError(t, err)
				assert.Equal(t, http.StatusOK, status, string(body))
				deleted, refused := batchOutcome(t, addr, pki, header, tc.allowed)
				assert.Equal(t, []string{"public/a"}, deleted)
				assert.Equal(t, []string{"secret/b"}, refused)
			}
			status, body, err := throughEndpoint(t, addr, pki, header, tc.refused, http.MethodGet, "/owner-bucket", nil)
			require.NoError(t, err)
			assert.Equal(t, http.StatusForbidden, status, string(body))
			deleted, refused := batchOutcome(t, addr, pki, header, tc.refused)
			assert.Empty(t, deleted)
			assert.ElementsMatch(t, []string{"public/a", "secret/b"}, refused)
		})
	}
}

// An account principal names the account's root user only, so it grants an IAM
// user of that account nothing, by account ID or by root ARN.
func TestEndpointPolicy_AccountPrincipalDoesNotNameAUser(t *testing.T) {
	addr, pki, _ := policyGate(t)
	for _, principal := range []string{
		`{"AWS":"` + policyAccount + `"}`,
		`{"AWS":"arn:aws:iam::` + policyAccount + `:root"}`,
	} {
		header := policyHeaderFor(t, endpointPolicyFor(principal, ""))
		status, body, err := throughEndpoint(t, addr, pki, header, aliceKey, http.MethodGet, "/owner-bucket", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusForbidden, status, "%s: %s", principal, body)
		deleted, refused := batchOutcome(t, addr, pki, header, aliceKey)
		assert.Empty(t, deleted, principal)
		assert.ElementsMatch(t, []string{"public/a", "secret/b"}, refused, principal)
	}
}

// AWS applies an endpoint policy to every principal, so a service account that
// skips identity policy is still held to it.
func TestEndpointPolicy_HoldsAServiceAccount(t *testing.T) {
	addr, pki, _ := policyGate(t)
	header := policyHeaderFor(t, endpointPolicyFor(`"*"`, aliceARN))

	status, body, err := throughEndpoint(t, addr, pki, header, serviceKey, http.MethodGet, "/owner-bucket", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, status, string(body))

	// Without an endpoint policy the same caller is served, so the refusal above
	// is the endpoint policy's.
	v1, err := testHeader().Marshal()
	require.NoError(t, err)
	status, body, err = throughEndpoint(t, addr, pki, v1, serviceKey, http.MethodGet, "/owner-bucket", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status, string(body))
}

// malformedDeny is a broad Allow beside a Deny whose NotIpAddress operand is not
// an address: refused at write, and a Deny that still denies if it gets past.
const malformedDeny = `{"Version":"2012-10-17","Statement":[` +
	`{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"},` +
	`{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":"*",` +
	`"Condition":{"NotIpAddress":{"aws:VpcSourceIp":"not-an-address"}}}]}`

// A policy the gate would not enforce as written closes the session, which the
// proxy reads as a failed check, so it is never served under a narrower reading.
func TestEndpointPolicy_UnenforceablePolicyClosesTheSession(t *testing.T) {
	addr, pki, _ := policyGate(t)
	doc, err := iampolicy.ParseEndpointPolicy([]byte(malformedDeny))
	require.NoError(t, err)
	require.Error(t, iampolicy.CheckEndpointPolicy(doc), "the write path must refuse this policy")

	for name, policy := range map[string]string{
		"malformed operand": malformedDeny,
		"not JSON":          "{",
		"named user principal": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":"` + aliceARN + `"},"Action":"s3:*","Resource":"*"}]}`,
	} {
		_, _, err := throughEndpoint(t, addr, pki, policyHeaderFor(t, policy), aliceKey, http.MethodGet, "/owner-bucket", nil)
		assert.Error(t, err, "%s: the gate served a request under a policy it cannot enforce", name)
	}
}

// Injected past both the write path and the handshake, the malformed Deny still
// denies: an operand the evaluator cannot read fails closed.
func TestEndpointPolicy_MalformedDenyFailsClosedAtTheGate(t *testing.T) {
	_, _, gate := policyGate(t)
	doc, err := iampolicy.ParseEndpointPolicy([]byte(malformedDeny))
	require.NoError(t, err)

	for name, policy := range map[string]*iampolicy.PolicyDocument{"no endpoint policy": nil, "malformed deny": &doc} {
		req := httptest.NewRequest(http.MethodGet, "/owner-bucket", nil)
		h := testHeader()
		req.RemoteAddr = h.Source.String()
		req = req.WithContext(withEndpoint(context.Background(), h, policy))
		signTestReq(t, req, nil, aliceKey, callerSecret, "ap-southeast-2", "s3")
		rr := httptest.NewRecorder()
		gate.ServeHTTP(rr, req)
		want := http.StatusForbidden
		if policy == nil {
			want = http.StatusOK
		}
		assert.Equal(t, want, rr.Code, "%s: %s", name, rr.Body.String())
	}
}
