package auth_test

import (
	"testing"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRoleARN(t *testing.T) {
	tests := []struct {
		name        string
		arn         string
		wantAccount string
		wantName    string
		wantErr     bool
	}{
		{"simple", "arn:aws:iam::000000000001:role/MyRole", "000000000001", "MyRole", false},
		{"nested path", "arn:aws:iam::000000000001:role/some/path/MyRole", "000000000001", "MyRole", false},
		{"role prefix only", "arn:aws:iam::000000000001:role/", "", "", true},
		{"trailing slash, empty name", "arn:aws:iam::000000000001:role/path/", "", "", true},
		{"empty account", "arn:aws:iam:::role/MyRole", "", "", true},
		{"not a role resource", "arn:aws:iam::000000000001:user/Bob", "", "", true},
		{"region present", "arn:aws:iam:us-east-1:000000000000:role/app", "", "", true},
		{"wrong service", "arn:aws:s3:::000000000001:role/MyRole", "", "", true},
		{"too few fields", "arn:aws:iam::000000000001", "", "", true},
		{"junk", "not-an-arn", "", "", true},
		{"empty", "", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, name, err := auth.ParseRoleARN(tt.arn)
			if tt.wantErr {
				require.Error(t, err)
				assert.Empty(t, account, "account must be empty on error")
				assert.Empty(t, name, "name must be empty on error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantAccount, account, "account")
			assert.Equal(t, tt.wantName, name, "name")
		})
	}
}

func TestExtractPolicyName(t *testing.T) {
	tests := []struct {
		arn  string
		want string
	}{
		{"arn:aws:iam::000000000001:policy/AdministratorAccess", "AdministratorAccess"},
		{"arn:aws:iam::000000000001:policy/path/to/MyPolicy", "MyPolicy"},
		{"arn:aws:iam::000000000001:policy/", ""},
		{"invalid-arn", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := auth.ExtractPolicyName(tt.arn)
		assert.Equal(t, tt.want, got, "ExtractPolicyName(%q)", tt.arn)
	}
}
