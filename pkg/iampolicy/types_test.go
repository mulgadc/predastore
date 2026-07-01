package iampolicy_test

import (
	"encoding/json"
	"testing"

	"github.com/mulgadc/predastore/pkg/iampolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringOrArr_UnmarshalSingleString(t *testing.T) {
	var s iampolicy.StringOrArr
	require.NoError(t, json.Unmarshal([]byte(`"s3:GetObject"`), &s))
	assert.Equal(t, iampolicy.StringOrArr{"s3:GetObject"}, s)
}

func TestStringOrArr_UnmarshalArray(t *testing.T) {
	var s iampolicy.StringOrArr
	require.NoError(t, json.Unmarshal([]byte(`["s3:Get*","s3:List*"]`), &s))
	assert.Equal(t, iampolicy.StringOrArr{"s3:Get*", "s3:List*"}, s)
}

func TestStringOrArr_UnmarshalNull(t *testing.T) {
	// A JSON null yields a nil slice (an inert field), not [""].
	var s iampolicy.StringOrArr
	require.NoError(t, json.Unmarshal([]byte(`null`), &s))
	assert.Nil(t, s)
}

func TestStringOrArr_UnmarshalEmptyArray(t *testing.T) {
	var s iampolicy.StringOrArr
	require.NoError(t, json.Unmarshal([]byte(`[]`), &s))
	assert.Equal(t, iampolicy.StringOrArr{}, s)
}

func TestStringOrArr_MarshalSingleElement(t *testing.T) {
	data, err := json.Marshal(iampolicy.StringOrArr{"ec2:*"})
	require.NoError(t, err)
	assert.Equal(t, `"ec2:*"`, string(data))
}

func TestStringOrArr_MarshalMultipleElements(t *testing.T) {
	data, err := json.Marshal(iampolicy.StringOrArr{"s3:Get*", "s3:Put*"})
	require.NoError(t, err)
	assert.Equal(t, `["s3:Get*","s3:Put*"]`, string(data))
}

func TestStringOrArr_RoundTrip(t *testing.T) {
	for _, original := range []iampolicy.StringOrArr{
		{"iam:CreateUser"},
		{"ec2:Describe*", "ec2:Run*", "ec2:Stop*"},
	} {
		data, err := json.Marshal(original)
		require.NoError(t, err)

		var decoded iampolicy.StringOrArr
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, original, decoded)
	}
}

// TestStatement_UnmarshalStringOrArrayForms proves a full statement parses with
// Action/Resource given either as a bare string or an array.
func TestStatement_UnmarshalStringOrArrayForms(t *testing.T) {
	raw := `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"},
		{"Effect":"Deny","Action":["s3:PutObject","s3:DeleteObject"],"Resource":["arn:aws:s3:::b","arn:aws:s3:::b/*"]}
	]}`
	var doc iampolicy.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(raw), &doc))
	require.Len(t, doc.Statement, 2)
	assert.Equal(t, iampolicy.StringOrArr{"s3:GetObject"}, doc.Statement[0].Action)
	assert.Equal(t, iampolicy.StringOrArr{"s3:PutObject", "s3:DeleteObject"}, doc.Statement[1].Action)
	assert.Equal(t, iampolicy.EffectDeny, doc.Statement[1].Effect)
}
