package store_test

import (
	"testing"

	"pgregory.net/rapid"
)

type storeReference map[string][]byte

func (stRef *storeReference) Lookup(key string) objRef
