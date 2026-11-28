package model

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// MultipartPlacement stores the upload UUID and part number for one placement.
type MultipartPlacement [18]byte

// ParseMultipartPlacement accepts the canonical key in the objects table.
func ParseMultipartPlacement(key string) (MultipartPlacement, error) {
	var part MultipartPlacement
	if len(key) != 47 || !strings.HasPrefix(key, "part:") || key[41] != ':' {
		return part, errors.New("invalid multipart placement key")
	}
	upload := strings.ReplaceAll(key[5:41], "-", "")
	number, err := strconv.ParseUint(key[42:], 10, 16)
	if len(upload) != 32 || err != nil || number < 1 || number > 10000 {
		return part, errors.New("invalid multipart placement key")
	}
	if _, err := hex.Decode(part[:16], []byte(upload)); err != nil {
		return MultipartPlacement{}, fmt.Errorf("invalid multipart upload UUID: %w", err)
	}
	binary.BigEndian.PutUint16(part[16:], uint16(number))
	if part.Key() != key {
		return MultipartPlacement{}, errors.New("invalid multipart placement key")
	}
	return part, nil
}

// Key returns the exact placement key, without its table prefix.
func (part MultipartPlacement) Key() string {
	n := binary.BigEndian.Uint16(part[16:])
	if n < 1 || n > 10000 {
		return ""
	}
	return fmt.Sprintf("part:%x-%x-%x-%x-%x:%05d", part[:4], part[4:6], part[6:8], part[8:10], part[10:16], n)
}
