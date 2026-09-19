package job

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"
)

// Cursor is the keyset position for List: the (created_at, id) of the last row
// of the previous page, newest first. On the wire it is an opaque base64url token.
type Cursor struct {
	CreatedAt time.Time
	ID        ID
}

// cursorLen is 8 bytes of unix nanoseconds + 16 bytes of uuid.
const cursorLen = 8 + 16

// IsZero reports whether the cursor points at the first page.
func (c Cursor) IsZero() bool { return c.CreatedAt.IsZero() && c.ID == ID{} }

// Encode returns the opaque page token; "" for the zero cursor.
func (c Cursor) Encode() string {
	if c.IsZero() {
		return ""
	}
	var buf [cursorLen]byte
	var nanos int64
	if !c.CreatedAt.IsZero() { // UnixNano is undefined for the zero time
		nanos = c.CreatedAt.UnixNano()
	}
	binary.BigEndian.PutUint64(buf[:8], uint64(nanos)) // two's complement round-trips
	copy(buf[8:], c.ID[:])
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

// DecodeCursor is the inverse of Encode. "" is the zero cursor; anything that
// was not produced by Encode fails with ErrInvalidCursor.
func DecodeCursor(token string) (Cursor, error) {
	if token == "" {
		return Cursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	if len(raw) != cursorLen {
		return Cursor{}, fmt.Errorf("%w: %d bytes, want %d", ErrInvalidCursor, len(raw), cursorLen)
	}
	var c Cursor
	if nanos := int64(binary.BigEndian.Uint64(raw[:8])); nanos != 0 { //nolint:gosec // two's complement, see Encode
		c.CreatedAt = time.Unix(0, nanos).UTC()
	}
	copy(c.ID[:], raw[8:])
	if c.IsZero() {
		return Cursor{}, fmt.Errorf("%w: zero cursor encoded", ErrInvalidCursor)
	}
	return c, nil
}
