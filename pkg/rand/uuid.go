package rand

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

type uuid struct {
	value string
}

func parseUUID(s string) (u *uuid, err error) {
	// the string format should be either in
	// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (or)
	// xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

	// Remove double quotes if any
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		s = s[1 : len(s)-1]
	}

	if len(s) == 32 {
		if _, err = hex.DecodeString(s); err != nil {
			return nil, err
		}

		return &uuid{value: s}, nil
	}

	if len(s) == 36 {
		var us string

		if _, err = hex.DecodeString(s[0:8]); err != nil {
			return nil, err
		}
		us += s[0:8]

		if _, err = hex.DecodeString(s[9:13]); err != nil {
			return nil, err
		}
		us += s[9:13]

		if _, err = hex.DecodeString(s[14:18]); err != nil {
			return nil, err
		}
		us += s[14:18]

		if _, err = hex.DecodeString(s[19:23]); err != nil {
			return nil, err
		}
		us += s[19:23]

		if _, err = hex.DecodeString(s[24:]); err != nil {
			return nil, err
		}
		us += s[24:]

		return &uuid{value: us}, nil
	}

	return nil, errors.New("unknown UUID string " + s)
}

func newUUID() *uuid {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err)
	}

	// variant bits; for more info
	// see https://www.ietf.org/rfc/rfc4122.txt section 4.1.1
	b[8] = b[8]&0x3f | 0x80

	// version 4 (pseudo-random); for more info
	// see https://www.ietf.org/rfc/rfc4122.txt section 4.1.3
	b[6] = b[6]&0x0f | 0x40

	return &uuid{value: hex.EncodeToString(b)}
}

func (uuid uuid) String() string {
	return uuid.value
}

func (uuid uuid) Humanize() string {
	return uuid.value[0:4] + "-" + uuid.value[4:6] + "-" + uuid.value[6:8] + "-" + uuid.value[8:10] + "-" + uuid.value[10:]
}

func (uuid uuid) MarshalJSON() ([]byte, error) {
	return []byte(`"` + uuid.Humanize() + `"`), nil
}

func (uuid *uuid) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	u, err := parseUUID(s)
	if err != nil {
		return err
	}

	uuid.value = u.value

	return nil
}

// UUID is random 128 bits (16 bytes) UUID
type UUID struct {
	*uuid
}

// NewUUID creates new random UUID
func NewUUID() UUID {
	return UUID{newUUID()}
}

// Equal returns given uuid is equal or not.
func (uuid UUID) Equal(uuid2 UUID) bool {
	return uuid.uuid.value == uuid2.uuid.value
}

// ParseUUID parses string to UUID.
func ParseUUID(s string) (u UUID, err error) {
	var uu *uuid
	if uu, err = parseUUID(s); err == nil {
		u = UUID{uu}
	}

	return u, err
}
