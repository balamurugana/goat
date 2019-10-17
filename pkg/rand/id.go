package rand

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
)

// ID is random unique ID string created using random bytes.
type ID struct {
	value string
}

// NewID is random unique ID created using random bytes of given length.
func NewID(length int) *ID {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	return &ID{value: base64.RawURLEncoding.EncodeToString(b)}
}

func (id ID) String() string {
	return id.value
}

// MarshalJSON returns JSON encoded value of this ID.
func (id ID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + id.value + `"`), nil
}

// UnmarshalJSON decodes JSON value to ID.
func (id *ID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	id.value = s
	return nil
}
