package disk

import "github.com/balamurugana/goat/pkg/rand"

// NewTempFilename returns new temporary filename.
func NewTempFilename() string {
	return rand.NewID(128).String()
}

type DataID struct {
	*rand.ID
}

func NewDataID() DataID {
	return DataID{rand.NewID(128)}
}

type UploadID struct {
	*rand.ID
}

func NewUploadID() UploadID {
	return UploadID{rand.NewID(128)}
}

type VersionID struct {
	*rand.ID
}

func NewVersionID() VersionID {
	return VersionID{rand.NewID(128)}
}
