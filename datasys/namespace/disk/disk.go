package disk

import (
	"errors"
	"os"
	"path"
	"path/filepath"

	xrand "github.com/balamurugana/goat/pkg/rand"
)

func newTempName() string {
	return xrand.NewID(8).String()
}

type Disk struct {
	id       string
	storeDir string

	bucketsDir string
	tmpDir     string
	trashDir   string
}

func NewDisk(id, dir string) (*Disk, error) {
	storeDir := filepath.Clean(dir)
	if !filepath.IsAbs(storeDir) {
		pwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}

		storeDir, err = filepath.Abs(filepath.Join(pwd, storeDir))
		if err != nil {
			return nil, err
		}
	}

	storeDir = filepath.ToSlash(storeDir)

	filename := filepath.Join(storeDir, ".isWritable")
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	file.Close()
	if err = os.Remove(filename); err != nil {
		return nil, err
	}

	bucketsDir := path.Join(storeDir, "buckets")
	if err := os.Mkdir(bucketsDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	tmpDir := path.Join(storeDir, "tmp")
	if err := os.Mkdir(tmpDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	trashDir := path.Join(storeDir, "trash")
	if err := os.Mkdir(trashDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	return &Disk{
		id:         id,
		storeDir:   storeDir,
		bucketsDir: bucketsDir,
		tmpDir:     tmpDir,
		trashDir:   trashDir,
	}, nil
}

func (disk *Disk) ID() string {
	return disk.id
}

// Uploads CRUD
// * CreateUpload/RevertCreateUpload
// * UploadPart/RevertUploadPart
// * AbortUpload/RevertAbortUpload
// * CompleteUpload/RevertCompleteUpload
// * ListParts
// * ListUploads
//
//
// Object CRUD
// * PutObject
// * GetObject
// * DeleteObject
// * ListObject
// * ListObjects
//
// * SetObjectMetaData
// * GetObjectMetaData
// * DeleteObjectMetaData
// * ListObjectMetaData
//
