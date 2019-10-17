package disk

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/balamurugana/goat/datasys/dataspace/disk"
	xerrors "github.com/balamurugana/goat/datasys/errors"
	"github.com/balamurugana/goat/datasys/namespace/s3"
	xhash "github.com/balamurugana/goat/pkg/hash"
	xos "github.com/balamurugana/goat/pkg/os"
)

// CreateUpload creates new multipart upload of given object. Below paths are created.
// Directory: <STORE_DIR>/buckets/<BUCKET>/uploadids/<OBJECTHASH>/<UPLOADID>/
// File: <STORE_DIR>/buckets/<BUCKET>/multipart/<OBJECT>/<UPLOADID>.<OBJECTID>
func (disk *Disk) CreateUpload(bucketName, objectName string, uploadID disk.UploadID, uploadInfo *s3.Upload) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if !xos.Exist(bucketDir) {
		return xerrors.ErrBucketNotFound
	}

	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	if err := xos.CreatePath(uploadIDDir, "", true); err != nil {
		if errors.Is(err, os.ErrExist) {
			err = xerrors.ErrUploadIDAlreadyExist
		}

		return err
	}

	tempMetaDataFile := path.Join(disk.tmpDir, fmt.Sprintf("%v.%v.%v", objectNameHash, uploadID.String(), newTempName()))
	if err := xos.WriteJSONFile(tempMetaDataFile, uploadInfo); err != nil {
		xos.RemovePath(uploadIDDir, path.Join(bucketDir, "uploadids"), false)
		return err
	}

	metaDataFile := path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+objectID)
	if strings.HasSuffix(objectName, "/") {
		metaDataFile = path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+slashObjectID)
	}
	if err := xos.CreatePath(metaDataFile, tempMetaDataFile, false); err != nil {
		xos.RemovePath(uploadIDDir, path.Join(bucketDir, "uploadids"), false)
		xos.RemovePath(metaDataFile, path.Join(bucketDir, "multipart"), false)
		return err
	}

	return nil
}

// RevertCreateUpload undoes successfully created upload.
func (disk *Disk) RevertCreateUpload(bucketName, objectName string, uploadID disk.UploadID) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	objectNameHash := xhash.SumInBase64(objectName)

	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	err1 := xos.RemovePath(uploadIDDir, path.Join(bucketDir, "uploadids"), false)

	metaDataFile := path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+objectID)
	if strings.HasSuffix(objectName, "/") {
		metaDataFile = path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+slashObjectID)
	}
	err2 := xos.RemovePath(metaDataFile, path.Join(bucketDir, "multipart"), false)

	return mergeErrors(err1, err2)
}
