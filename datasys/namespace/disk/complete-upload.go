package disk

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/balamurugana/goat/datasys/dataspace/disk"
	xerrors "github.com/balamurugana/goat/datasys/errors"
	"github.com/balamurugana/goat/datasys/namespace/s3"
	xhash "github.com/balamurugana/goat/pkg/hash"
	xos "github.com/balamurugana/goat/pkg/os"
)

func (disk *Disk) CompleteUpload(bucketName, objectName string, uploadID disk.UploadID, objectInfo *s3.Object, dataInfo []byte, versionID disk.VersionID, isDefault bool) (bool, error) {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if !xos.Exist(bucketDir) {
		return false, xerrors.ErrBucketNotFound
	}

	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return false, xerrors.ErrUploadIDNotFound
	}

	tempVersionFile := path.Join(disk.tmpDir, fmt.Sprintf("%v.%v.%v", objectNameHash, versionID, newTempName()))
	if err := xos.WriteJSONFile(tempVersionFile, objectInfo); err != nil {
		return false, err
	}

	tempDataInfoFile := path.Join(disk.tmpDir, fmt.Sprintf("%v.%v.datainfo.%v", objectNameHash, versionID, newTempName()))
	if err := ioutil.WriteFile(tempDataInfoFile, dataInfo, 0644); err != nil {
		return false, err
	}

	tempDefaultVersionFile := path.Join(disk.tmpDir, fmt.Sprintf("%v.default.%v", objectNameHash, newTempName()))
	if err := ioutil.WriteFile(tempDefaultVersionFile, []byte(versionID.String()), 0644); err != nil {
		return false, err
	}

	objectDir := path.Join(bucketDir, "objects", objectName)

	versionFile := path.Join(objectDir, versionID.String())
	if err := xos.CreatePath(versionFile, tempVersionFile, false); err != nil {
		return false, err
	}

	dataInfoFile := path.Join(objectDir, versionID.String()+".datainfo")
	if err := xos.CreatePath(dataInfoFile, tempDataInfoFile, false); err != nil {
		xos.RemovePath(versionFile, path.Join(bucketDir, "objects"), false)
		return false, err
	}

	defaultFile := path.Join(objectDir, objectID)
	if strings.HasSuffix(objectName, "/") {
		defaultFile = path.Join(objectDir, slashObjectID)
	}
	trashDefaultFile := path.Join(disk.trashDir, fmt.Sprintf("%v.default.%v", objectNameHash, versionID))

	defaultExists := xos.Exist(defaultFile)

	if isDefault || !defaultExists {
		if err := os.Rename(defaultFile, trashDefaultFile); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				xos.RemovePath(versionFile, path.Join(bucketDir, "objects"), false)
				xos.RemovePath(dataInfoFile, path.Join(bucketDir, "objects"), false)
				return false, err
			}
		}

		if err := os.Rename(tempDefaultVersionFile, defaultFile); err != nil {
			if !defaultExists {
				xos.RemovePath(defaultFile, path.Join(bucketDir, "objects"), false)
			}
			xos.RemovePath(versionFile, path.Join(bucketDir, "objects"), false)
			xos.RemovePath(dataInfoFile, path.Join(bucketDir, "objects"), false)
			return false, err
		}
	}

	if err := disk.AbortUpload(bucketName, objectName, uploadID); err != nil {
		if isDefault {
			os.Rename(trashDefaultFile, defaultFile)
		} else if !defaultExists {
			xos.RemovePath(defaultFile, path.Join(bucketDir, "objects"), false)
		}

		xos.RemovePath(versionFile, path.Join(bucketDir, "objects"), false)
		xos.RemovePath(dataInfoFile, path.Join(bucketDir, "objects"), false)
		return false, err
	}

	return !defaultExists, nil
}

func (disk *Disk) RevertCompleteUpload(bucketName, objectName string, uploadID disk.UploadID, versionID disk.VersionID, isDefault bool) error {
	var err1, err2, err3, err4 error

	err1 = disk.RevertAbortUpload(bucketName, objectName, uploadID)

	bucketDir := path.Join(disk.bucketsDir, bucketName)
	objectNameHash := xhash.SumInBase64(objectName)
	objectDir := path.Join(bucketDir, "objects", objectName)

	if isDefault {
		defaultFile := path.Join(objectDir, objectID)
		if strings.HasSuffix(objectName, "/") {
			defaultFile = path.Join(objectDir, slashObjectID)
		}
		trashDefaultFile := path.Join(disk.trashDir, fmt.Sprintf("%v.default.%v", objectNameHash, versionID))
		if err2 = os.Rename(trashDefaultFile, defaultFile); errors.Is(err2, os.ErrNotExist) {
			err2 = xos.RemovePath(defaultFile, path.Join(bucketDir, "objects"), false)
		}
	}

	versionFile := path.Join(objectDir, versionID.String())
	err3 = xos.RemovePath(versionFile, path.Join(bucketDir, "objects"), false)
	dataInfoFile := path.Join(objectDir, versionID.String()+".datainfo")
	err4 = xos.RemovePath(dataInfoFile, path.Join(bucketDir, "objects"), false)

	return mergeErrors(err1, err2, err3, err4)
}
