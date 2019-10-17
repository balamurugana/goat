package disk

import (
	"os"
	"path"
	"strings"

	"github.com/balamurugana/goat/datasys/dataspace/disk"
	xerrors "github.com/balamurugana/goat/datasys/errors"
	xhash "github.com/balamurugana/goat/pkg/hash"
	xos "github.com/balamurugana/goat/pkg/os"
)

func (disk *Disk) AbortUpload(bucketName, objectName string, uploadID disk.UploadID) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if !xos.Exist(bucketDir) {
		return xerrors.ErrBucketNotFound
	}

	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return xerrors.ErrUploadIDNotFound
	}

	trashUploadIDDir := path.Join(disk.trashDir, objectNameHash+"."+uploadID.String())
	if err := os.Rename(uploadIDDir, trashUploadIDDir); err != nil {
		return err
	}

	objectDir := path.Join(bucketDir, "multipart", objectName)

	metaDataFile := path.Join(objectDir, uploadID.String()+"."+objectID)
	if strings.HasSuffix(objectName, "/") {
		metaDataFile = path.Join(objectDir, uploadID.String()+"."+slashObjectID)
	}
	trashMetaDataFile := path.Join(disk.trashDir, uploadID.String()+"."+objectID)
	if strings.HasSuffix(objectName, "/") {
		trashMetaDataFile = path.Join(disk.trashDir, uploadID.String()+"."+slashObjectID)
	}
	if err := os.Rename(metaDataFile, trashMetaDataFile); err != nil {
		os.Rename(trashUploadIDDir, uploadIDDir)
		return err
	}

	if err := xos.RemovePath(objectDir, path.Join(bucketDir, "multipart"), false); err != nil {
		xos.CreatePath(objectDir, "", false) // FIXME: handle errors here properly.
		os.Rename(trashMetaDataFile, metaDataFile)
		os.Rename(trashUploadIDDir, uploadIDDir)
		return err
	}

	// FIXME: cleanup trash dir

	return nil
}

func (disk *Disk) RevertAbortUpload(bucketName, objectName string, uploadID disk.UploadID) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	trashUploadIDDir := path.Join(disk.trashDir, objectNameHash+"."+uploadID.String())
	if err := os.Rename(trashUploadIDDir, uploadIDDir); err != nil {
		return err
	}

	trashMetaDataFile := path.Join(disk.trashDir, uploadID.String()+"."+objectID)
	if strings.HasSuffix(objectName, "/") {
		trashMetaDataFile = path.Join(disk.trashDir, uploadID.String()+"."+slashObjectID)
	}
	metaDataFile := path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+objectID)
	if strings.HasSuffix(objectName, "/") {
		metaDataFile = path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+slashObjectID)
	}
	return xos.CreatePath(metaDataFile, trashMetaDataFile, false)
}
