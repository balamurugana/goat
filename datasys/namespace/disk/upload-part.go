package disk

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/balamurugana/goat/datasys/dataspace/disk"
	xerrors "github.com/balamurugana/goat/datasys/errors"
	"github.com/balamurugana/goat/datasys/namespace/s3"
	xhash "github.com/balamurugana/goat/pkg/hash"
	xos "github.com/balamurugana/goat/pkg/os"
)

func (disk *Disk) UploadPart(bucketName, objectName string, uploadID disk.UploadID, partNumber uint, partInfo *s3.Part, dataInfo []byte) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if !xos.Exist(bucketDir) {
		return xerrors.ErrBucketNotFound
	}

	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return xerrors.ErrUploadIDNotFound
	}

	tempPartFile := path.Join(disk.tmpDir, fmt.Sprintf("%v.part.%v", partNumber, newTempName()))
	if err := xos.WriteJSONFile(tempPartFile, partInfo); err != nil {
		return err
	}

	tempDataInfoFile := path.Join(disk.tmpDir, fmt.Sprintf("%v.datainfo.%v", partNumber, newTempName()))
	if err := ioutil.WriteFile(tempDataInfoFile, dataInfo, 0644); err != nil {
		return err
	}

	partFile := path.Join(uploadIDDir, fmt.Sprintf("%v.part", partNumber))
	dataInfoFile := path.Join(uploadIDDir, fmt.Sprintf("%v.datainfo", partNumber))
	trashPartFile := path.Join(disk.trashDir, fmt.Sprintf("%v.%v.part", uploadID, partNumber))
	trashDataInfoFile := path.Join(disk.trashDir, fmt.Sprintf("%v.%v.datainfo", uploadID, partNumber))

	if err := os.Rename(partFile, trashPartFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err := os.Rename(dataInfoFile, trashDataInfoFile); err != nil {
		os.Rename(trashDataInfoFile, dataInfoFile)
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	if err := os.Rename(tempPartFile, partFile); err != nil {
		os.Rename(trashPartFile, partFile)
		os.Rename(trashDataInfoFile, dataInfoFile)
		return err
	}

	if err := os.Rename(tempDataInfoFile, dataInfoFile); err != nil {
		os.Remove(partFile)
		os.Rename(trashPartFile, partFile)
		os.Rename(trashDataInfoFile, dataInfoFile)
		return err
	}

	return nil
}

func (disk *Disk) RevertUploadPart(bucketName, objectName string, uploadID disk.UploadID, partNumber uint) error {
	trashPartFile := path.Join(disk.trashDir, fmt.Sprintf("%v.%v.part", uploadID, partNumber))
	trashDataInfoFile := path.Join(disk.trashDir, fmt.Sprintf("%v.%v.datainfo", uploadID, partNumber))

	bucketDir := path.Join(disk.bucketsDir, bucketName)
	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	partFile := path.Join(uploadIDDir, fmt.Sprintf("%v.part", partNumber))
	dataInfoFile := path.Join(uploadIDDir, fmt.Sprintf("%v.datainfo", partNumber))

	os.Remove(partFile)
	os.Remove(dataInfoFile)

	if !xos.Exist(trashPartFile) {
		return nil
	}

	err1 := os.Rename(trashPartFile, partFile)
	err2 := os.Rename(trashDataInfoFile, dataInfoFile)

	return mergeErrors(err1, err2)
}
