package disk

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/balamurugana/goat/datasys/dataspace/disk"
	xerrors "github.com/balamurugana/goat/datasys/errors"
	"github.com/balamurugana/goat/datasys/namespace/s3"
	xhash "github.com/balamurugana/goat/pkg/hash"
	xos "github.com/balamurugana/goat/pkg/os"
)

func mergeErrors(errs ...error) error {
	var index int
	var s []string
	for i, err := range errs {
		if err != nil {
			index = i
			s = append(s, err.Error())
		}
	}

	switch len(s) {
	case 0:
		return nil
	case 1:
		return errs[index]
	}

	return fmt.Errorf("too many errors; [%v]", strings.Join(s, ", "))
}

//
// Multipart upload directory structure.
//
// STORE_DIR/
// `-- buckets/
//     `-- BUCKET/
//         |-- objects/
//         |-- multipart/
//         |   `-- OBJECT/
//         |       |-- UPLOADID.yjf-iAepYVVTZIyO6tXZ1Ghz6iAZOwmVtGqg6y_or2s.default
//         |       `-- UPLOADID.Q5C0uOv_8zmsnKK27G_pCfdaDWyJnaWyJFbwKUNEMzM.default
//         `-- uploadids/
//             `-- OBJECTHASH/
//                 `-- UPLOADID/
//                     `-- N.part
//

func (disk *Disk) GetUpload(bucketName, objectName string, uploadID disk.UploadID) (*s3.Upload, error) {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if !xos.Exist(bucketDir) {
		return nil, xerrors.ErrBucketNotFound
	}

	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return nil, xerrors.ErrUploadIDNotFound
	}

	var uploadInfo s3.Upload
	metaDataFile := path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+objectID)
	if strings.HasSuffix(objectName, "/") {
		metaDataFile = path.Join(bucketDir, "multipart", objectName, uploadID.String()+"."+slashObjectID)
	}
	if err := xos.ReadJSONFile(metaDataFile, -1, &uploadInfo); err != nil {
		return nil, err
	}

	return &uploadInfo, nil
}

func (disk *Disk) GetParts(bucketName, objectName string, uploadID disk.UploadID, partNumbers []uint) (partInfos []*s3.Part, dataInfos [][]byte, err error) {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())

	partInfos = make([]*s3.Part, len(partNumbers))
	dataInfos = make([][]byte, len(partNumbers))
	errs := make([]error, len(partNumbers))

	for i, partNumber := range partNumbers {
		var partInfo s3.Part
		partFile := path.Join(uploadIDDir, fmt.Sprintf("%v.part", partNumber))
		if err := xos.ReadJSONFile(partFile, -1, &partInfo); err != nil {
			errs[i] = err
			continue
		}

		dataInfoFile := path.Join(uploadIDDir, fmt.Sprintf("%v.datainfo", partNumber))
		data, err := ioutil.ReadFile(dataInfoFile)
		if err != nil {
			errs[i] = err
			continue
		}

		partInfos[i] = &partInfo
		dataInfos[i] = data
	}

	errCount := 0
	for i := range errs {
		if errs[i] != nil {
			errCount++
		}
	}

	if errCount == len(partNumbers) {
		return nil, nil, fmt.Errorf("multiple errors; %v", errs)
	}

	return partInfos, dataInfos, nil
}

func (disk *Disk) ListParts(bucketName, objectName string, uploadID disk.UploadID, maxParts, partNumberMarker uint) ([]int, []*s3.Part, int, error) {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if !xos.Exist(bucketDir) {
		return nil, nil, 0, xerrors.ErrBucketNotFound
	}

	objectNameHash := xhash.SumInBase64(objectName)
	uploadIDDir := path.Join(bucketDir, "uploadids", objectNameHash, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return nil, nil, 0, xerrors.ErrUploadIDNotFound
	}

	partNumbers := []int{}
	picker := func(name string, mode os.FileMode) (stop bool) {
		if strings.HasSuffix(name, ".part") {
			if partNumber, err := strconv.Atoi(strings.TrimSuffix(name, ".part")); err == nil {
				partNumbers = append(partNumbers, partNumber)
			}
		}

		return false
	}

	if err := xos.Readdirnames(uploadIDDir, picker); err != nil {
		return nil, nil, 0, err
	}

	sort.Ints(partNumbers)
	i := sort.SearchInts(partNumbers, int(partNumberMarker))
	if i == len(partNumbers) {
		return nil, nil, 0, fmt.Errorf("no more parts")
	}

	if i > 0 {
		if partNumbers[i] != int(partNumberMarker) {
			return nil, nil, 0, fmt.Errorf("part not found")
		}

		i++
	}

	if i >= len(partNumbers) {
		return nil, nil, 0, nil
	}

	partNumbers = partNumbers[i:]
	nextPartNumberMarker := 0
	if len(partNumbers) > int(maxParts) {
		partNumbers = partNumbers[:maxParts]
		nextPartNumberMarker = partNumbers[maxParts-1]
	}

	partInfos := make([]*s3.Part, len(partNumbers))
	for i := range partNumbers {
		partFile := path.Join(uploadIDDir, fmt.Sprintf("%v.part", partNumbers[i]))

		var partInfo s3.Part
		if err := xos.ReadJSONFile(partFile, -1, &partInfo); err == nil {
			partInfos[i] = &partInfo
		}
	}

	return partNumbers, partInfos, nextPartNumberMarker, nil

	// <NextPartNumberMarker>integer</NextPartNumberMarker>
	// <IsTruncated>boolean</IsTruncated>
	// <Part>
	//    <ETag>string</ETag>
	//    <LastModified>timestamp</LastModified>
	//    <PartNumber>integer</PartNumber>
	//    <Size>integer</Size>
	// </Part>

}
