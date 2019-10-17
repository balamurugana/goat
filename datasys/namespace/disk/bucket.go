package disk

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"

	xerrors "github.com/balamurugana/goat/datasys/errors"
	"github.com/balamurugana/goat/datasys/namespace/s3"
	xos "github.com/balamurugana/goat/pkg/os"
)

//
// Bucket directory structure.
// STORE_DIR/
// `-- buckets/
//     `-- BUCKET/
//         |-- objects/
//         |-- multipart/
//         `-- uploadids/
//

func (disk *Disk) ListBuckets() (map[string]*s3.Bucket, error) {
	bucketMap := make(map[string]*s3.Bucket)
	picker := func(name string, mode os.FileMode) (stop bool) {
		if mode.IsDir() {
			bucketMap[name] = nil
		}

		return false
	}

	if err := xos.Readdirnames(disk.bucketsDir, picker); err != nil {
		return nil, err
	}

	for bucket := range bucketMap {
		var bucketInfo s3.Bucket
		if err := xos.ReadJSONFile(path.Join(disk.bucketsDir, bucket, "bucket.json"), -1, &bucketInfo); err != nil {
			delete(bucketMap, bucket) // FIXME: handle error properly here.
			return nil, err
		}

		bucketMap[bucket] = &bucketInfo
	}

	return bucketMap, nil
}

func (disk *Disk) CreateBucket(bucketName string, bucketInfo *s3.Bucket, metaDataFiles map[string][]byte) error {
	tempBucketDir := path.Join(disk.tmpDir, bucketName+"."+newTempName())
	if err := os.Mkdir(tempBucketDir, 0755); err != nil {
		return err
	}

	objectsDir := path.Join(tempBucketDir, "objects")
	if err := os.Mkdir(objectsDir, 0755); err != nil {
		return err
	}

	multipartDir := path.Join(tempBucketDir, "multipart")
	if err := os.Mkdir(multipartDir, 0755); err != nil {
		return err
	}

	uploadidsDir := path.Join(tempBucketDir, "uploadids")
	if err := os.Mkdir(uploadidsDir, 0755); err != nil {
		return err
	}

	for filename, data := range metaDataFiles {
		if err := ioutil.WriteFile(path.Join(tempBucketDir, filename), data, 0644); err != nil {
			return err
		}
	}

	if err := xos.WriteJSONFile(path.Join(tempBucketDir, "bucket.json"), bucketInfo); err != nil {
		return err
	}

	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if err := os.Rename(tempBucketDir, bucketDir); err != nil {
		if errors.Is(err, os.ErrExist) {
			err = xerrors.ErrBucketAlreadyExist
		}

		return err
	}

	return nil
}

func (disk *Disk) RevertCreateBucket(bucketName string) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	trashBucketDir := path.Join(disk.trashDir, bucketName+"."+newTempName())
	return os.Rename(bucketDir, trashBucketDir)
}

func (disk *Disk) GetBucket(bucketName string) (*s3.Bucket, map[string][]byte, error) {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	if !xos.Exist(bucketDir) {
		return nil, nil, xerrors.ErrBucketNotFound
	}

	filenames := []string{}
	picker := func(name string, mode os.FileMode) (stop bool) {
		if mode.IsRegular() {
			filenames = append(filenames, name)
		}

		return false
	}

	if err := xos.Readdirnames(bucketDir, picker); err != nil {
		return nil, nil, err
	}

	metaDataFiles := make(map[string][]byte)
	for _, filename := range filenames {
		data, err := ioutil.ReadFile(path.Join(bucketDir, filename))
		if err != nil {
			return nil, nil, err
		}

		metaDataFiles[filename] = data
	}

	data, found := metaDataFiles["bucket.json"]
	if !found {
		return nil, nil, xerrors.ErrBucketNotFound
	}
	delete(metaDataFiles, "bucket.json")

	var bucketInfo s3.Bucket
	if err := json.Unmarshal(data, &bucketInfo); err != nil {
		return nil, nil, err
	}

	return &bucketInfo, metaDataFiles, nil
}

func (disk *Disk) DeleteBucket(bucketName string) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	trashBucketDir := path.Join(disk.trashDir, bucketName)

	isEmpty := func(dir string) (bool, error) {
		empty := true
		err := xos.Readdirnames(dir,
			func(name string, mode os.FileMode) (stop bool) {
				empty = false
				return true
			},
		)

		if err != nil {
			return false, err
		}

		return empty, nil
	}

	objectsDir := path.Join(bucketDir, "objects")
	empty, err := isEmpty(objectsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = xerrors.ErrBucketNotFound
		}

		return err
	}
	if !empty {
		return xerrors.ErrBucketNotEmpty
	}

	multipartDir := path.Join(bucketDir, "multipart")
	empty, err = isEmpty(multipartDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = xerrors.ErrBucketNotFound
		}

		return err
	}
	if !empty {
		return xerrors.ErrBucketNotEmpty
	}

	if err = os.Rename(bucketDir, trashBucketDir); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrBucketNotFound
	}

	// FIXME: cleanup trash dir

	return err
}

func (disk *Disk) RevertDeleteBucket(bucketName string) error {
	bucketDir := path.Join(disk.bucketsDir, bucketName)
	trashBucketDir := path.Join(disk.trashDir, bucketName)
	return os.Rename(trashBucketDir, bucketDir)
}

func (disk *Disk) BucketExist(bucketName string) bool {
	return xos.Exist(path.Join(disk.bucketsDir, bucketName))
}

func (disk *Disk) SetBucketMetaData(bucketName string, name string, data []byte) (err error) {
	tempMetaDataFile := path.Join(disk.tmpDir, bucketName+"."+name+"."+newTempName())
	if err = ioutil.WriteFile(tempMetaDataFile, data, 0644); err != nil {
		return err
	}

	metaDataFile := path.Join(disk.bucketsDir, bucketName, name)
	if err = os.Rename(tempMetaDataFile, metaDataFile); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrBucketNotFound
	}

	return err
}

func (disk *Disk) RevertSetBucketMetaData(bucketName string, name string) error {
	metaDataFile := path.Join(disk.bucketsDir, bucketName, name)
	trashMetaDataFile := path.Join(disk.trashDir, bucketName+"."+name+"."+newTempName())
	return os.Rename(metaDataFile, trashMetaDataFile)
}

func (disk *Disk) GetBucketMetaData(bucketName string, name string) (data []byte, err error) {
	metaDataFile := path.Join(disk.bucketsDir, bucketName, name)
	if data, err = ioutil.ReadFile(metaDataFile); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrBucketNotFound
	}

	// FIXME: differentiate bucket not found and meta data not found error.

	return data, err
}

func (disk *Disk) DeleteBucketMetaData(bucketName string, name string) (err error) {
	metaDataFile := path.Join(disk.bucketsDir, bucketName, name)
	trashMetaDataFile := path.Join(disk.trashDir, bucketName+"."+name)
	if err = os.Rename(metaDataFile, trashMetaDataFile); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrBucketNotFound
	}

	// FIXME: differentiate bucket not found and meta data not found error.

	return err
}

func (disk *Disk) RevertDeleteBucketMetaData(bucketName string, name string) error {
	metaDataFile := path.Join(disk.bucketsDir, bucketName, name)
	trashMetaDataFile := path.Join(disk.trashDir, bucketName+"."+name)
	return os.Rename(trashMetaDataFile, metaDataFile)
}
