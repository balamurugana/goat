package disk

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"

	xerrors "github.com/balamurugana/goat/datasys/errors"
	xos "github.com/balamurugana/goat/pkg/os"
)

type Disk struct {
	id       string
	storeDir string

	dataDir    string
	tmpDir     string
	uploadsDir string
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

	dataDir := path.Join(storeDir, "data")
	if err := os.Mkdir(dataDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	tmpDir := path.Join(storeDir, "tmp")
	if err := os.Mkdir(tmpDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	uploadsDir := path.Join(storeDir, "uploads")
	if err := os.Mkdir(uploadsDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	trashDir := path.Join(storeDir, "trash")
	if err := os.Mkdir(trashDir, 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	return &Disk{
		id:         id,
		storeDir:   storeDir,
		dataDir:    dataDir,
		tmpDir:     tmpDir,
		uploadsDir: uploadsDir,
		trashDir:   trashDir,
	}, nil
}

func (disk *Disk) ID() string {
	return disk.id
}

func (disk *Disk) SaveTempFile(filename string, data io.Reader, size uint64, bitrotProtection bool) (checksum string, err error) {
	return xos.WriteFile(path.Join(disk.tmpDir, filename), data, size, bitrotProtection)
}

func (disk *Disk) RemoveTempFile(filename string, bitrotProtection bool) (err error) {
	return xos.RemoveFile(path.Join(disk.tmpDir, filename), bitrotProtection)
}

func (disk *Disk) InitUpload(uploadID UploadID) (err error) {
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())

	if err = os.Mkdir(uploadIDDir, os.ModePerm); errors.Is(err, os.ErrExist) {
		err = xerrors.ErrUploadIDAlreadyExist
	}

	return err
}

func (disk *Disk) RevertInitUpload(uploadID UploadID) (err error) {
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())

	if err = os.Remove(uploadIDDir); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrUploadIDNotFound
	}

	return err
}

func (disk *Disk) UploadPart(uploadID UploadID, partID, tempFile string) (err error) {
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return xerrors.ErrUploadIDNotFound
	}

	src := path.Join(disk.tmpDir, tempFile)
	dest := path.Join(uploadIDDir, partID+".part")
	return xos.RenameFile(src, dest, true)
}

func (disk *Disk) RevertUploadPart(uploadID UploadID, partID, tempFile string) (err error) {
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return xerrors.ErrUploadIDNotFound
	}

	partFile := path.Join(uploadIDDir, partID+".part")
	dest := path.Join(disk.tmpDir, tempFile)
	if err = xos.RenameFile(partFile, dest, true); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrPartNotFound
	}

	return err
}

// func (disk *Disk) UploadPartCopy(uploadID UploadID, partID string, srcID DataID, offset int64, length uint64) (etag string, err error) {
// }
//

func (disk *Disk) AbortUpload(uploadID UploadID) (err error) {
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())
	trashDir := path.Join(disk.trashDir, uploadID.String())
	if err = os.Rename(uploadIDDir, trashDir); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrUploadIDNotFound
	}

	// FIXME: cleanup trash

	return err
}

func (disk *Disk) RevertAbortUpload(uploadID UploadID) (err error) {
	uploadIDDirInTrash := path.Join(disk.trashDir, uploadID.String())
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())
	if err = os.Rename(uploadIDDirInTrash, uploadIDDir); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrUploadIDNotFound
	}

	return err
}

func (disk *Disk) CompleteUpload(dataID DataID, uploadID UploadID, parts []Part) (err error) {
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())
	if !xos.Exist(uploadIDDir) {
		return xerrors.ErrUploadIDNotFound
	}

	dataDir := path.Join(disk.dataDir, dataID.String())
	if xos.Exist(dataDir) {
		return xerrors.ErrDataIDAlreadyExist
	}

	size := uint64(0)
	for _, part := range parts {
		size += part.Size
	}

	// FIXME: remove unwanted parts here.

	dataInfo := &DataInfo{
		Parts: parts,
		Size:  size,
	}

	err = func() error {
		dataJSONFile := path.Join(uploadIDDir, "data.json")
		file, err := os.OpenFile(dataJSONFile, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		return json.NewEncoder(file).Encode(dataInfo)
	}()
	if err != nil {
		return err
	}

	return os.Rename(uploadIDDir, dataDir)
}

func (disk *Disk) RevertCompleteUpload(dataID DataID, uploadID UploadID, parts []Part) (err error) {
	dataDir := path.Join(disk.dataDir, dataID.String())
	uploadIDDir := path.Join(disk.uploadsDir, uploadID.String())
	if err = os.Rename(dataDir, uploadIDDir); errors.Is(err, os.ErrNotExist) {
		err = xerrors.ErrDataIDNotFound
	}
	if err != nil {
		return err
	}

	dataJSONFile := path.Join(uploadIDDir, "data.json")
	os.Remove(dataJSONFile)

	return err
}

// func (disk *Disk) ListParts(uploadID string) ([]Part, error) {
// }
//

func (disk *Disk) Get(dataID DataID, offset int64, length uint64) (rc io.ReadCloser, err error) {
	dataDir := path.Join(disk.dataDir, dataID.String())
	if !xos.Exist(dataDir) {
		err = xerrors.ErrDataIDNotFound
	}

	var dataInfo DataInfo
	err = func() error {
		file, err := os.Open(path.Join(dataDir, "data.json"))
		if err != nil {
			return err
		}
		defer file.Close()

		return json.NewDecoder(file).Decode(&dataInfo)
	}()
	if err != nil {
		return nil, err
	}

	return newDataReader(dataDir, &dataInfo, offset, length)
}

// func (disk *Disk) GetMetadata(ID string) (map[string][]string, error) {
// }
//
// func (disk *Disk) Delete(ID string) error {
// }
//
// func (disk *Disk) Copy(ID, srcID string, offset, length uint64, metadata map[string][]string) error {
// }
//
