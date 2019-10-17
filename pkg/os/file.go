package os

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/balamurugana/goat/pkg/boundary"
	xhash "github.com/balamurugana/goat/pkg/hash"
)

const defaultBlockSize = 1024 * 1024 // 1 MiB

type SectionFileReader struct {
	file   *os.File
	reader *io.SectionReader
}

func (sf *SectionFileReader) Read(p []byte) (int, error) {
	return sf.reader.Read(p)
}

func (sf *SectionFileReader) Close() error {
	return sf.file.Close()
}

func NewSectionFileReader(file *os.File, offset, length int64) *SectionFileReader {
	return &SectionFileReader{
		file:   file,
		reader: io.NewSectionReader(file, offset, length),
	}
}

func WriteFile(filename string, data io.Reader, size uint64, bitrotProtection bool) (checksum string, err error) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return "", err
	}

	defer file.Close()

	hashWriter := xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
	var writer io.Writer

	blockCount := size / defaultBlockSize
	if blockCount*defaultBlockSize < size {
		blockCount++
	}

	if bitrotProtection {
		checksumFile, err := createChecksumFile(filename, defaultBlockSize, uint(blockCount), size)
		if err != nil {
			return "", err
		}

		defer checksumFile.Close()

		writer = io.MultiWriter(file, checksumFile, hashWriter)
	} else {
		writer = io.MultiWriter(file, hashWriter)
	}

	buf := make([]byte, defaultBlockSize)

	for i := uint64(0); i < blockCount; i++ {
		if i == (blockCount - 1) {
			buf = buf[:size-i*defaultBlockSize]
		}

		if _, err = io.ReadFull(data, buf); err != nil {
			return "", err
		}

		if _, err = writer.Write(buf); err != nil {
			return "", err
		}
	}

	return hashWriter.HexSum(nil), nil
}

func RemoveFile(filename string, bitrotProtection bool) error {
	var err2 error

	err1 := os.Remove(filename)
	if bitrotProtection {
		err2 = os.Remove(filename + ".checksum")
	}

	if err1 != nil {
		if err2 != nil {
			return fmt.Errorf("multiple remove error; %v; %v", err1, err2)
		}

		return err1
	}

	return err2
}

func OpenFile(filename string, offset int64, length uint64, bitrotProtection bool) (io.ReadCloser, error) {
	var file *os.File
	var err error
	if file, err = os.Open(filename); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	dataLength := int64(length)

	if !bitrotProtection {
		var fi os.FileInfo
		if fi, err = file.Stat(); err != nil {
			return nil, err
		}

		if offset < 0 {
			offset = fi.Size() - offset
		}

		if offset < 0 {
			return nil, errors.New("insufficient data")
		}

		if offset+dataLength > fi.Size() {
			return nil, errors.New("insufficient data")
		}

		return NewSectionFileReader(file, offset, dataLength), nil
	}

	var checksumFile *checksumFile
	if checksumFile, err = openChecksumFile(filename); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			checksumFile.Close()
		}
	}()

	size := int64(checksumFile.header.DataLength)

	if offset < 0 {
		offset = size - offset
	}

	if offset < 0 {
		return nil, errors.New("insufficient data")
	}

	if offset+dataLength > size {
		return nil, errors.New("insufficient data")
	}

	blocksToSkip, blocksToRead, bytesToSkipInFirstBlock, bytesToReadInLastBlock, lastBlockSize := boundary.CalcBoundaries(
		size, int64(checksumFile.header.BlockSize), int64(checksumFile.header.BlockCount), offset, dataLength,
	)

	if err = checksumFile.Skip(uint(blocksToSkip)); err != nil {
		return nil, err
	}

	bytesToSkip := blocksToSkip * int64(checksumFile.header.BlockSize)
	if checksumFile.header.BlockCount <= 1 {
		bytesToSkip = blocksToSkip * lastBlockSize
	}

	if _, err = file.Seek(bytesToSkip, io.SeekStart); err != nil {
		return nil, err
	}

	return &checksummedReader{
		file:                    file,
		checksumFile:            checksumFile,
		blocksToRead:            blocksToRead,
		bytesToSkipInFirstBlock: bytesToSkipInFirstBlock,
		bytesToReadInLastBlock:  bytesToReadInLastBlock,
		lastBlockSize:           lastBlockSize,
	}, nil
}

func RenameFile(oldname, newname string, bitrotProtection bool) error {
	if bitrotProtection {
		if err := os.Rename(oldname+".checksum", newname+".checksum"); err != nil {
			return err
		}
	}

	return os.Rename(oldname, newname)
}

func WriteJSONFile(filename string, inter interface{}) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	return json.NewEncoder(file).Encode(inter)
}

func ReadJSONFile(filename string, limit int64, inter interface{}) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}

	defer file.Close()

	if limit > 0 {
		return json.NewDecoder(io.LimitReader(file, limit)).Decode(inter)
	}

	return json.NewDecoder(file).Decode(inter)
}
