package os

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"

	xhash "github.com/balamurugana/goat/pkg/hash"
)

type checksumHeader struct {
	HashName   string `json:"hashName"`
	HashKey    string `json:"hashKey"`
	HashLength uint   `json:"hashLength"`
	BlockSize  uint   `json:"blockSize"`
	BlockCount uint   `json:"blockCount"`
	DataLength uint64 `json:"dataLength"`
}

type checksumFile struct {
	*os.File
	hasher xhash.Hash
	header *checksumHeader
	buf    []byte
}

func createChecksumFile(filename string, blockSize, blockCount uint, size uint64) (*checksumFile, error) {
	var file *os.File
	var err error

	if file, err = os.OpenFile(filename+".checksum", os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	hasher := xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
	header := &checksumHeader{
		HashName:   hasher.Name(),
		HashKey:    hasher.HashKey(),
		HashLength: hasher.HashLength(),
		BlockSize:  blockSize,
		BlockCount: blockCount,
		DataLength: size,
	}

	if err = json.NewEncoder(file).Encode(header); err != nil {
		return nil, err
	}

	return &checksumFile{
		File:   file,
		hasher: hasher,
		header: header,
	}, nil
}

func openChecksumFile(filename string) (*checksumFile, error) {
	var file *os.File
	var err error

	if file, err = os.Open(filename + ".checksum"); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	// Read header data.
	var data []byte
	var n int
	for {
		buf := make([]byte, 1024)
		if n, err = io.ReadFull(file, buf); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}

		data = append(data, buf[:n]...)
		if i := bytes.IndexRune(data, '\n'); i >= 0 {
			data = data[:i+1]
			break
		}
	}

	if _, err = file.Seek(int64(len(data)), io.SeekStart); err != nil {
		return nil, err
	}

	header := new(checksumHeader)
	if err = json.Unmarshal(data, header); err != nil {
		return nil, err
	}

	var key []byte
	if key, err = hex.DecodeString(header.HashKey); err != nil {
		return nil, err
	}

	if len(key) == 0 {
		key = nil
	}

	var hasher xhash.Hash
	if hasher, err = xhash.NewHash(header.HashName, key); err != nil {
		return nil, err
	}

	return &checksumFile{
		File:   file,
		header: header,
		hasher: hasher,
	}, nil
}

func (file *checksumFile) Write(b []byte) (n int, err error) {
	file.hasher.Reset()
	if n, err = file.hasher.Write(b); err != nil {
		return n, err
	}

	_, err = file.File.WriteString(file.hasher.HexSum(nil) + "\n")

	return n, err
}

func (file *checksumFile) Skip(blockCount uint) error {
	bytesToSkip := int64(blockCount * (file.header.HashLength + 1)) // skip hashes including '\n'
	_, err := file.File.Seek(bytesToSkip, io.SeekCurrent)
	return err
}

func (file *checksumFile) ReadSum() (string, error) {
	if file.buf == nil {
		file.buf = make([]byte, file.header.HashLength+1)
	}

	if _, err := file.Read(file.buf); err != nil {
		return "", err
	}

	return string(file.buf[:file.header.HashLength]), nil
}
