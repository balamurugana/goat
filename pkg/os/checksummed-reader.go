package os

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type checksummedReader struct {
	file         *os.File
	checksumFile *checksumFile

	blocksToRead            int64
	bytesToSkipInFirstBlock int64
	bytesToReadInLastBlock  int64
	lastBlockSize           int64

	block          []byte
	bytesAvailable int64
	index          int64
}

func (reader *checksummedReader) readBlock() error {
	if reader.index == reader.blocksToRead {
		return io.EOF
	}

	if reader.block == nil {
		reader.block = make([]byte, reader.checksumFile.header.BlockSize)
	} else {
		reader.block = reader.block[:cap(reader.block)]
	}

	if reader.index == reader.blocksToRead-1 {
		reader.block = reader.block[:reader.lastBlockSize]
	}

	checksum, err := reader.checksumFile.ReadSum()
	if err != nil {
		return err
	}

	if _, err = io.ReadFull(reader.file, reader.block); err != nil {
		return err
	}

	reader.checksumFile.hasher.Reset()
	if _, err = reader.checksumFile.hasher.Write(reader.block); err != nil {
		return err
	}
	c := reader.checksumFile.hasher.HexSum(nil)

	if c != checksum {
		return fmt.Errorf("checksum mismatch; expected: %v, got: %v", checksum, c)
	}

	if reader.index == 0 {
		copy(reader.block, reader.block[reader.bytesToSkipInFirstBlock:])
		length := int64(len(reader.block)) - reader.bytesToSkipInFirstBlock
		reader.block = reader.block[:length]
	}

	if reader.index == reader.blocksToRead-1 {
		reader.block = reader.block[:reader.bytesToReadInLastBlock]
	}

	reader.index++

	reader.bytesAvailable = int64(len(reader.block))

	return nil
}

func (reader *checksummedReader) Read(b []byte) (n int, err error) {
	for n < len(b) {
		if reader.bytesAvailable == 0 {
			if err := reader.readBlock(); err != nil {
				return n, err
			}
		}

		off := int64(len(reader.block)) - reader.bytesAvailable
		copied := copy(b[n:], reader.block[off:])
		reader.bytesAvailable -= int64(copied)

		n += copied
	}

	return n, nil
}

func (reader *checksummedReader) Close() error {
	err1 := reader.file.Close()
	err2 := reader.checksumFile.Close()

	if err1 != nil {
		if err2 != nil {
			return errors.New("multiple close error")
		}

		return err1
	}

	return err2
}
