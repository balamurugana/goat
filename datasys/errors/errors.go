package errors

import "errors"

var (
	ErrUploadIDAlreadyExist = errors.New("upload ID already exist")
	ErrUploadIDNotFound     = errors.New("upload ID not found")
	ErrPartChecksumNotFound = errors.New("part checksum file not found")
	ErrPartNotFound         = errors.New("part file not found")
	ErrDataIDAlreadyExist   = errors.New("data ID already exist")
	ErrDataIDNotFound       = errors.New("data ID not found")
)

var (
	ErrBucketAlreadyExist = errors.New("bucket already exist")
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketNotEmpty     = errors.New("bucket not empty")
)
