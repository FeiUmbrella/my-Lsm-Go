package utils

import "errors"

var (
	// File errors
	ErrFileNotFound    = errors.New("file not found")
	ErrFileCorrupted   = errors.New("file corrupted")
	ErrInvalidChecksum = errors.New("invalid checksum")

	// SST errors
	ErrSSTNotFound      = errors.New("SST not found")
	ErrSSTCorrupted     = errors.New("SST file corrupted")
	ErrInvalidSSTFile   = errors.New("invalid SST file format")
	ErrInvalidMetadata  = errors.New("invalid metadata format")
	ErrBlockNotFound    = errors.New("block not found")
	ErrEmptySST         = errors.New("cannot build empty SST")
	ErrInvalidBlockSize = errors.New("invalid block size")
	ErrCorruptedFile    = errors.New("corrupted SST file")
)
